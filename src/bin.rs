use std::env;
use cfg_if::cfg_if;

use realm::cmd;
use realm::conf::{Config, FullConf, LogConf, DnsConf, EndpointConf, EndpointInfo, CmdOverride};
use realm::ENV_CONFIG;

cfg_if! {
    if #[cfg(feature = "mi-malloc")] {
        use mimalloc::MiMalloc;
        #[global_allocator]
        static GLOBAL: MiMalloc = MiMalloc;
    } else if #[cfg(all(feature = "jemalloc", not(target_env = "msvc")))] {
        use jemallocator::Jemalloc;
        #[global_allocator]
        static GLOBAL: Jemalloc = Jemalloc;
    } else if #[cfg(all(feature = "page-alloc", unix))] {
        use mmap_allocator::MmapAllocator;
        #[global_allocator]
        static GLOBAL: MmapAllocator = MmapAllocator::new();
    }
}

/// Config file and command line overrides, which can be loaded again.
#[derive(Clone)]
struct ConfSource {
    path: String,
    opts: CmdOverride,
}

impl ConfSource {
    fn load(&self) -> FullConf {
        let mut conf = FullConf::from_conf_file(&self.path);
        conf.apply_global_opts().apply_cmd_opts(self.opts.clone());
        conf
    }
}

fn main() {
    let (conf, source) = 'blk: {
        if let Ok(conf_str) = env::var(ENV_CONFIG) {
            if let Ok(conf) = FullConf::from_conf_str(&conf_str) {
                break 'blk (conf, None);
            }
        };

        use cmd::CmdInput;
        match cmd::scan() {
            CmdInput::Endpoint(ep, opts) => {
                let mut conf = FullConf::default();
                conf.add_endpoint(ep).apply_global_opts().apply_cmd_opts(opts);
                (conf, None)
            }
            CmdInput::Config(path, opts, reload) => {
                let source = ConfSource { path, opts };
                (source.load(), reload.then_some(source))
            }
            CmdInput::None => std::process::exit(0),
        }
    };

    start_from_conf(conf, source);
}

fn start_from_conf(full: FullConf, source: Option<ConfSource>) {
    let FullConf {
        log: log_conf,
        dns: dns_conf,
        endpoints: endpoints_conf,
        ..
    } = full;

    setup_log(log_conf.clone());
    setup_dns(dns_conf.clone());
    setup_transport();

    let endpoints = build_endpoints(endpoints_conf);
    endpoints.iter().for_each(|(_, x)| println!("inited: {}", x.endpoint));

    execute(endpoints, source.map(|source| (source, log_conf, dns_conf)));
}

fn build_endpoints(confs: Vec<EndpointConf>) -> Vec<(EndpointConf, EndpointInfo)> {
    confs.into_iter().map(|conf| (conf.clone(), conf.build())).collect()
}

fn setup_log(log: LogConf) {
    println!("log: {}", log);

    let (level, output) = log.build();
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{}[{}][{}]{}",
                chrono::Local::now().format("[%Y-%m-%d][%H:%M:%S]"),
                record.target(),
                record.level(),
                message
            ))
        })
        // filtered by max level, which can be changed on reload
        .level(log::LevelFilter::Trace)
        .chain(output)
        .apply()
        .unwrap_or_else(|e| panic!("failed to setup logger: {}", e));
    log::set_max_level(level);
}

fn setup_dns(dns: DnsConf) {
    println!("dns: {}", dns);

    let (conf, opts) = dns.build();
    realm::core::dns::build_lazy(conf, opts);
}

fn setup_transport() {
    #[cfg(feature = "transport")]
    {
        realm::core::kaminari::install_tls_provider();
    }
}

type Reload = (ConfSource, LogConf, DnsConf);

fn execute(eps: Vec<(EndpointConf, EndpointInfo)>, reload: Option<Reload>) {
    #[cfg(feature = "multi-thread")]
    {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(run(eps, reload))
    }

    #[cfg(not(feature = "multi-thread"))]
    {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(run(eps, reload))
    }
}

async fn run(endpoints: Vec<(EndpointConf, EndpointInfo)>, reload: Option<Reload>) {
    use realm::core::tcp::run_tcp;
    use realm::core::udp::run_udp;
    use futures::future::join_all;

    #[cfg(unix)]
    if let Some(reload) = reload {
        return run_reloadable(endpoints, reload).await;
    }

    #[cfg(not(unix))]
    if reload.is_some() {
        eprintln!("--reload is not supported on this platform, ignored");
    }

    let mut workers = Vec::with_capacity(2 * endpoints.len());

    for (
        _,
        EndpointInfo {
            endpoint,
            no_tcp,
            use_udp,
        },
    ) in endpoints
    {
        if use_udp {
            workers.push(tokio::spawn(run_udp(endpoint.clone())));
        }

        if !no_tcp {
            workers.push(tokio::spawn(run_tcp(endpoint)));
        }
    }

    workers.shrink_to_fit();

    join_all(workers).await;
}

/// Run relays, reload config file on SIGHUP.
#[cfg(unix)]
async fn run_reloadable(endpoints: Vec<(EndpointConf, EndpointInfo)>, reload: Reload) {
    use std::process::exit;
    use log::Level;
    use tokio::signal::unix::{signal, SignalKind};
    use realm::relay::{Relays, report};
    use realm::consts::DEFAULT_LOG_FILE;

    // listen before relays start, the default action of SIGHUP is to terminate
    let mut hup = signal(SignalKind::hangup()).unwrap_or_else(|e| panic!("failed to listen to SIGHUP: {}", e));

    let mut relays = Relays::default();
    if let Err(e) = relays.start(endpoints) {
        eprintln!("{}", e);
        exit(1);
    }
    if relays.is_empty() {
        eprintln!("no endpoint is running");
        exit(1);
    }

    let (source, mut log_conf, dns_conf) = reload;
    quiet_panic::install();

    loop {
        tokio::select! {
            res = hup.recv() => if res.is_none() {
                break;
            },
            // also when no endpoint is left after a reload
            _ = relays.join() => {
                report(Level::Error, format_args!("all endpoints exited"));
                exit(1);
            }
        }

        report(Level::Info, format_args!("reload: {}", source.path));

        // config errors are panics, which are caught here instead of killing the process
        let src = source.clone();
        let loaded = tokio::task::spawn_blocking(move || {
            let _quiet = quiet_panic::Guard::new();
            let FullConf {
                log, dns, endpoints, ..
            } = src.load();
            (log, dns, build_endpoints(endpoints))
        })
        .await;

        let (new_log, new_dns, endpoints) = match loaded {
            Ok(x) => x,
            Err(e) => {
                report(
                    Level::Error,
                    format_args!("reload failed, keep running: {}", quiet_panic::reason(e)),
                );
                continue;
            }
        };

        if let Err(e) = relays.apply(endpoints).await {
            report(Level::Error, format_args!("reload failed, keep running: {}", e));
            continue;
        }

        if new_log.level != log_conf.level {
            let level = new_log.level.unwrap_or_default();
            log::set_max_level(level.into());
            log_conf.level = new_log.level;
            report(Level::Info, format_args!("log level: {}", level));
        }
        let output = |conf: &LogConf| conf.output.clone().unwrap_or_else(|| DEFAULT_LOG_FILE.to_string());
        if output(&new_log) != output(&log_conf) {
            report(Level::Warn, format_args!("log output is changed, restart to apply"));
        }
        if new_dns != dns_conf {
            report(Level::Warn, format_args!("dns is changed, restart to apply"));
        }

        report(Level::Info, format_args!("reload: done"));
    }
}

/// Silence the panic message of loading config, which is reported as an error instead.
#[cfg(unix)]
mod quiet_panic {
    use std::cell::Cell;
    use tokio::task::JoinError;

    thread_local! {
        static QUIET: Cell<bool> = const { Cell::new(false) };
    }

    pub fn install() {
        let default = std::panic::take_hook();
        std::panic::set_hook(Box::new(move |info| {
            if !QUIET.with(Cell::get) {
                default(info)
            }
        }));
    }

    /// Panics on this thread are quiet while the guard is alive.
    pub struct Guard;

    impl Guard {
        pub fn new() -> Self {
            QUIET.with(|x| x.set(true));
            Guard
        }
    }

    impl Drop for Guard {
        fn drop(&mut self) {
            QUIET.with(|x| x.set(false));
        }
    }

    pub fn reason(e: JoinError) -> String {
        match e.try_into_panic() {
            Ok(p) => p
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default(),
            Err(e) => e.to_string(),
        }
    }
}
