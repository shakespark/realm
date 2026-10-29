use std::env;
use cfg_if::cfg_if;

use realm::cmd;
use realm::conf::{Config, FullConf, LogConf, DnsConf, EndpointConf, EndpointInfo, CmdOverride};
use realm::relay::Relays;
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
            CmdInput::Config(path, opts) => {
                let source = ConfSource { path, opts };
                (source.load(), Some(source))
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
    let mut relays = Relays::default();
    relays.apply(endpoints).await.unwrap_or_else(|e| panic!("{}", e));

    #[cfg(unix)]
    if let Some(reload) = reload {
        return reload_on_signal(relays, reload).await;
    }

    #[cfg(not(unix))]
    let _ = reload;

    relays.join().await;
}

/// Reload config file on SIGHUP.
#[cfg(unix)]
async fn reload_on_signal(mut relays: Relays, reload: Reload) {
    use log::Level;
    use tokio::signal::unix::{signal, SignalKind};
    use realm::relay::report;
    use realm::consts::DEFAULT_LOG_FILE;

    let (source, mut log_conf, dns_conf) = reload;
    let mut hup = signal(SignalKind::hangup()).unwrap_or_else(|e| panic!("failed to listen to SIGHUP: {}", e));

    while hup.recv().await.is_some() {
        report(Level::Info, format_args!("reload: {}", source.path));

        // config errors are panics, which are caught here instead of killing the process
        let src = source.clone();
        let loaded = tokio::task::spawn_blocking(move || {
            let FullConf {
                log, dns, endpoints, ..
            } = src.load();
            (log, dns, build_endpoints(endpoints))
        })
        .await;

        let (new_log, new_dns, endpoints) = match loaded {
            Ok(x) => x,
            Err(e) => {
                let reason = match e.try_into_panic() {
                    Ok(p) => p
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| p.downcast_ref::<&str>().map(|s| s.to_string()))
                        .unwrap_or_default(),
                    Err(e) => e.to_string(),
                };
                report(Level::Error, format_args!("reload failed, keep running: {}", reason));
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
