use once_cell::sync::Lazy;
use tracing_subscriber::{Layer, fmt, layer::SubscriberExt, util::SubscriberInitExt};

pub fn init_logger() {
    Lazy::force(&INIT_LOGGER);
}

static INIT_LOGGER: Lazy<()> = Lazy::new(|| {
    let fmt_layer = fmt::layer()
        .with_ansi(true)
        .with_target(true)
        .with_level(true)
        .with_line_number(true)
        .with_file(true)
        .with_thread_ids(false)
        .boxed();

    tracing_subscriber::registry()
        .with(fmt_layer)
        .with(tracing_subscriber::EnvFilter::from_env("LOG_LEVEL"))
        .init();
});
