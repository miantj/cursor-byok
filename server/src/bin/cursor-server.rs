//! Starts the Cursor BYOK server executable.
use cursor_server::{App, Config, Result};
use tracing_subscriber::prelude::*;

fn main() -> Result<()> {
    if cursor_server::local_app::release_proxy_if_requested() {
        return Ok(());
    }
    serve()
}

#[tokio::main]
async fn serve() -> Result<()> {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "cursor_server=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    App::new(Config::from_env()?).await?.serve().await
}
