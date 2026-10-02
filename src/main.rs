use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Instant;

use axum::{Router, middleware, routing::get};
use rmcp::transport::streamable_http_server::{
    StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

use crabbian_watcher::config::Config;
use crabbian_watcher::events::EventBus;
use crabbian_watcher::ingest::FeedStats;
use crabbian_watcher::mcp::{self, Watcher};
use crabbian_watcher::rest::Rest;
use crabbian_watcher::{control, ingest, rest, shard};

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config::from_env();
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::new(&cfg.log_level))
        .init();
    rustls::crypto::ring::default_provider()
        .install_default()
        .ok();

    let epoch: String = uuid::Uuid::new_v4().simple().to_string()[..8].into();
    let started = Instant::now();
    let ct = CancellationToken::new();

    let bus = Arc::new(EventBus::new(epoch.clone()));
    let stats = Arc::new(FeedStats::default());
    let (router, shard_io) = shard::Router::new(shard::SHARDS);
    let (want_tx, want_rx) = watch::channel(Arc::new(BTreeSet::new()));
    let (ctl_tx, ctl_rx) = mpsc::unbounded_channel();
    let (fired_tx, fired_rx) = mpsc::unbounded_channel();
    let (rest_tx, rest_rx) = mpsc::unbounded_channel();

    for io in shard_io {
        tokio::spawn(shard::run(
            io,
            bus.clone(),
            fired_tx.clone(),
            ct.child_token(),
        ));
    }
    tokio::spawn(control::run(
        ctl_rx,
        fired_rx,
        router.clone(),
        want_tx,
        rest_tx.clone(),
        ct.child_token(),
    ));
    tokio::spawn(rest::run(
        Rest::new()?,
        rest_rx,
        want_rx.clone(),
        router.clone(),
        ctl_tx.clone(),
        ct.child_token(),
    ));
    tokio::spawn(ingest::run(
        cfg.binance_ws_url.clone(),
        want_rx,
        router.clone(),
        stats.clone(),
        rest_tx,
        ct.child_token(),
    ));

    let service: StreamableHttpService<Watcher, LocalSessionManager> = StreamableHttpService::new(
        move || {
            Ok(Watcher::new(
                bus.clone(),
                ctl_tx.clone(),
                router.clone(),
                stats.clone(),
                started,
            ))
        },
        Default::default(),
        StreamableHttpServerConfig::default()
            .disable_allowed_hosts()
            .with_cancellation_token(ct.child_token()),
    );
    let mcp = Router::new()
        .nest_service("/mcp", service)
        .layer(middleware::from_fn_with_state(
            Arc::<str>::from(cfg.api_key.as_str()),
            mcp::require_api_key,
        ));
    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .merge(mcp);

    let listener = tokio::net::TcpListener::bind(&cfg.bind).await?;
    tracing::info!(process = %cfg.log_process, bind = %cfg.bind, binance_ws = %cfg.binance_ws_url, epoch = %epoch, "started");

    axum::serve(listener, app)
        .with_graceful_shutdown({
            let ct = ct.clone();
            async move {
                shutdown_signal().await;
                ct.cancel();
            }
        })
        .await?;
    tracing::info!("stopped");
    Ok(())
}
