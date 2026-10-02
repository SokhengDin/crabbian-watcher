use std::sync::Arc;
use std::sync::atomic::Ordering::Relaxed;
use std::time::{Duration, Instant};

use axum::{
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use rmcp::{
    Json, ServerHandler,
    handler::server::{router::tool::ToolRouter, wrapper::Parameters},
    tool, tool_handler, tool_router,
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, oneshot};

use crate::control::{Cmd, CreateResult, InterestResult, LevelsResult, RuleView};
use crate::events::{EventBus, WaitResult};
use crate::ingest::FeedStats;
use crate::levels::Level;
use crate::rules::{Owner, RuleSpec};
use crate::shard::{Router, SymbolSnapshot, now_ms};

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SetInterestArgs {
    #[schemars(
        description = "Complete list of USDT perpetual symbols to watch, e.g. [\"BTCUSDT\", \"SOLUSDT\"]. Replaces the previous list"
    )]
    pub symbols: Vec<String>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SetLevelsArgs {
    #[schemars(
        description = "Complete list of price levels that matter to the desk. Replaces all previous levels"
    )]
    pub levels: Vec<Level>,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct ListRulesArgs {
    #[schemars(description = "Owner whose active rules to list")]
    pub owner: Owner,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct CancelRuleArgs {
    #[schemars(description = "Id returned by create_rule")]
    pub rule_id: String,
    #[schemars(description = "User id of the rule's owner; only the owner can cancel")]
    pub user_id: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct SymbolArgs {
    #[schemars(description = "USDT perpetual symbol, e.g. SOLUSDT")]
    pub symbol: String,
}

#[derive(Debug, Deserialize, JsonSchema)]
pub struct WaitEventsArgs {
    #[schemars(
        description = "Highest seq already processed (the `next` of the previous call, 0 at start)"
    )]
    pub after: u64,
    #[schemars(description = "Epoch from the previous call; omit at start")]
    pub epoch: Option<String>,
    #[schemars(description = "Seconds to wait for a new event, 1 to 25")]
    pub timeout_s: u32,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct RulesList {
    pub rules: Vec<RuleView>,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct CancelResult {
    pub cancelled: bool,
}

#[derive(Debug, Serialize, JsonSchema)]
pub struct WatcherStatus {
    #[schemars(
        description = "Random id generated at process start; a change means cursors are meaningless"
    )]
    pub epoch: String,
    pub uptime_s: u64,
    #[schemars(description = "Resident memory in MiB, null if unavailable")]
    pub rss_mib: Option<f64>,
    pub feed_connected: bool,
    #[schemars(
        description = "Milliseconds since the last Binance message; above 60000 means fast protection is blind"
    )]
    pub feed_age_ms: Option<u64>,
    pub feed_down_s: Option<u64>,
    pub msgs_per_sec: u64,
    pub dropped_ticks: u64,
    pub parse_errors: u64,
    pub reconnects: u64,
    pub interest: usize,
    pub symbols: usize,
    pub warm_symbols: usize,
    pub warming_up: bool,
    pub levels: usize,
    pub rules: usize,
    pub known_symbols: usize,
    pub events_today: u64,
    pub events_total: u64,
    pub last_seq: u64,
}

#[derive(Clone)]
pub struct Watcher {
    bus: Arc<EventBus>,
    ctl: mpsc::UnboundedSender<Cmd>,
    router: Router,
    stats: Arc<FeedStats>,
    started: Instant,
    tool_router: ToolRouter<Self>,
}

async fn ask<T>(
    ctl: &mpsc::UnboundedSender<Cmd>,
    f: impl FnOnce(oneshot::Sender<T>) -> Cmd,
) -> Result<T, String> {
    let (tx, rx) = oneshot::channel();
    ctl.send(f(tx))
        .map_err(|_| "control task is down".to_string())?;
    rx.await
        .map_err(|_| "control task dropped the request".to_string())
}

#[tool_router(router = tool_router)]
impl Watcher {
    pub fn new(
        bus: Arc<EventBus>,
        ctl: mpsc::UnboundedSender<Cmd>,
        router: Router,
        stats: Arc<FeedStats>,
        started: Instant,
    ) -> Self {
        Self {
            bus,
            ctl,
            router,
            stats,
            started,
            tool_router: Self::tool_router(),
        }
    }

    #[tool(
        description = "Set the full list of USDT perpetual symbols crabbian-watcher streams and analyzes. Replaces the previous list; symbols with levels or rules stay subscribed anyway. Returns what was added, removed and which symbols are unknown."
    )]
    async fn set_interest(
        &self,
        Parameters(a): Parameters<SetInterestArgs>,
    ) -> Result<Json<InterestResult>, String> {
        ask(&self.ctl, |tx| Cmd::SetInterest(a.symbols, tx))
            .await?
            .map(Json)
    }

    #[tool(
        description = "Replace all price levels the desk cares about: thesis levels, and the sl, tp and liquidation prices of open trades (with trade_id). A cross, or first approach within 0.25 ATR, raises a level event; sl/tp/liquidation hits mean protect now."
    )]
    async fn set_levels(
        &self,
        Parameters(a): Parameters<SetLevelsArgs>,
    ) -> Result<Json<LevelsResult>, String> {
        ask(&self.ctl, |tx| Cmd::SetLevels(a.levels, tx))
            .await?
            .map(Json)
    }

    #[tool(
        description = "Create a market alert rule: typed conditions that must all hold (price_cross, move_z, volume_x, taker_imbalance, liq_burst, oi_change_z, funding_above). When it fires, an event carries the note and plan to the owner. action notify = heads-up, wake = run the watcher agent. Never opens trades. Returns the rule id or names the invalid field."
    )]
    async fn create_rule(
        &self,
        Parameters(spec): Parameters<RuleSpec>,
    ) -> Result<Json<CreateResult>, String> {
        ask(&self.ctl, |tx| Cmd::CreateRule(Box::new(spec), tx))
            .await?
            .map(Json)
    }

    #[tool(
        description = "List an owner's active rules with their conditions and seconds left before they expire."
    )]
    async fn list_rules(
        &self,
        Parameters(a): Parameters<ListRulesArgs>,
    ) -> Result<Json<RulesList>, String> {
        Ok(Json(RulesList {
            rules: ask(&self.ctl, |tx| Cmd::ListRules(a.owner, tx)).await?,
        }))
    }

    #[tool(
        description = "Cancel a rule. Only the user who owns the rule can cancel it; returns cancelled false otherwise."
    )]
    async fn cancel_rule(
        &self,
        Parameters(a): Parameters<CancelRuleArgs>,
    ) -> Result<Json<CancelResult>, String> {
        Ok(Json(CancelResult {
            cancelled: ask(&self.ctl, |tx| Cmd::CancelRule(a.rule_id, a.user_id, tx)).await?,
        }))
    }

    #[tool(
        description = "Live analysis state of one watched symbol: price, 1m volatility, move z-scores over 1/5/15 minutes, volume multiples, taker buy share, liquidations, funding, mark, open interest z, cooldowns and warm-up."
    )]
    async fn symbol_state(
        &self,
        Parameters(a): Parameters<SymbolArgs>,
    ) -> Result<Json<SymbolSnapshot>, String> {
        let s = a.symbol.trim().to_ascii_uppercase();
        self.router
            .query(&s)
            .await
            .map(Json)
            .ok_or_else(|| format!("{s} is not watched; add it with set_interest"))
    }

    #[tool(
        description = "Long-poll for events after cursor `after`. Returns at once if newer events exist, otherwise waits up to timeout_s. If the epoch changed (service restarted) it returns everything in the ring with the new epoch; reset your cursor. `dropped` counts events lost before you read them. Delivery is at-least-once; dedupe by seq."
    )]
    async fn wait_events(
        &self,
        Parameters(a): Parameters<WaitEventsArgs>,
    ) -> Result<Json<WaitResult>, String> {
        let timeout = Duration::from_secs(a.timeout_s.clamp(1, 25) as u64);
        Ok(Json(
            self.bus.wait(a.after, a.epoch.as_deref(), timeout).await,
        ))
    }

    #[tool(
        description = "Health of the crabbian-watcher sensor: epoch, feed age and connection, symbols and warm-up, message rate, dropped ticks, rules, levels, events today and memory. Use it to check the service is alive and whether it restarted."
    )]
    async fn watcher_status(&self) -> Result<Json<WatcherStatus>, String> {
        let c = ask(&self.ctl, Cmd::Status).await?;
        let sh = self.router.status().await;
        let b = self.bus.stats();
        let now = now_ms();
        let last = self.stats.last_msg_ms.load(Relaxed);
        let down = self.stats.down_since_ms.load(Relaxed);
        Ok(Json(WatcherStatus {
            epoch: self.bus.epoch().to_string(),
            uptime_s: self.started.elapsed().as_secs(),
            rss_mib: memory_stats::memory_stats()
                .map(|s| (s.physical_mem as f64 / 1_048_576.0 * 10.0).round() / 10.0),
            feed_connected: self.stats.connected.load(Relaxed),
            feed_age_ms: (last > 0).then(|| now.saturating_sub(last)),
            feed_down_s: (down > 0).then(|| now.saturating_sub(down) / 1000),
            msgs_per_sec: self.stats.msgs_per_sec.load(Relaxed),
            dropped_ticks: self.stats.dropped.load(Relaxed),
            parse_errors: self.stats.parse_errors.load(Relaxed),
            reconnects: self.stats.reconnects.load(Relaxed),
            interest: c.interest,
            symbols: sh.symbols,
            warm_symbols: sh.warm,
            warming_up: sh.warm < sh.symbols,
            levels: c.levels,
            rules: c.rules,
            known_symbols: c.known_symbols,
            events_today: b.today,
            events_total: b.total,
            last_seq: b.last_seq,
        }))
    }
}

#[tool_handler(router = self.tool_router)]
impl ServerHandler for Watcher {}

pub async fn require_api_key(
    State(key): State<Arc<str>>,
    req: Request,
    next: Next,
) -> Result<Response, StatusCode> {
    let ok = req
        .headers()
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v == &*key);
    if ok {
        Ok(next.run(req).await)
    } else {
        Err(StatusCode::UNAUTHORIZED)
    }
}
