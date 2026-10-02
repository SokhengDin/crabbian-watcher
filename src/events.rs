use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

use schemars::JsonSchema;
use serde::Serialize;
use tokio::sync::Notify;

use crate::detector::Dir;
use crate::detector::signals::{Confirmer, OiRead};
use crate::levels::LevelHit;
use crate::rules::{Action, Owner, Plan};

pub const RING_CAP: usize = 1_000;
const DAY_MS: u64 = 86_400_000;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum EventKind {
    Jump,
    Level,
    Liq,
    Oi,
    Rule,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, JsonSchema)]
pub struct Signals {
    pub price: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub z1: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub z5: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub z15: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub window_s: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub move_pct: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vol_x: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub taker_buy: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub taker_sell: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub liq_usd: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub liq_x: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oi_z: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub oi_read: Option<OiRead>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub funding: Option<f64>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub confirmers: Vec<Confirmer>,
}

#[derive(Debug, Clone, PartialEq, Serialize, JsonSchema)]
pub struct RuleRef {
    pub rule_id: String,
    pub owner: Owner,
    pub note: String,
    pub action: Action,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub plan: Option<Plan>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_seq: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, JsonSchema)]
pub struct Event {
    pub seq: u64,
    pub epoch: String,
    pub ts: String,
    pub symbol: String,
    #[schemars(
        description = "1 heads up, 2 please check. Tier 0 (protect) is implied by a levels_hit entry of kind sl, tp or liquidation"
    )]
    pub tier: u8,
    pub kind: EventKind,
    pub direction: Dir,
    pub signals: Signals,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub levels_hit: Vec<LevelHit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rule: Option<RuleRef>,
}

#[derive(Debug, Clone, PartialEq, Serialize, JsonSchema)]
pub struct WaitResult {
    #[schemars(
        description = "Current epoch; if it differs from what you sent, reset your cursor to `next`"
    )]
    pub epoch: String,
    #[schemars(description = "Pass this as `after` on the next call")]
    pub next: u64,
    #[schemars(description = "Events that fell out of the ring before you read them")]
    pub dropped: u64,
    pub events: Vec<Event>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BusStats {
    pub last_seq: u64,
    pub today: u64,
    pub total: u64,
}

struct Inner {
    ring: VecDeque<Event>,
    next_seq: u64,
    day: u64,
    stats: BusStats,
}

pub struct EventBus {
    epoch: String,
    inner: Mutex<Inner>,
    notify: Notify,
}

pub fn iso_ms(ms: u64) -> String {
    chrono::DateTime::from_timestamp_millis(ms as i64)
        .map(|t| t.format("%Y-%m-%dT%H:%M:%S%.3fZ").to_string())
        .unwrap_or_default()
}

impl EventBus {
    pub fn new(epoch: impl Into<String>) -> Self {
        Self {
            epoch: epoch.into(),
            inner: Mutex::new(Inner {
                ring: VecDeque::with_capacity(RING_CAP),
                next_seq: 1,
                day: 0,
                stats: BusStats::default(),
            }),
            notify: Notify::new(),
        }
    }

    pub fn epoch(&self) -> &str {
        &self.epoch
    }

    pub fn stats(&self) -> BusStats {
        self.inner.lock().map(|i| i.stats).unwrap_or_default()
    }

    pub fn push(&self, mut e: Event, now_ms: u64) -> u64 {
        let seq = {
            let mut g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
            let seq = g.next_seq;
            g.next_seq += 1;
            e.seq = seq;
            e.epoch.clone_from(&self.epoch);
            e.ts = iso_ms(now_ms);
            let day = now_ms / DAY_MS;
            if day != g.day {
                g.day = day;
                g.stats.today = 0;
            }
            g.stats.today += 1;
            g.stats.total += 1;
            g.stats.last_seq = seq;
            tracing::info!(
                seq, symbol = %e.symbol, tier = e.tier, kind = ?e.kind, direction = ?e.direction,
                z5 = e.signals.z5, move_pct = e.signals.move_pct, levels = e.levels_hit.len(),
                rule = e.rule.as_ref().map(|r| r.rule_id.as_str()), "event"
            );
            if g.ring.len() == RING_CAP {
                g.ring.pop_front();
            }
            g.ring.push_back(e);
            seq
        };
        self.notify.notify_waiters();
        seq
    }

    pub fn poll(&self, after: u64, epoch: Option<&str>) -> Option<WaitResult> {
        let g = self.inner.lock().unwrap_or_else(|p| p.into_inner());
        let result = |events: Vec<Event>, dropped, next| WaitResult {
            epoch: self.epoch.clone(),
            next,
            dropped,
            events,
        };
        if epoch != Some(self.epoch.as_str()) {
            let events: Vec<Event> = g.ring.iter().cloned().collect();
            let next = events.last().map_or(0, |e| e.seq);
            return Some(result(events, 0, next));
        }
        let oldest = g.ring.front()?.seq;
        let dropped = oldest.saturating_sub(after + 1);
        let events: Vec<Event> = g.ring.iter().filter(|e| e.seq > after).cloned().collect();
        let next = events.last()?.seq;
        Some(result(events, dropped, next))
    }

    pub async fn wait(&self, after: u64, epoch: Option<&str>, timeout: Duration) -> WaitResult {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(r) = self.poll(after, epoch) {
                return r;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return WaitResult {
                    epoch: self.epoch.clone(),
                    next: after,
                    dropped: 0,
                    events: Vec::new(),
                };
            }
        }
    }
}
