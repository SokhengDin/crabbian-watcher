use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::detector::consts::{COOLDOWN_MS, WINDOWS_S};
use crate::detector::{Dir, Metrics};
use crate::levels::Side;

pub const MAX_RULES: usize = 1_000;
pub const MAX_CONDITIONS: usize = 8;
pub const TTL_DEFAULT_S: u64 = 86_400;
pub const TTL_MAX_S: u64 = 604_800;
pub const TTL_MIN_S: u64 = 60;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Owner {
    #[schemars(description = "Desk user id (UUID string) that owns the rule")]
    pub user_id: String,
    #[schemars(description = "Agent that created the rule, e.g. supervisor, watcher, analyst")]
    pub agent: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum TakerSide {
    Buy,
    Sell,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Condition {
    #[schemars(
        description = "Price crosses `level` in direction `dir` (it must have been on the other side first)"
    )]
    PriceCross { level: f64, dir: Dir },
    #[schemars(
        description = "Move over `window_s` (60, 300 or 900) is at least `min` standard deviations, optionally in `dir`"
    )]
    MoveZ {
        min: f64,
        window_s: u64,
        dir: Option<Dir>,
    },
    #[schemars(
        description = "Volume over `window_s` (60, 300 or 900) is at least `min` times its 24h median"
    )]
    VolumeX { min: f64, window_s: u64 },
    #[schemars(
        description = "Share of taker volume on `side` over `window_s` (60, 300 or 900) is at least `min` (0.5 to 1.0)"
    )]
    TakerImbalance {
        min: f64,
        side: TakerSide,
        window_s: u64,
    },
    #[schemars(
        description = "Liquidation notional in the last 60s is at least `min` times the 24h p95 per minute"
    )]
    LiqBurst { min: f64 },
    #[schemars(
        description = "5-minute open interest change z-score vs 7 days is at least `min` in absolute value, optionally in `dir`"
    )]
    OiChangeZ { min: f64, dir: Option<Dir> },
    #[schemars(description = "Current funding rate is above `rate` (e.g. 0.0005 = 0.05%)")]
    FundingAbove { rate: f64 },
}

impl Condition {
    fn needs_baselines(&self) -> bool {
        !matches!(
            self,
            Condition::PriceCross { .. } | Condition::FundingAbove { .. }
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct When {
    #[schemars(description = "Every condition must be true at the same time")]
    pub all: Vec<Condition>,
}

impl When {
    pub fn direction(&self) -> Option<Dir> {
        self.all.iter().find_map(|c| match c {
            Condition::PriceCross { dir, .. } => Some(*dir),
            Condition::MoveZ { dir, .. } | Condition::OiChangeZ { dir, .. } => *dir,
            Condition::TakerImbalance { side, .. } => Some(match side {
                TakerSide::Buy => Dir::Up,
                TakerSide::Sell => Dir::Down,
            }),
            _ => None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    Notify,
    Wake,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Plan {
    pub side: Side,
    #[schemars(description = "Planned entry price; optional")]
    pub entry: Option<f64>,
    #[schemars(description = "Stop loss: below entry for long, above for short")]
    pub sl: f64,
    #[schemars(description = "Take profit: above entry for long, below for short")]
    pub tp: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct RuleSpec {
    #[schemars(
        description = "Optional caller-chosen id (letters, digits, _ and -, max 64). Re-sending an existing id is a no-op, so sync can resend rules safely"
    )]
    pub rule_id: Option<String>,
    #[schemars(description = "USDT perpetual symbol, e.g. SOLUSDT")]
    pub symbol: String,
    #[schemars(description = "Conditions that must all hold")]
    pub when: When,
    #[schemars(
        description = "notify = heads-up flash alert (tier 1); wake = also run the watcher agent (tier 2). Never opens a trade"
    )]
    pub action: Action,
    #[schemars(
        description = "Why this rule exists; travels in the event so the woken agent knows the thesis"
    )]
    pub note: String,
    #[schemars(description = "Optional passive trade plan carried in the event")]
    pub plan: Option<Plan>,
    #[schemars(description = "Lifetime in seconds, default 86400, max 604800")]
    pub ttl_s: Option<u64>,
    #[schemars(
        description = "Fire once then expire (default true); false = fire again after a 15 minute cooldown"
    )]
    pub once: Option<bool>,
    #[schemars(description = "Who owns the rule; filled by the bot")]
    pub owner: Owner,
    #[schemars(description = "seq of the event that led to this rule, if any")]
    pub parent_seq: Option<u64>,
}

#[derive(Debug, thiserror::Error, PartialEq)]
#[error("invalid {field}: {reason}")]
pub struct RuleError {
    pub field: String,
    pub reason: String,
}

fn bad(field: impl Into<String>, reason: impl Into<String>) -> RuleError {
    RuleError {
        field: field.into(),
        reason: reason.into(),
    }
}

pub fn valid_symbol(s: &str) -> bool {
    s.len() > 4
        && s.len() <= 23
        && s.ends_with("USDT")
        && s.bytes()
            .all(|b| b.is_ascii_uppercase() || b.is_ascii_digit())
}

fn window_ok(field: String, w: u64) -> Result<(), RuleError> {
    if WINDOWS_S.contains(&w) {
        Ok(())
    } else {
        Err(bad(field, "must be 60, 300 or 900"))
    }
}

fn pos(field: String, v: f64) -> Result<(), RuleError> {
    if v.is_finite() && v > 0.0 {
        Ok(())
    } else {
        Err(bad(field, "must be a positive number"))
    }
}

impl RuleSpec {
    pub fn normalize(&mut self) {
        self.symbol = self.symbol.trim().to_ascii_uppercase();
    }

    pub fn once(&self) -> bool {
        self.once.unwrap_or(true)
    }

    pub fn ttl_s(&self) -> u64 {
        self.ttl_s.unwrap_or(TTL_DEFAULT_S)
    }

    pub fn validate(&self, known: impl Fn(&str) -> bool) -> Result<(), RuleError> {
        if let Some(id) = &self.rule_id
            && (id.is_empty()
                || id.len() > 64
                || !id
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-'))
        {
            return Err(bad("rule_id", "use 1-64 letters, digits, _ or -"));
        }
        if !valid_symbol(&self.symbol) || !known(&self.symbol) {
            return Err(bad(
                "symbol",
                format!("{} is not a known USDT perpetual", self.symbol),
            ));
        }
        if self.when.all.is_empty() || self.when.all.len() > MAX_CONDITIONS {
            return Err(bad(
                "when.all",
                format!("needs 1 to {MAX_CONDITIONS} conditions"),
            ));
        }
        for (i, c) in self.when.all.iter().enumerate() {
            let f = |name: &str| format!("when.all[{i}].{name}");
            match c {
                Condition::PriceCross { level, .. } => pos(f("level"), *level)?,
                Condition::MoveZ { min, window_s, .. } => {
                    pos(f("min"), *min)?;
                    window_ok(f("window_s"), *window_s)?
                }
                Condition::VolumeX { min, window_s } => {
                    pos(f("min"), *min)?;
                    window_ok(f("window_s"), *window_s)?
                }
                Condition::TakerImbalance { min, window_s, .. } => {
                    if !(min.is_finite() && *min > 0.5 && *min <= 1.0) {
                        return Err(bad(f("min"), "must be above 0.5 and at most 1.0"));
                    }
                    window_ok(f("window_s"), *window_s)?
                }
                Condition::LiqBurst { min } | Condition::OiChangeZ { min, .. } => {
                    pos(f("min"), *min)?
                }
                Condition::FundingAbove { rate } => {
                    if !rate.is_finite() || rate.abs() > 0.05 {
                        return Err(bad(
                            f("rate"),
                            "must be a funding rate between -0.05 and 0.05",
                        ));
                    }
                }
            }
        }
        if self.note.trim().is_empty() || self.note.len() > 2_000 {
            return Err(bad("note", "must be 1 to 2000 characters"));
        }
        if !(TTL_MIN_S..=TTL_MAX_S).contains(&self.ttl_s()) {
            return Err(bad(
                "ttl_s",
                format!("must be between {TTL_MIN_S} and {TTL_MAX_S}"),
            ));
        }
        if self.owner.agent.trim().is_empty() {
            return Err(bad("owner.agent", "must not be empty"));
        }
        if let Some(p) = &self.plan {
            pos("plan.sl".into(), p.sl)?;
            pos("plan.tp".into(), p.tp)?;
            if let Some(e) = p.entry {
                pos("plan.entry".into(), e)?;
            }
            let (lo, hi) = match p.side {
                Side::Long => (p.sl, p.tp),
                Side::Short => (p.tp, p.sl),
            };
            let entry_ok = p.entry.is_none_or(|e| lo < e && e < hi);
            if lo >= hi || !entry_ok {
                let msg = match p.side {
                    Side::Long => "long needs sl below entry and tp above entry",
                    Side::Short => "short needs sl above entry and tp below entry",
                };
                return Err(bad("plan", msg));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct RuleEntry {
    pub id: String,
    pub spec: RuleSpec,
    pub expires_ms: u64,
}

#[derive(Debug, Clone)]
pub struct RuleRt {
    pub entry: Arc<RuleEntry>,
    armed: Vec<bool>,
    last_fired: Option<u64>,
    pub done: bool,
}

impl RuleRt {
    pub fn new(entry: Arc<RuleEntry>) -> Self {
        let n = entry.spec.when.all.len();
        Self {
            entry,
            armed: vec![false; n],
            last_fired: None,
            done: false,
        }
    }

    pub fn replace(old: &mut Vec<RuleRt>, new: Vec<Arc<RuleEntry>>) {
        let prev = std::mem::take(old);
        old.extend(
            new.into_iter()
                .map(|e| match prev.iter().find(|r| r.entry.id == e.id) {
                    Some(r) => RuleRt {
                        entry: e,
                        ..r.clone()
                    },
                    None => RuleRt::new(e),
                }),
        );
    }

    pub fn levels(&self) -> impl Iterator<Item = f64> + '_ {
        self.entry.spec.when.all.iter().filter_map(|c| match c {
            Condition::PriceCross { level, .. } => Some(*level),
            _ => None,
        })
    }

    pub fn eval(&mut self, m: &Metrics) -> bool {
        if self.done || m.now_ms >= self.entry.expires_ms {
            return false;
        }
        let mut all = true;
        for (i, c) in self.entry.spec.when.all.iter().enumerate() {
            let ok = if c.needs_baselines() && !m.warm {
                false
            } else {
                match c {
                    Condition::PriceCross { level, dir } => {
                        let beyond = match dir {
                            Dir::Up => m.price >= *level,
                            Dir::Down => m.price <= *level,
                        };
                        if !beyond {
                            self.armed[i] = true;
                        }
                        self.armed[i] && beyond
                    }
                    Condition::MoveZ { min, window_s, dir } => {
                        m.window(*window_s).is_some_and(|w| {
                            w.z.abs() >= *min && dir.is_none_or(|d| Dir::of(w.z) == d)
                        })
                    }
                    Condition::VolumeX { min, window_s } => {
                        m.window(*window_s).is_some_and(|w| w.vol_x >= *min)
                    }
                    Condition::TakerImbalance {
                        min,
                        side,
                        window_s,
                    } => m.window(*window_s).is_some_and(|w| {
                        let d = if *side == TakerSide::Buy {
                            Dir::Up
                        } else {
                            Dir::Down
                        };
                        w.vol > 0.0 && w.taker_share(d) >= *min
                    }),
                    Condition::LiqBurst { min } => m.liq_x >= *min,
                    Condition::OiChangeZ { min, dir } => m
                        .oi_z
                        .is_some_and(|z| z.abs() >= *min && dir.is_none_or(|d| Dir::of(z) == d)),
                    Condition::FundingAbove { rate } => m.funding > *rate,
                }
            };
            all &= ok;
        }
        if !all || self.last_fired.is_some_and(|t| m.now_ms < t + COOLDOWN_MS) {
            return false;
        }
        self.last_fired = Some(m.now_ms);
        self.armed.iter_mut().for_each(|a| *a = false);
        self.done = self.entry.spec.once();
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    pub fn spec() -> RuleSpec {
        RuleSpec {
            rule_id: None,
            symbol: "SOLUSDT".into(),
            when: When {
                all: vec![Condition::PriceCross {
                    level: 150.0,
                    dir: Dir::Up,
                }],
            },
            action: Action::Wake,
            note: "reclaim".into(),
            plan: Some(Plan {
                side: Side::Long,
                entry: Some(150.0),
                sl: 146.0,
                tp: 158.0,
            }),
            ttl_s: None,
            once: None,
            owner: Owner {
                user_id: "42".into(),
                agent: "supervisor".into(),
            },
            parent_seq: None,
        }
    }

    #[test]
    fn validation_names_the_field() {
        assert!(spec().validate(|_| true).is_ok());
        assert_eq!(spec().validate(|_| false).unwrap_err().field, "symbol");
        let mut s = spec();
        s.plan = Some(Plan {
            side: Side::Long,
            entry: Some(150.0),
            sl: 152.0,
            tp: 158.0,
        });
        assert_eq!(s.validate(|_| true).unwrap_err().field, "plan");
        let mut s = spec();
        s.when.all.push(Condition::VolumeX {
            min: 2.0,
            window_s: 120,
        });
        assert_eq!(
            s.validate(|_| true).unwrap_err().field,
            "when.all[1].window_s"
        );
        let mut s = spec();
        s.ttl_s = Some(TTL_MAX_S + 1);
        assert_eq!(s.validate(|_| true).unwrap_err().field, "ttl_s");
        let mut s = spec();
        s.plan = Some(Plan {
            side: Side::Short,
            entry: None,
            sl: 160.0,
            tp: 140.0,
        });
        assert!(s.validate(|_| true).is_ok());
    }

    #[test]
    fn price_cross_needs_the_other_side_first_and_once_fires_once() {
        let mut r = RuleRt::new(Arc::new(RuleEntry {
            id: "r_1".into(),
            spec: spec(),
            expires_ms: u64::MAX,
        }));
        let m = |price, now_ms| Metrics {
            price,
            now_ms,
            warm: true,
            ..Default::default()
        };
        assert!(!r.eval(&m(151.0, 0)), "already above at creation");
        assert!(!r.eval(&m(149.0, 1)));
        assert!(r.eval(&m(150.5, 2)));
        assert!(!r.eval(&m(149.0, 3)));
        assert!(!r.eval(&m(151.0, COOLDOWN_MS * 2)));
        assert!(r.done);
    }
}
