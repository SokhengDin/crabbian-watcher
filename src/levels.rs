use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::detector::Dir;
use crate::detector::consts::{LEVEL_NEAR_ATR, LEVEL_REARM_ATR, THESIS_REPEAT_MS};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LevelKind {
    Thesis,
    Sl,
    Tp,
    Liquidation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Side {
    Long,
    Short,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, JsonSchema)]
pub struct Level {
    #[schemars(description = "USDT perpetual symbol, e.g. SOLUSDT")]
    pub symbol: String,
    #[schemars(description = "Price of the level")]
    pub price: f64,
    #[schemars(
        description = "thesis (a key level from a strategy), sl, tp or liquidation (of an open trade)"
    )]
    pub kind: LevelKind,
    #[schemars(description = "Side of the trade the level belongs to, if any")]
    pub side: Option<Side>,
    #[schemars(
        description = "Id (UUID string) of the open trade the level belongs to; set it for sl, tp and liquidation levels"
    )]
    pub trade_id: Option<String>,
}

impl Level {
    pub fn protects(&self) -> bool {
        matches!(
            self.kind,
            LevelKind::Sl | LevelKind::Tp | LevelKind::Liquidation
        )
    }

    pub fn of_trade(&self) -> bool {
        self.trade_id.is_some() || self.protects()
    }

    fn same(&self, o: &Level) -> bool {
        self.kind == o.kind
            && self.trade_id == o.trade_id
            && self.price.to_bits() == o.price.to_bits()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Touch {
    Cross,
    Near,
}

#[derive(Debug, Clone, PartialEq, Serialize, JsonSchema)]
pub struct LevelHit {
    pub kind: LevelKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub trade_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub side: Option<Side>,
    pub price: f64,
    #[schemars(
        description = "cross: price reached or passed the level; near: price first came within 0.25 ATR of it"
    )]
    pub touch: Touch,
    pub direction: Dir,
}

#[derive(Debug, Clone)]
pub struct LevelWatch {
    pub level: Level,
    side: i8,
    cross_armed: bool,
    near_armed: bool,
    last_touch_ms: Option<u64>,
}

impl LevelWatch {
    pub fn new(level: Level) -> Self {
        Self {
            level,
            side: 0,
            cross_armed: true,
            near_armed: true,
            last_touch_ms: None,
        }
    }

    fn report(&mut self, now_ms: u64) -> bool {
        if self.level.kind != LevelKind::Thesis {
            return true;
        }
        let quiet = self
            .last_touch_ms
            .is_some_and(|t| now_ms < t + THESIS_REPEAT_MS);
        self.last_touch_ms = Some(now_ms);
        !quiet
    }
}

pub fn replace(old: &mut Vec<LevelWatch>, new: Vec<Level>) {
    let prev = std::mem::take(old);
    old.extend(new.into_iter().map(|l| {
        prev.iter()
            .find(|w| w.level.same(&l))
            .cloned()
            .unwrap_or_else(|| LevelWatch::new(l))
    }));
}

pub fn check(watches: &mut [LevelWatch], px: f64, atr: f64, now_ms: u64, out: &mut Vec<LevelHit>) {
    let band = LEVEL_NEAR_ATR * atr;
    for w in watches.iter_mut() {
        let lv = w.level.price;
        let d = px - lv;
        if w.side == 0 {
            w.side = if d >= 0.0 { 1 } else { -1 };
            w.near_armed = d.abs() > band;
            continue;
        }
        let crossed = (w.side > 0 && px <= lv) || (w.side < 0 && px >= lv);
        if crossed {
            w.side = -w.side;
            if w.cross_armed {
                if w.report(now_ms) {
                    out.push(hit(
                        &w.level,
                        Touch::Cross,
                        if w.side > 0 { Dir::Up } else { Dir::Down },
                    ));
                }
                w.cross_armed = false;
                w.near_armed = false;
            }
        } else if d.abs() > LEVEL_REARM_ATR * atr {
            w.cross_armed = true;
            w.near_armed = true;
        } else if d.abs() <= band && w.near_armed {
            if w.report(now_ms) {
                out.push(hit(
                    &w.level,
                    Touch::Near,
                    if d < 0.0 { Dir::Up } else { Dir::Down },
                ));
            }
            w.near_armed = false;
        }
    }
}

fn hit(l: &Level, touch: Touch, direction: Dir) -> LevelHit {
    LevelHit {
        kind: l.kind,
        trade_id: l.trade_id.clone(),
        side: l.side,
        price: l.price,
        touch,
        direction,
    }
}

pub fn trip_prices(watches: &[LevelWatch], atr: f64) -> impl Iterator<Item = f64> + '_ {
    let band = LEVEL_NEAR_ATR * atr;
    watches
        .iter()
        .flat_map(move |w| [w.level.price, w.level.price - band, w.level.price + band])
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sl() -> Level {
        Level {
            symbol: "SOLUSDT".into(),
            price: 100.0,
            kind: LevelKind::Sl,
            side: Some(Side::Long),
            trade_id: Some("7".into()),
        }
    }

    #[test]
    fn near_then_cross_then_no_chatter() {
        let mut w = vec![LevelWatch::new(sl())];
        let mut out = Vec::new();
        for px in [110.0, 105.0, 100.4] {
            check(&mut w, px, 2.0, 0, &mut out);
        }
        assert_eq!(out.len(), 1);
        assert_eq!((out[0].touch, out[0].direction), (Touch::Near, Dir::Down));
        check(&mut w, 99.9, 2.0, 0, &mut out);
        assert_eq!((out[1].touch, out[1].direction), (Touch::Cross, Dir::Down));
        for px in [100.1, 99.9, 100.2, 99.8] {
            check(&mut w, px, 2.0, 0, &mut out);
        }
        assert_eq!(out.len(), 2, "chatter around the level is suppressed");
        check(&mut w, 98.0, 2.0, 0, &mut out);
        check(&mut w, 100.0, 2.0, 0, &mut out);
        assert_eq!(out.len(), 3, "re-armed after moving 0.5 ATR away");
    }

    #[test]
    fn replace_keeps_state_for_same_level() {
        let mut w = vec![LevelWatch::new(sl())];
        check(&mut w, 110.0, 2.0, 0, &mut Vec::new());
        replace(&mut w, vec![sl()]);
        let mut out = Vec::new();
        check(&mut w, 99.0, 2.0, 0, &mut out);
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn thesis_level_reports_once_until_price_stays_away_an_hour() {
        let thesis = Level {
            kind: LevelKind::Thesis,
            side: None,
            trade_id: None,
            ..sl()
        };
        let mut w = vec![LevelWatch::new(thesis), LevelWatch::new(sl())];
        let mut out = Vec::new();
        let min = 60_000;
        check(&mut w, 110.0, 2.0, 0, &mut out);
        check(&mut w, 99.0, 2.0, min, &mut out);
        assert_eq!(out.len(), 2, "first cross reports both levels");
        check(&mut w, 97.0, 2.0, 2 * min, &mut out);
        check(&mut w, 101.0, 2.0, 30 * min, &mut out);
        assert_eq!(out.len(), 3, "repeat inside the hour: only the stop loss");
        assert_eq!(out[2].kind, LevelKind::Sl);
        check(&mut w, 104.0, 2.0, 31 * min, &mut out);
        check(&mut w, 99.0, 2.0, 80 * min, &mut out);
        assert_eq!(
            out.len(),
            4,
            "the suppressed touch at 30 min restarted the hour"
        );
        check(&mut w, 97.0, 2.0, 81 * min, &mut out);
        check(&mut w, 101.0, 2.0, 141 * min, &mut out);
        assert_eq!(
            out.len(),
            6,
            "an hour after the last touch the thesis level reports again"
        );
    }
}
