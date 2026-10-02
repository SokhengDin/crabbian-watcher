use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::detector::Dir;
use crate::detector::consts::{LEVEL_NEAR_ATR, LEVEL_REARM_ATR};

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
}

impl LevelWatch {
    pub fn new(level: Level) -> Self {
        Self {
            level,
            side: 0,
            cross_armed: true,
            near_armed: true,
        }
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

pub fn check(watches: &mut [LevelWatch], px: f64, atr: f64, out: &mut Vec<LevelHit>) {
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
                out.push(hit(
                    &w.level,
                    Touch::Cross,
                    if w.side > 0 { Dir::Up } else { Dir::Down },
                ));
                w.cross_armed = false;
                w.near_armed = false;
            }
        } else if d.abs() > LEVEL_REARM_ATR * atr {
            w.cross_armed = true;
            w.near_armed = true;
        } else if d.abs() <= band && w.near_armed {
            out.push(hit(
                &w.level,
                Touch::Near,
                if d < 0.0 { Dir::Up } else { Dir::Down },
            ));
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
            check(&mut w, px, 2.0, &mut out);
        }
        assert_eq!(out.len(), 1);
        assert_eq!((out[0].touch, out[0].direction), (Touch::Near, Dir::Down));
        check(&mut w, 99.9, 2.0, &mut out);
        assert_eq!((out[1].touch, out[1].direction), (Touch::Cross, Dir::Down));
        for px in [100.1, 99.9, 100.2, 99.8] {
            check(&mut w, px, 2.0, &mut out);
        }
        assert_eq!(out.len(), 2, "chatter around the level is suppressed");
        check(&mut w, 98.0, 2.0, &mut out);
        check(&mut w, 100.0, 2.0, &mut out);
        assert_eq!(out.len(), 3, "re-armed after moving 0.5 ATR away");
    }

    #[test]
    fn replace_keeps_state_for_same_level() {
        let mut w = vec![LevelWatch::new(sl())];
        check(&mut w, 110.0, 2.0, &mut Vec::new());
        replace(&mut w, vec![sl()]);
        let mut out = Vec::new();
        check(&mut w, 99.0, 2.0, &mut out);
        assert_eq!(out.len(), 1);
    }
}
