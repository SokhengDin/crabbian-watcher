use schemars::JsonSchema;
use serde::Serialize;

use super::consts::*;
use super::state::SymbolState;
use super::{Dir, ratio};

#[derive(Debug, Clone, Copy, Default)]
pub struct Window {
    pub ok: bool,
    pub start_px: f64,
    pub ln_move: f64,
    pub sigma_k: f64,
    pub z: f64,
    pub vol: f64,
    pub vol_x: f64,
    pub taker_buy: f64,
}

impl Window {
    pub fn move_pct(&self) -> f64 {
        self.ln_move.exp_m1() * 100.0
    }

    pub fn taker_share(&self, d: Dir) -> f64 {
        match d {
            Dir::Up => self.taker_buy,
            Dir::Down => 1.0 - self.taker_buy,
        }
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Metrics {
    pub now_ms: u64,
    pub price: f64,
    pub w: [Window; 3],
    pub sigma_1m: f64,
    pub liq_long: f64,
    pub liq_short: f64,
    pub liq_x: f64,
    pub oi_chg: Option<f64>,
    pub oi_z: Option<f64>,
    pub funding: f64,
    pub mark: f64,
    pub warm: bool,
}

impl Metrics {
    pub fn liq_usd(&self) -> f64 {
        self.liq_long + self.liq_short
    }

    pub fn window(&self, window_s: u64) -> Option<&Window> {
        WINDOWS_S
            .iter()
            .position(|w| *w == window_s)
            .map(|i| &self.w[i])
            .filter(|w| w.ok)
    }
}

pub fn measure(st: &SymbolState, now_ms: u64) -> Option<Metrics> {
    let price = st.last_px;
    if price <= 0.0 {
        return None;
    }
    let mut m = Metrics {
        now_ms,
        price,
        funding: st.funding,
        mark: st.mark,
        warm: st.warm(now_ms),
        ..Default::default()
    };
    m.sigma_1m = st.base.ewma_var.sqrt();
    for (i, &k) in WINDOWS_S.iter().enumerate() {
        let start = now_ms.saturating_sub(k * SEC_MS);
        let (mut vol, mut buy, mut ll, mut ls) = (
            st.cur1s.vol,
            st.cur1s.buy_vol,
            st.cur1s.liq_long,
            st.cur1s.liq_short,
        );
        let mut start_px = None;
        for b in st.bars1s.iter().rev() {
            if b.t + SEC_MS <= start {
                start_px = Some(b.c);
                break;
            }
            vol += b.vol;
            buy += b.buy_vol;
            ll += b.liq_long;
            ls += b.liq_short;
        }
        if k == LIQ_WINDOW_S {
            (m.liq_long, m.liq_short) = (ll, ls);
        }
        let (Some(p0), Some(sig)) = (start_px.filter(|p| *p > 0.0), st.sigma_at(start)) else {
            continue;
        };
        let sigma_k = (k as f64 / 60.0).sqrt() * sig.max(SIGMA_FLOOR);
        let ln_move = (price / p0).ln();
        m.w[i] = Window {
            ok: true,
            start_px: p0,
            ln_move,
            sigma_k,
            z: ln_move / sigma_k,
            vol,
            vol_x: ratio(vol, st.base.vol_median[i], VOL_FLOOR),
            taker_buy: if vol > VOL_FLOOR { buy / vol } else { 0.5 },
        };
    }
    m.liq_x = ratio(m.liq_usd(), st.base.liq_p95.max(LIQ_P95_FLOOR_USD), 0.0);
    m.oi_chg = st.oi_change();
    if st.base.oi_ready {
        m.oi_z = m
            .oi_chg
            .map(|c| (c - st.base.oi_mean) / st.base.oi_sd.max(OI_SIGMA_FLOOR));
    }
    Some(m)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    Jump,
    Liq,
    Oi,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Confirmer {
    Volume,
    Taker,
    Liq,
    Oi,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Candidate {
    pub kind: Kind,
    pub dir: Dir,
    pub win: usize,
    pub confirmers: Vec<Confirmer>,
    pub tier: u8,
}

fn confirm(m: &Metrics, w: &Window, d: Dir, skip: Kind) -> Vec<Confirmer> {
    let mut c = Vec::new();
    if w.vol_x >= VOL_SURGE_X {
        c.push(Confirmer::Volume);
    }
    if w.taker_share(d) >= TAKER_SHARE {
        c.push(Confirmer::Taker);
    }
    if skip != Kind::Liq && m.liq_x >= LIQ_BURST_X {
        c.push(Confirmer::Liq);
    }
    if skip != Kind::Oi && m.oi_z.is_some_and(|z| z.abs() >= OI_SHOCK_Z) {
        c.push(Confirmer::Oi);
    }
    c
}

pub fn detect(m: &Metrics) -> Option<Candidate> {
    let tier = |conf: &[Confirmer]| {
        let big_z = m.w.iter().any(|w| w.ok && w.z.abs() >= TIER2_Z);
        let flow = conf
            .iter()
            .any(|c| matches!(c, Confirmer::Volume | Confirmer::Taker));
        let others = conf
            .iter()
            .filter(|c| matches!(c, Confirmer::Liq | Confirmer::Oi))
            .count();
        if big_z || flow as usize + others >= TIER2_CONFIRMERS {
            2
        } else {
            1
        }
    };
    let jump = (0..3)
        .filter(|&i| {
            m.w[i].ok && m.w[i].z.abs() >= JUMP_Z[i] && m.w[i].ln_move.abs() >= JUMP_MIN_MOVE
        })
        .max_by(|&a, &b| (m.w[a].z.abs() / JUMP_Z[a]).total_cmp(&(m.w[b].z.abs() / JUMP_Z[b])));
    if let Some(win) = jump {
        let w = &m.w[win];
        let dir = Dir::of(w.ln_move);
        let confirmers = confirm(m, w, dir, Kind::Jump);
        if confirmers
            .iter()
            .any(|c| matches!(c, Confirmer::Volume | Confirmer::Taker))
        {
            return Some(Candidate {
                kind: Kind::Jump,
                dir,
                win,
                tier: tier(&confirmers),
                confirmers,
            });
        }
    }
    if m.liq_x >= LIQ_BURST_X && m.w[0].ok {
        let dir = if m.liq_long >= m.liq_short {
            Dir::Down
        } else {
            Dir::Up
        };
        let confirmers = confirm(m, &m.w[0], dir, Kind::Liq);
        if !confirmers.is_empty() {
            return Some(Candidate {
                kind: Kind::Liq,
                dir,
                win: 0,
                tier: tier(&confirmers),
                confirmers,
            });
        }
    }
    if m.oi_z.is_some_and(|z| z.abs() >= OI_SHOCK_Z) && m.w[1].ok {
        let dir = Dir::of(m.w[1].ln_move);
        let confirmers = confirm(m, &m.w[1], dir, Kind::Oi);
        if !confirmers.is_empty() {
            return Some(Candidate {
                kind: Kind::Oi,
                dir,
                win: 1,
                tier: tier(&confirmers),
                confirmers,
            });
        }
    }
    None
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OiRead {
    NewLongs,
    ShortsBuilding,
    ShortSqueeze,
    LongsClosing,
}

pub fn oi_read(m: &Metrics, dir: Dir) -> Option<OiRead> {
    let z = m.oi_z?;
    if z.abs() < OI_READ_Z {
        return None;
    }
    Some(match (dir, z > 0.0) {
        (Dir::Up, true) => OiRead::NewLongs,
        (Dir::Down, true) => OiRead::ShortsBuilding,
        (Dir::Up, false) => OiRead::ShortSqueeze,
        (Dir::Down, false) => OiRead::LongsClosing,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn jump(z: f64, vol_x: f64, taker_buy: f64, liq_x: f64) -> Metrics {
        let mut m = Metrics {
            price: 100.0,
            warm: true,
            liq_x,
            liq_short: if liq_x > 0.0 { 1e6 } else { 0.0 },
            ..Default::default()
        };
        m.w[1] = Window {
            ok: true,
            start_px: 98.0,
            ln_move: 0.02,
            sigma_k: 0.02 / z,
            z,
            vol: 10.0,
            vol_x,
            taker_buy,
        };
        m
    }

    #[test]
    fn volume_and_taker_count_as_one_confirmer() {
        let c = detect(&jump(4.0, 5.0, 0.9, 0.0)).unwrap();
        assert_eq!(c.confirmers, vec![Confirmer::Volume, Confirmer::Taker]);
        assert_eq!(c.tier, 1);
    }

    #[test]
    fn flow_plus_liquidations_is_tier_two() {
        let c = detect(&jump(4.0, 5.0, 0.9, 3.0)).unwrap();
        assert_eq!(c.tier, 2);
    }

    #[test]
    fn a_very_large_z_is_tier_two_on_flow_alone() {
        let c = detect(&jump(7.0, 5.0, 0.5, 0.0)).unwrap();
        assert_eq!((c.confirmers, c.tier), (vec![Confirmer::Volume], 2));
    }

    #[test]
    fn a_jump_without_flow_is_dropped() {
        assert!(detect(&jump(4.0, 1.0, 0.5, 0.0)).is_none());
    }
}
