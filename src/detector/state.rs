use super::baselines::{self, Baselines};
use super::consts::*;
use crate::ring::Ring;

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct Bar {
    pub t: u64,
    pub o: f64,
    pub h: f64,
    pub l: f64,
    pub c: f64,
    pub vol: f64,
    pub buy_vol: f64,
    pub trades: u32,
    pub liq_long: f64,
    pub liq_short: f64,
}

impl Bar {
    pub fn flat(t: u64, px: f64) -> Self {
        Self {
            t,
            o: px,
            h: px,
            l: px,
            c: px,
            ..Default::default()
        }
    }

    fn add(&mut self, px: f64, qty: f64, taker_sell: bool) {
        self.h = self.h.max(px);
        self.l = self.l.min(px);
        self.c = px;
        self.vol += qty;
        if !taker_sell {
            self.buy_vol += qty;
        }
        self.trades += 1;
    }
}

#[derive(Debug, Clone)]
pub struct SymbolState {
    pub bars1s: Ring<Bar>,
    pub cur1s: Bar,
    pub bars1m: Ring<Bar>,
    pub sigma1m: Ring<f64>,
    pub cur1m: Bar,
    pub base: Baselines,
    pub last_px: f64,
    pub last_agg: u64,
    pub mark: f64,
    pub funding: f64,
    pub oi_hist: Ring<(u64, f64)>,
    pub oi_live: Ring<(u64, f64)>,
    pub warm_from_ms: u64,
    scratch: Vec<f64>,
}

impl SymbolState {
    pub fn new(now_ms: u64) -> Self {
        Self {
            bars1s: Ring::new(BARS_1S),
            cur1s: Bar::default(),
            bars1m: Ring::new(BARS_1M),
            sigma1m: Ring::new(BARS_1M),
            cur1m: Bar::default(),
            base: Baselines::default(),
            last_px: 0.0,
            last_agg: 0,
            mark: 0.0,
            funding: 0.0,
            oi_hist: Ring::new(OI_HIST_CAP),
            oi_live: Ring::new(OI_LIVE_CAP),
            warm_from_ms: now_ms + WARMUP_MS,
            scratch: Vec::with_capacity(4 * ATR_BARS.max(DAY_1M)),
        }
    }

    pub fn warm(&self, now_ms: u64) -> bool {
        now_ms >= self.warm_from_ms
    }

    pub fn rewarm(&mut self, now_ms: u64) {
        self.warm_from_ms = self.warm_from_ms.max(now_ms + WARMUP_MS);
    }

    pub fn atr(&self) -> f64 {
        self.base.atr.max(self.last_px * ATR_FLOOR_FRAC)
    }

    fn start(&mut self, ts: u64, px: f64) {
        self.last_px = px;
        self.cur1s = Bar::flat(ts - ts % SEC_MS, px);
        self.cur1m = Bar::flat(ts - ts % MIN_MS, px);
    }

    pub fn on_trade(&mut self, agg: u64, px: f64, qty: f64, taker_sell: bool, ts: u64) -> bool {
        if !(px.is_finite() && px > 0.0 && qty.is_finite() && qty >= 0.0)
            || (agg != 0 && agg <= self.last_agg)
        {
            return false;
        }
        self.last_agg = self.last_agg.max(agg);
        if self.last_px <= 0.0 {
            self.start(ts, px);
        }
        self.roll(ts);
        self.cur1s.add(px, qty, taker_sell);
        self.cur1m.add(px, qty, taker_sell);
        self.last_px = px;
        true
    }

    pub fn on_liq(&mut self, usd: f64, long_liquidated: bool) {
        if self.last_px <= 0.0 || !usd.is_finite() || usd <= 0.0 {
            return;
        }
        for b in [&mut self.cur1s, &mut self.cur1m] {
            if long_liquidated {
                b.liq_long += usd
            } else {
                b.liq_short += usd
            }
        }
    }

    pub fn on_mark(&mut self, mark: f64, funding: f64) {
        if mark.is_finite() && mark > 0.0 {
            self.mark = mark;
        }
        if funding.is_finite() {
            self.funding = funding;
        }
    }

    pub fn roll(&mut self, now_ms: u64) {
        if self.last_px <= 0.0 {
            return;
        }
        let sec = now_ms - now_ms % SEC_MS;
        if sec > self.cur1s.t {
            let gap = ((sec - self.cur1s.t) / SEC_MS) as usize;
            self.bars1s.push(self.cur1s);
            let fill = (gap - 1).min(BARS_1S);
            let c = self.cur1s.c;
            for i in (1..=fill).rev() {
                self.bars1s.push(Bar::flat(sec - i as u64 * SEC_MS, c));
            }
            self.cur1s = Bar::flat(sec, self.last_px);
        }
        let min = now_ms - now_ms % MIN_MS;
        if min > self.cur1m.t {
            let gap = ((min - self.cur1m.t) / MIN_MS) as usize;
            self.push_minute(self.cur1m);
            let fill = (gap - 1).min(BARS_1M);
            let c = self.cur1m.c;
            for i in (1..=fill).rev() {
                self.push_minute(Bar::flat(min - i as u64 * MIN_MS, c));
            }
            self.cur1m = Bar::flat(min, self.last_px);
            self.refresh();
        }
    }

    fn push_minute(&mut self, b: Bar) {
        if let Some(prev) = self
            .bars1m
            .newest()
            .map(|p| p.c)
            .filter(|c| *c > 0.0 && b.c > 0.0)
        {
            let r = (b.c / prev).ln();
            self.base.ewma_var = baselines::ewma(self.base.ewma_var, self.base.ewma_n, r);
            self.base.ewma_n += 1;
        }
        self.bars1m.push(b);
        self.sigma1m.push(self.base.ewma_var.sqrt());
    }

    fn refresh(&mut self) {
        self.base.vol_median = baselines::window_medians(&self.bars1m, &mut self.scratch);
        self.base.liq_p95 = baselines::liq_p95(&self.bars1m, &mut self.scratch);
        self.base.atr = baselines::atr(&self.bars1m, &mut self.scratch);
    }

    pub fn merge_1m(&mut self, rest: &[Bar], now_ms: u64) {
        let live_from = if self.last_px > 0.0 {
            self.cur1m.t
        } else {
            u64::MAX
        };
        let mut v: Vec<Bar> = self.bars1m.iter().copied().collect();
        for b in rest
            .iter()
            .filter(|b| b.t + MIN_MS <= now_ms && b.t < live_from && b.c > 0.0)
        {
            match v.binary_search_by_key(&b.t, |x| x.t) {
                Ok(i) => {
                    v[i] = Bar {
                        liq_long: v[i].liq_long,
                        liq_short: v[i].liq_short,
                        ..*b
                    }
                }
                Err(i) => v.insert(i, *b),
            }
        }
        let skip = v.len().saturating_sub(BARS_1M);
        self.bars1m.clear();
        self.sigma1m.clear();
        self.base.ewma_var = 0.0;
        self.base.ewma_n = 0;
        for b in &v[skip..] {
            self.push_minute(*b);
        }
        self.refresh();
        if self.last_px <= 0.0
            && let Some(last) = v.last()
        {
            self.last_px = last.c;
            self.cur1m = Bar::flat(last.t + MIN_MS, last.c);
            self.cur1s = Bar::flat(now_ms - now_ms % SEC_MS, last.c);
            self.roll(now_ms);
        }
    }

    pub fn sigma_at(&self, start_ms: u64) -> Option<f64> {
        (0..self.bars1m.len()).find_map(|j| {
            let b = self.bars1m.back(j)?;
            (b.t + MIN_MS <= start_ms)
                .then(|| self.sigma1m.back(j).copied())
                .flatten()
        })
    }

    pub fn on_oi(&mut self, ts: u64, oi: f64) {
        if !(oi.is_finite() && oi > 0.0) {
            return;
        }
        self.oi_live.push((ts, oi));
        if self
            .oi_hist
            .newest()
            .is_none_or(|(t, _)| ts >= t + OI_STEP_MS)
        {
            self.oi_hist.push((ts, oi));
            self.refresh_oi();
        }
    }

    pub fn set_oi_hist(&mut self, mut pts: Vec<(u64, f64)>) {
        pts.sort_by_key(|p| p.0);
        let live: Vec<(u64, f64)> = self
            .oi_hist
            .iter()
            .copied()
            .filter(|p| pts.last().is_none_or(|l| p.0 > l.0))
            .collect();
        self.oi_hist.clear();
        for p in pts
            .into_iter()
            .chain(live)
            .filter(|p| p.1.is_finite() && p.1 > 0.0)
        {
            self.oi_hist.push(p);
        }
        self.refresh_oi();
    }

    fn refresh_oi(&mut self) {
        match baselines::oi_stats(&self.oi_hist) {
            Some((m, sd)) => {
                (self.base.oi_mean, self.base.oi_sd, self.base.oi_ready) = (m, sd, true)
            }
            None => self.base.oi_ready = false,
        }
    }

    pub fn oi_change(&self) -> Option<f64> {
        let &(t1, v1) = self.oi_live.newest()?;
        let target = t1.checked_sub(OI_STEP_MS)?;
        let v0 = self
            .oi_live
            .iter()
            .rev()
            .find(|p| p.0 <= target)
            .or_else(|| {
                self.oi_hist
                    .iter()
                    .rev()
                    .find(|p| p.0 <= target && p.0 + OI_STEP_MS >= target)
            })
            .map(|p| p.1)?;
        (v0 > 0.0).then(|| (v1 / v0).ln())
    }
}
