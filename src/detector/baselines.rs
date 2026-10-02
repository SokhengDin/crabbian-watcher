use super::consts::*;
use super::state::Bar;
use crate::ring::Ring;

#[derive(Debug, Clone, Copy, Default)]
pub struct Baselines {
    pub ewma_var: f64,
    pub ewma_n: u64,
    pub vol_median: [f64; 3],
    pub liq_p95: f64,
    pub atr: f64,
    pub oi_mean: f64,
    pub oi_sd: f64,
    pub oi_ready: bool,
}

pub fn ewma(var: f64, n: u64, r: f64) -> f64 {
    let a = 2.0 / (EWMA_SPAN_MIN + 1.0);
    if n == 0 {
        r * r
    } else {
        (1.0 - a) * var + a * r * r
    }
}

pub fn quantile(buf: &mut [f64], q: f64) -> f64 {
    if buf.is_empty() {
        return 0.0;
    }
    let k = ((buf.len() - 1) as f64 * q).round() as usize;
    *buf.select_nth_unstable_by(k, f64::total_cmp).1
}

pub fn window_medians(bars: &Ring<Bar>, scratch: &mut Vec<f64>) -> [f64; 3] {
    let n = bars.len().min(DAY_1M);
    let mut out = [0.0; 3];
    for (i, w) in WINDOWS_S.iter().enumerate() {
        let block = (*w / 60) as usize;
        scratch.clear();
        let mut k = 0;
        while k + block <= n {
            scratch.push(
                (k..k + block)
                    .filter_map(|j| bars.back(j))
                    .map(|b| b.vol)
                    .sum(),
            );
            k += block;
        }
        out[i] = quantile(scratch, 0.5);
    }
    out
}

pub fn liq_p95(bars: &Ring<Bar>, scratch: &mut Vec<f64>) -> f64 {
    scratch.clear();
    scratch.extend(
        (0..bars.len().min(DAY_1M))
            .filter_map(|j| bars.back(j))
            .map(|b| b.liq_long + b.liq_short),
    );
    quantile(scratch, 0.95)
}

pub fn atr(bars: &Ring<Bar>, scratch: &mut Vec<f64>) -> f64 {
    let n = bars.len().min(ATR_BARS);
    if n <= ATR_PERIOD {
        return 0.0;
    }
    scratch.clear();
    for j in (0..n).rev() {
        if let Some(b) = bars.back(j) {
            scratch.push(b.h);
        }
    }
    for j in (0..n).rev() {
        if let Some(b) = bars.back(j) {
            scratch.push(b.l);
        }
    }
    for j in (0..n).rev() {
        if let Some(b) = bars.back(j) {
            scratch.push(b.c);
        }
    }
    scratch.resize(4 * n, 0.0);
    let (hlc, out) = scratch.split_at_mut(3 * n);
    let (h, lc) = hlc.split_at(n);
    let (l, c) = lc.split_at(n);
    match kand::ohlcv::atr::atr(h, l, c, ATR_PERIOD, out) {
        Ok(()) => out.last().copied().filter(|v| v.is_finite()).unwrap_or(0.0),
        Err(_) => 0.0,
    }
}

pub fn oi_stats(hist: &Ring<(u64, f64)>) -> Option<(f64, f64)> {
    let (mut n, mut sum, mut sq) = (0usize, 0.0, 0.0);
    let mut prev: Option<(u64, f64)> = None;
    for &(t, v) in hist.iter() {
        if let Some((pt, pv)) = prev {
            let dt = t.saturating_sub(pt);
            if pv > 0.0 && v > 0.0 && (OI_STEP_MS * 4 / 5..=OI_STEP_MS * 6 / 5).contains(&dt) {
                let d = (v / pv).ln();
                n += 1;
                sum += d;
                sq += d * d;
            }
        }
        prev = Some((t, v));
    }
    if n < OI_MIN_SAMPLES {
        return None;
    }
    let mean = sum / n as f64;
    Some((mean, (sq / n as f64 - mean * mean).max(0.0).sqrt()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn quantile_and_ewma() {
        let mut v = vec![5.0, 1.0, 3.0, 2.0, 4.0];
        assert_eq!(quantile(&mut v, 0.5), 3.0);
        assert_eq!(quantile(&mut [], 0.5), 0.0);
        assert_eq!(ewma(0.0, 0, 0.01), 0.0001);
        assert!(ewma(0.0001, 10, 0.0) < 0.0001);
    }

    #[test]
    fn atr_on_constant_range() {
        let mut r = Ring::new(200);
        for i in 0..60 {
            r.push(Bar {
                t: i * MIN_MS,
                o: 100.0,
                h: 101.0,
                l: 99.0,
                c: 100.0,
                ..Default::default()
            });
        }
        let a = atr(&r, &mut Vec::new());
        assert!((a - 2.0).abs() < 1e-9, "{a}");
    }
}
