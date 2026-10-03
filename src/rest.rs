use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use binance_sdk::config::ConfigurationRestApi;
use binance_sdk::derivatives_trading_usds_futures::DerivativesTradingUsdsFuturesRestApi;
use binance_sdk::derivatives_trading_usds_futures::rest_api::{
    KlineCandlestickDataIntervalEnum, KlineCandlestickDataItemInner, KlineCandlestickDataParams,
    OpenInterestParams, OpenInterestStatisticsParams, OpenInterestStatisticsPeriodEnum, RestApi,
};
use tokio::sync::{mpsc, watch};
use tokio_util::sync::CancellationToken;

use crate::control::{Cmd, Wanted};
use crate::detector::Bar;
use crate::detector::consts::{BACKFILL_1M, MIN_MS, OI_STEP_MS};
use crate::shard::{Ctrl, Router, now_ms};

const KLINE_PAGE: i64 = 1_000;
const OI_PAGE: i64 = 500;
const OI_HIST_MS: u64 = 7 * 86_400_000;
const OI_POLL: Duration = Duration::from_secs(30);
const SYMBOLS_REFRESH: Duration = Duration::from_secs(3_600);
const GAP_PAD_MS: u64 = 2 * MIN_MS;

#[derive(Debug, thiserror::Error)]
pub enum RestError {
    #[error("binance request failed: {0}")]
    Request(String),
    #[error("unexpected response: {0}")]
    Shape(String),
}

#[derive(Debug, Clone)]
pub enum RestJob {
    Backfill(String),
    GapFill { since_ms: u64 },
}

pub struct Rest {
    api: RestApi,
}

fn req(e: impl std::fmt::Display) -> RestError {
    RestError::Request(e.to_string())
}

fn num(v: Option<&KlineCandlestickDataItemInner>) -> Option<f64> {
    match v? {
        KlineCandlestickDataItemInner::String(s) => s.parse().ok(),
        KlineCandlestickDataItemInner::Integer(i) => Some(*i as f64),
        KlineCandlestickDataItemInner::Other(v) => v.as_f64(),
    }
}

fn int(v: Option<&KlineCandlestickDataItemInner>) -> Option<u64> {
    match v? {
        KlineCandlestickDataItemInner::Integer(i) => u64::try_from(*i).ok(),
        KlineCandlestickDataItemInner::String(s) => s.parse().ok(),
        KlineCandlestickDataItemInner::Other(v) => v.as_u64(),
    }
}

pub fn is_perpetual(
    contract_type: Option<&str>,
    quote: Option<&str>,
    status: Option<&str>,
) -> bool {
    contract_type.is_some_and(|t| t.ends_with("PERPETUAL"))
        && quote == Some("USDT")
        && status == Some("TRADING")
}

pub fn kline_bar(k: &[KlineCandlestickDataItemInner]) -> Option<Bar> {
    Some(Bar {
        t: int(k.first())?,
        o: num(k.get(1))?,
        h: num(k.get(2))?,
        l: num(k.get(3))?,
        c: num(k.get(4))?,
        vol: num(k.get(5))?,
        trades: int(k.get(8)).unwrap_or(0) as u32,
        buy_vol: num(k.get(9)).unwrap_or(0.0),
        ..Default::default()
    })
}

impl Rest {
    pub fn new() -> Result<Self, RestError> {
        let cfg = ConfigurationRestApi::builder()
            .timeout(10_000)
            .retries(2)
            .build()
            .map_err(req)?;
        Ok(Self {
            api: DerivativesTradingUsdsFuturesRestApi::production(cfg),
        })
    }

    pub async fn usdt_perps(&self) -> Result<HashSet<String>, RestError> {
        let info = self
            .api
            .exchange_information()
            .await
            .map_err(req)?
            .data()
            .await
            .map_err(req)?;
        let mut kept = HashSet::new();
        let mut dropped: BTreeMap<String, usize> = BTreeMap::new();
        for s in info.symbols.unwrap_or_default() {
            let kind = s.contract_type.as_deref();
            if is_perpetual(kind, s.quote_asset.as_deref(), s.status.as_deref()) {
                kept.extend(s.symbol);
            } else {
                let reason = match kind {
                    Some(k) if k.ends_with("PERPETUAL") => "perpetual not USDT or not trading",
                    Some(k) => k,
                    None => "no contract type",
                };
                *dropped.entry(reason.to_string()).or_default() += 1;
            }
        }
        tracing::info!(kept = kept.len(), ?dropped, "exchange symbols filtered");
        Ok(kept)
    }

    pub async fn klines_1m(
        &self,
        symbol: &str,
        mut start: u64,
        end: u64,
    ) -> Result<Vec<Bar>, RestError> {
        let mut out: Vec<Bar> = Vec::new();
        while start < end {
            let p = KlineCandlestickDataParams::builder(
                symbol.to_string(),
                KlineCandlestickDataIntervalEnum::Interval1m,
            )
            .start_time(start as i64)
            .end_time(end as i64)
            .limit(KLINE_PAGE)
            .build()
            .map_err(req)?;
            let rows = self
                .api
                .kline_candlestick_data(p)
                .await
                .map_err(req)?
                .data()
                .await
                .map_err(req)?;
            let n = rows.len();
            let bars: Vec<Bar> = rows.iter().filter_map(|k| kline_bar(k)).collect();
            if bars.len() != n {
                return Err(RestError::Shape(format!("{symbol} kline row")));
            }
            let Some(last) = bars.last().map(|b| b.t) else {
                break;
            };
            out.extend(bars);
            if (n as i64) < KLINE_PAGE {
                break;
            }
            start = last + MIN_MS;
        }
        let now = now_ms();
        out.retain(|b| b.t + MIN_MS <= now);
        Ok(out)
    }

    pub async fn open_interest(&self, symbol: &str) -> Result<(u64, f64), RestError> {
        let p = OpenInterestParams::builder(symbol.to_string())
            .build()
            .map_err(req)?;
        let r = self
            .api
            .open_interest(p)
            .await
            .map_err(req)?
            .data()
            .await
            .map_err(req)?;
        let oi = r
            .open_interest
            .and_then(|s| s.parse().ok())
            .ok_or_else(|| RestError::Shape(format!("{symbol} openInterest")))?;
        Ok((
            r.time
                .and_then(|t| u64::try_from(t).ok())
                .unwrap_or_else(now_ms),
            oi,
        ))
    }

    pub async fn oi_hist(&self, symbol: &str) -> Result<Vec<(u64, f64)>, RestError> {
        let mut start = now_ms() - OI_HIST_MS;
        let mut out = Vec::new();
        loop {
            let p = OpenInterestStatisticsParams::builder(
                symbol.to_string(),
                OpenInterestStatisticsPeriodEnum::Period5m,
            )
            .start_time(start as i64)
            .limit(OI_PAGE)
            .build()
            .map_err(req)?;
            let rows = self
                .api
                .open_interest_statistics(p)
                .await
                .map_err(req)?
                .data()
                .await
                .map_err(req)?;
            let n = rows.len();
            let pts: Vec<(u64, f64)> = rows
                .into_iter()
                .filter_map(|r| {
                    Some((
                        u64::try_from(r.timestamp?).ok()?,
                        r.sum_open_interest?.parse().ok()?,
                    ))
                })
                .collect();
            let Some(last) = pts.last().map(|p| p.0) else {
                break;
            };
            out.extend(pts);
            if (n as i64) < OI_PAGE {
                break;
            }
            start = last + OI_STEP_MS;
        }
        Ok(out)
    }
}

async fn backfill(rest: &Rest, router: &Router, s: &str) {
    let now = now_ms();
    match rest.klines_1m(s, now - BACKFILL_1M * MIN_MS, now).await {
        Ok(bars) => {
            tracing::info!(symbol = %s, bars = bars.len(), "kline backfill");
            router.ctrl(s, Ctrl::Bars1m(s.to_string(), bars));
        }
        Err(e) => tracing::warn!(symbol = %s, error = %e, "kline backfill failed"),
    }
    match rest.oi_hist(s).await {
        Ok(pts) => router.ctrl(s, Ctrl::OiHist(s.to_string(), pts)),
        Err(e) => tracing::warn!(symbol = %s, error = %e, "oi history failed"),
    }
}

pub async fn run(
    rest: Rest,
    mut jobs: mpsc::UnboundedReceiver<RestJob>,
    want: watch::Receiver<Wanted>,
    router: Router,
    ctl: mpsc::UnboundedSender<Cmd>,
    ct: CancellationToken,
) {
    let mut oi_tick = tokio::time::interval(OI_POLL);
    let mut sym_tick = tokio::time::interval(SYMBOLS_REFRESH);
    oi_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = ct.cancelled() => break,
            _ = sym_tick.tick() => match rest.usdt_perps().await {
                Ok(set) if !set.is_empty() => { let _ = ctl.send(Cmd::Known(set)); }
                Ok(_) => tracing::warn!("exchange info returned no USDT perpetuals"),
                Err(e) => tracing::warn!(error = %e, "exchange info failed"),
            },
            Some(job) = jobs.recv() => match job {
                RestJob::Backfill(s) => {
                    if want.borrow().contains(&s) {
                        backfill(&rest, &router, &s).await;
                    }
                }
                RestJob::GapFill { since_ms } => {
                    let syms: Vec<String> = want.borrow().iter().cloned().collect();
                    let now = now_ms();
                    for s in syms {
                        match rest.klines_1m(&s, since_ms.saturating_sub(GAP_PAD_MS), now).await {
                            Ok(bars) => router.ctrl(&s, Ctrl::Bars1m(s.clone(), bars)),
                            Err(e) => tracing::warn!(symbol = %s, error = %e, "gap backfill failed"),
                        }
                    }
                }
            },
            _ = oi_tick.tick() => {
                let syms: Vec<String> = want.borrow().iter().cloned().collect();
                for s in syms {
                    match rest.open_interest(&s).await {
                        Ok((ts, oi)) => router.ctrl(&s, Ctrl::Oi(s.clone(), ts, oi)),
                        Err(e) => tracing::debug!(symbol = %s, error = %e, "open interest failed"),
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_usdt_perpetual_kind_counts_and_dated_futures_do_not() {
        let ok = |t: &str| is_perpetual(Some(t), Some("USDT"), Some("TRADING"));
        assert!(ok("PERPETUAL") && ok("TRADIFI_PERPETUAL") && ok("FUTURE_PERPETUAL"));
        assert!(!ok("CURRENT_QUARTER") && !ok("NEXT_QUARTER"));
        assert!(!is_perpetual(
            Some("PERPETUAL"),
            Some("USDC"),
            Some("TRADING")
        ));
        assert!(!is_perpetual(
            Some("PERPETUAL"),
            Some("USDT"),
            Some("SETTLING")
        ));
        assert!(!is_perpetual(None, Some("USDT"), Some("TRADING")));
    }
}
