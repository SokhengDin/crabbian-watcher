use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::value::RawValue;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, sleep, sleep_until};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream, connect_async};
use tokio_util::sync::CancellationToken;

use crate::control::Wanted;
use crate::rest::RestJob;
use crate::shard::{Ctrl, Data, Router, Sym, now_ms};

const GLOBAL_STREAM: &str = "!forceOrder@arr";
const STALE: Duration = Duration::from_secs(5);
const ROTATE_AFTER: Duration = Duration::from_secs(23 * 3_600);
const ROTATE_RETRY: Duration = Duration::from_secs(60);
const BACKOFF_MIN: Duration = Duration::from_secs(1);
const BACKOFF_MAX: Duration = Duration::from_secs(30);
const SUB_BATCH: usize = 100;
const SUB_PACE: Duration = Duration::from_millis(250);

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("websocket: {0}")]
    Ws(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("encode: {0}")]
    Json(#[from] serde_json::Error),
}

#[derive(Default)]
pub struct FeedStats {
    pub connected: AtomicBool,
    pub last_msg_ms: AtomicU64,
    pub msgs: AtomicU64,
    pub msgs_per_sec: AtomicU64,
    pub dropped: AtomicU64,
    pub parse_errors: AtomicU64,
    pub reconnects: AtomicU64,
    pub down_since_ms: AtomicU64,
}

#[derive(Deserialize)]
struct Frame<'a> {
    #[serde(borrow)]
    stream: Option<&'a str>,
    #[serde(borrow)]
    data: Option<&'a RawValue>,
    id: Option<u64>,
    error: Option<WsErr>,
}

#[derive(Deserialize, Debug)]
struct WsErr {
    code: Option<i64>,
    msg: Option<String>,
}

#[derive(Deserialize)]
struct AggTrade<'a> {
    s: &'a str,
    a: u64,
    p: &'a str,
    q: &'a str,
    #[serde(rename = "T")]
    t: u64,
    m: bool,
}

#[derive(Deserialize)]
struct MarkPrice<'a> {
    s: &'a str,
    p: &'a str,
    #[serde(borrow)]
    r: Option<&'a str>,
}

#[derive(Deserialize)]
struct ForceOrder<'a> {
    #[serde(borrow)]
    o: ForceInner<'a>,
}

#[derive(Deserialize)]
struct ForceInner<'a> {
    s: &'a str,
    #[serde(rename = "S")]
    side: &'a str,
    #[serde(borrow)]
    ap: Option<&'a str>,
    #[serde(borrow)]
    z: Option<&'a str>,
    #[serde(borrow)]
    p: Option<&'a str>,
    #[serde(borrow)]
    q: Option<&'a str>,
}

#[derive(Debug, PartialEq)]
pub enum Parsed {
    Data(Data),
    Ack(u64),
    Error(Option<u64>, String),
    Ignored,
}

fn f(s: Option<&str>) -> f64 {
    s.and_then(|s| s.parse().ok()).unwrap_or(0.0)
}

pub fn parse_frame(text: &str) -> Option<Parsed> {
    let fr: Frame = serde_json::from_str(text).ok()?;
    if let Some(e) = fr.error {
        return Some(Parsed::Error(
            fr.id,
            format!("{:?} {}", e.code, e.msg.unwrap_or_default()),
        ));
    }
    let (Some(stream), Some(data)) = (fr.stream, fr.data) else {
        return Some(fr.id.map_or(Parsed::Ignored, Parsed::Ack));
    };
    let d = if stream.ends_with("@aggTrade") {
        let t: AggTrade = serde_json::from_str(data.get()).ok()?;
        Data::Trade {
            sym: Sym::new(t.s)?,
            agg: t.a,
            px: t.p.parse().ok()?,
            qty: t.q.parse().ok()?,
            taker_sell: t.m,
            ts: t.t,
        }
    } else if stream.contains("@markPrice") {
        let m: MarkPrice = serde_json::from_str(data.get()).ok()?;
        Data::Mark {
            sym: Sym::new(m.s)?,
            mark: m.p.parse().ok()?,
            funding: f(m.r),
        }
    } else if stream.starts_with("!forceOrder") {
        let o = serde_json::from_str::<ForceOrder>(data.get()).ok()?.o;
        let usd = match f(o.ap) * f(o.z) {
            v if v > 0.0 => v,
            _ => f(o.p) * f(o.q),
        };
        Data::Liq {
            sym: Sym::new(o.s)?,
            usd,
            long_liquidated: o.side == "SELL",
        }
    } else {
        return Some(Parsed::Ignored);
    };
    Some(Parsed::Data(d))
}

pub fn streams(want: &BTreeSet<String>) -> BTreeSet<String> {
    want.iter()
        .flat_map(|s| {
            let l = s.to_ascii_lowercase();
            [format!("{l}@aggTrade"), format!("{l}@markPrice@1s")]
        })
        .collect()
}

struct Conn {
    ws: Ws,
    subs: BTreeSet<String>,
    next_id: u64,
}

impl Conn {
    async fn open(url: &str, want: Wanted) -> Result<Conn, IngestError> {
        let sep = if url.contains('?') { '&' } else { '?' };
        let (ws, _) = connect_async(format!("{url}{sep}streams={GLOBAL_STREAM}")).await?;
        let mut c = Conn {
            ws,
            subs: BTreeSet::new(),
            next_id: 0,
        };
        c.sync(&want).await?;
        tracing::info!(streams = c.subs.len(), "binance connected");
        Ok(c)
    }

    async fn send(&mut self, method: &str, params: &[&String]) -> Result<(), IngestError> {
        self.next_id += 1;
        let body = serde_json::json!({ "method": method, "params": params, "id": self.next_id });
        self.ws.send(Message::Text(body.to_string().into())).await?;
        Ok(())
    }

    async fn sync(&mut self, want: &BTreeSet<String>) -> Result<(), IngestError> {
        let target = streams(want);
        let drop: Vec<String> = self.subs.difference(&target).cloned().collect();
        let add: Vec<String> = target.difference(&self.subs).cloned().collect();
        let batches: Vec<(&str, Vec<&String>)> = drop
            .chunks(SUB_BATCH)
            .map(|c| ("UNSUBSCRIBE", c.iter().collect()))
            .chain(
                add.chunks(SUB_BATCH)
                    .map(|c| ("SUBSCRIBE", c.iter().collect())),
            )
            .collect();
        for (i, (method, params)) in batches.iter().enumerate() {
            if i > 0 {
                sleep(SUB_PACE).await;
            }
            self.send(method, params).await?;
        }
        if !batches.is_empty() {
            tracing::info!(added = add.len(), removed = drop.len(), "streams updated");
        }
        self.subs = target;
        Ok(())
    }
}

enum Exit {
    Cancel,
    Dead(String),
    Rotated(Box<Conn>),
}

fn handle(text: &str, router: &Router, stats: &FeedStats) {
    match parse_frame(text) {
        Some(Parsed::Data(d)) => {
            let sym = match &d {
                Data::Trade { sym, .. } | Data::Mark { sym, .. } | Data::Liq { sym, .. } => *sym,
            };
            if !router.data(&sym, d) {
                stats.dropped.fetch_add(1, Relaxed);
            }
        }
        Some(Parsed::Error(id, msg)) => tracing::warn!(id, msg, "binance request error"),
        Some(Parsed::Ack(_) | Parsed::Ignored) => {}
        None => {
            stats.parse_errors.fetch_add(1, Relaxed);
        }
    }
}

async fn pump(
    url: &str,
    c: &mut Conn,
    want: &mut watch::Receiver<Wanted>,
    router: &Router,
    stats: &FeedStats,
    ct: &CancellationToken,
) -> Exit {
    let mut rotate_at = Instant::now() + ROTATE_AFTER;
    let mut pending: Option<JoinHandle<Result<Conn, IngestError>>> = None;
    let mut last_rx = Instant::now();
    let mut watchdog = tokio::time::interval(Duration::from_secs(1));
    let mut counted = stats.msgs.load(Relaxed);
    loop {
        tokio::select! {
            _ = ct.cancelled() => return Exit::Cancel,
            msg = c.ws.next() => match msg {
                Some(Ok(Message::Text(t))) => {
                    last_rx = Instant::now();
                    stats.msgs.fetch_add(1, Relaxed);
                    stats.last_msg_ms.store(now_ms(), Relaxed);
                    handle(t.as_str(), router, stats);
                }
                Some(Ok(Message::Close(f))) => return Exit::Dead(format!("closed by server: {f:?}")),
                Some(Ok(_)) => last_rx = Instant::now(),
                Some(Err(e)) => return Exit::Dead(e.to_string()),
                None => return Exit::Dead("stream ended".into()),
            },
            r = want.changed() => {
                if r.is_err() {
                    return Exit::Cancel;
                }
                let w = want.borrow_and_update().clone();
                if let Err(e) = c.sync(&w).await {
                    return Exit::Dead(e.to_string());
                }
            }
            _ = watchdog.tick() => {
                let total = stats.msgs.load(Relaxed);
                stats.msgs_per_sec.store(total - counted, Relaxed);
                counted = total;
                if !c.subs.is_empty() && last_rx.elapsed() > STALE {
                    return Exit::Dead("no message for 5s".into());
                }
            }
            _ = sleep_until(rotate_at), if pending.is_none() => {
                let (url, w) = (url.to_string(), want.borrow().clone());
                pending = Some(tokio::spawn(async move { Conn::open(&url, w).await }));
            }
            r = async {
                match pending.as_mut() {
                    Some(h) => h.await,
                    None => std::future::pending().await,
                }
            }, if pending.is_some() => {
                pending = None;
                match r {
                    Ok(Ok(next)) => return Exit::Rotated(Box::new(next)),
                    Ok(Err(e)) => tracing::warn!(error = %e, "rotation connect failed"),
                    Err(e) => tracing::warn!(error = %e, "rotation task failed"),
                }
                rotate_at = Instant::now() + ROTATE_RETRY;
            }
        }
    }
}

pub async fn run(
    url: String,
    mut want: watch::Receiver<Wanted>,
    router: Router,
    stats: Arc<FeedStats>,
    rest: mpsc::UnboundedSender<RestJob>,
    ct: CancellationToken,
) {
    let mut backoff = BACKOFF_MIN;
    let mut next: Option<Conn> = None;
    let mut ever = false;
    loop {
        let mut c = match next.take() {
            Some(c) => c,
            None => {
                let w = want.borrow_and_update().clone();
                let opened = tokio::select! {
                    _ = ct.cancelled() => return,
                    r = Conn::open(&url, w) => r,
                };
                match opened {
                    Ok(c) => {
                        backoff = BACKOFF_MIN;
                        if ever {
                            let last = stats.last_msg_ms.load(Relaxed);
                            let gap = now_ms().saturating_sub(last);
                            stats.reconnects.fetch_add(1, Relaxed);
                            tracing::info!(gap_ms = gap, "binance reconnected");
                            router.broadcast(|| Ctrl::Gap(gap));
                            let _ = rest.send(RestJob::GapFill { since_ms: last });
                        }
                        ever = true;
                        c
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, backoff_s = backoff.as_secs(), "binance connect failed");
                        tokio::select! {
                            _ = ct.cancelled() => return,
                            _ = sleep(backoff) => {}
                        }
                        backoff = (backoff * 2).min(BACKOFF_MAX);
                        continue;
                    }
                }
            }
        };
        stats.connected.store(true, Relaxed);
        stats.down_since_ms.store(0, Relaxed);
        match pump(&url, &mut c, &mut want, &router, &stats, &ct).await {
            Exit::Cancel => {
                let _ = c.ws.close(None).await;
                return;
            }
            Exit::Rotated(n) => {
                tracing::info!("binance connection rotated");
                let _ = c.ws.close(None).await;
                next = Some(*n);
            }
            Exit::Dead(why) => {
                tracing::warn!(reason = %why, "binance connection lost");
                stats.connected.store(false, Relaxed);
                stats.down_since_ms.store(now_ms(), Relaxed);
            }
        }
    }
}
