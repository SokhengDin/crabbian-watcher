use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use schemars::JsonSchema;
use serde::Serialize;
use tokio::sync::{mpsc, oneshot};
use tokio_util::sync::CancellationToken;

use crate::detector::consts::*;
use crate::detector::{
    Bar, Candidate, Dir, Gate, Kind, Metrics, SymbolState, detect, measure, oi_read,
};
use crate::events::{Event, EventBus, EventKind, RuleRef, Signals, iso_ms};
use crate::formula;
use crate::levels::{self, Level, LevelHit, LevelWatch};
use crate::rules::{Action, RuleEntry, RuleRt};

pub const SHARDS: usize = 4;
pub const DATA_CAP: usize = 8_192;

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct Sym {
    b: [u8; 23],
    n: u8,
}

impl Sym {
    pub fn new(s: &str) -> Option<Self> {
        let bytes = s.as_bytes();
        if bytes.is_empty() || bytes.len() > 23 || !bytes.iter().all(|c| c.is_ascii_alphanumeric())
        {
            return None;
        }
        let mut b = [0u8; 23];
        for (d, c) in b.iter_mut().zip(bytes) {
            *d = c.to_ascii_uppercase();
        }
        Some(Self {
            b,
            n: bytes.len() as u8,
        })
    }

    pub fn as_str(&self) -> &str {
        std::str::from_utf8(&self.b[..self.n as usize]).unwrap_or("")
    }

    fn hash(&self) -> usize {
        self.b[..self.n as usize]
            .iter()
            .fold(0xcbf2_9ce4_8422_2325u64, |h, c| {
                (h ^ *c as u64).wrapping_mul(0x100_0000_01b3)
            }) as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Data {
    Trade {
        sym: Sym,
        agg: u64,
        px: f64,
        qty: f64,
        taker_sell: bool,
        ts: u64,
    },
    Mark {
        sym: Sym,
        mark: f64,
        funding: f64,
    },
    Liq {
        sym: Sym,
        usd: f64,
        long_liquidated: bool,
    },
}

#[derive(Debug, Clone, Default, Serialize, JsonSchema)]
pub struct SymbolSnapshot {
    pub symbol: String,
    pub price: f64,
    pub sigma_1m: f64,
    pub z_1m: Option<f64>,
    pub z_5m: Option<f64>,
    pub z_15m: Option<f64>,
    pub move_pct_5m: Option<f64>,
    pub vol_x_1m: Option<f64>,
    pub vol_x_5m: Option<f64>,
    pub vol_x_15m: Option<f64>,
    #[schemars(description = "Share of taker volume that bought over the last 5 minutes")]
    pub taker_buy_share: Option<f64>,
    pub liq_usd_60s: f64,
    pub funding: f64,
    pub mark: f64,
    pub oi_z: Option<f64>,
    pub atr_1m: f64,
    pub cooldown_until_up: Option<String>,
    pub cooldown_until_down: Option<String>,
    pub warm: bool,
    pub warm_at: String,
    pub bars_1m: usize,
    pub levels: usize,
    pub rules: usize,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ShardStatus {
    pub symbols: usize,
    pub warm: usize,
}

pub enum Ctrl {
    Add(String),
    Remove(String),
    Bars1m(String, Vec<Bar>),
    OiHist(String, Vec<(u64, f64)>),
    Oi(String, u64, f64),
    Levels(String, Vec<Level>),
    Rules(String, Vec<Arc<RuleEntry>>),
    Gap(u64),
    Query(String, oneshot::Sender<Option<SymbolSnapshot>>),
    Status(oneshot::Sender<ShardStatus>),
}

#[derive(Clone)]
pub struct Router {
    data: Vec<mpsc::Sender<Data>>,
    ctrl: Vec<mpsc::UnboundedSender<Ctrl>>,
}

pub struct ShardIo {
    pub data: mpsc::Receiver<Data>,
    pub ctrl: mpsc::UnboundedReceiver<Ctrl>,
}

impl Router {
    pub fn new(n: usize) -> (Self, Vec<ShardIo>) {
        let (mut data, mut ctrl, mut io) = (Vec::new(), Vec::new(), Vec::new());
        for _ in 0..n {
            let (dt, dr) = mpsc::channel(DATA_CAP);
            let (ct, cr) = mpsc::unbounded_channel();
            data.push(dt);
            ctrl.push(ct);
            io.push(ShardIo { data: dr, ctrl: cr });
        }
        (Self { data, ctrl }, io)
    }

    fn idx(&self, sym: &Sym) -> usize {
        sym.hash() % self.data.len()
    }

    pub fn data(&self, sym: &Sym, d: Data) -> bool {
        self.data[self.idx(sym)].try_send(d).is_ok()
    }

    pub fn ctrl(&self, symbol: &str, c: Ctrl) {
        if let Some(s) = Sym::new(symbol) {
            let _ = self.ctrl[self.idx(&s)].send(c);
        }
    }

    pub fn broadcast(&self, f: impl Fn() -> Ctrl) {
        for c in &self.ctrl {
            let _ = c.send(f());
        }
    }

    pub async fn status(&self) -> ShardStatus {
        let mut total = ShardStatus::default();
        for c in &self.ctrl {
            let (tx, rx) = oneshot::channel();
            if c.send(Ctrl::Status(tx)).is_ok()
                && let Ok(s) = rx.await
            {
                total.symbols += s.symbols;
                total.warm += s.warm;
            }
        }
        total
    }

    pub async fn query(&self, symbol: &str) -> Option<SymbolSnapshot> {
        let (tx, rx) = oneshot::channel();
        self.ctrl(symbol, Ctrl::Query(symbol.to_string(), tx));
        rx.await.ok().flatten()
    }
}

pub struct SymbolCtx {
    pub symbol: String,
    pub st: SymbolState,
    pub gate: Gate,
    pub levels: Vec<LevelWatch>,
    pub rules: Vec<RuleRt>,
    pub fired_once: Vec<String>,
    trip_hi: f64,
    trip_lo: f64,
    tripped: bool,
}

fn r4(x: f64) -> f64 {
    (x * 1e4).round() / 1e4
}

impl SymbolCtx {
    pub fn new(symbol: impl Into<String>, now_ms: u64) -> Self {
        Self {
            symbol: symbol.into(),
            st: SymbolState::new(now_ms),
            gate: Gate::default(),
            levels: Vec::new(),
            rules: Vec::new(),
            fired_once: Vec::new(),
            trip_hi: f64::INFINITY,
            trip_lo: f64::NEG_INFINITY,
            tripped: false,
        }
    }

    pub fn set_levels(&mut self, new: Vec<Level>) {
        levels::replace(&mut self.levels, new);
        self.trip_hi = self.st.last_px;
        self.trip_lo = self.st.last_px;
    }

    pub fn set_rules(&mut self, new: Vec<Arc<RuleEntry>>) {
        RuleRt::replace(&mut self.rules, new);
        self.trip_hi = self.st.last_px;
        self.trip_lo = self.st.last_px;
    }

    pub fn on_trade(&mut self, agg: u64, px: f64, qty: f64, taker_sell: bool, ts: u64) -> bool {
        self.st.on_trade(agg, px, qty, taker_sell, ts)
            && !self.tripped
            && (px >= self.trip_hi || px <= self.trip_lo)
    }

    pub fn trade(
        &mut self,
        agg: u64,
        px: f64,
        qty: f64,
        taker_sell: bool,
        ts: u64,
        now_ms: u64,
    ) -> Vec<Event> {
        if self.on_trade(agg, px, qty, taker_sell, ts) {
            self.tripped = true;
            self.evaluate(now_ms)
        } else {
            Vec::new()
        }
    }

    pub fn tick(&mut self, now_ms: u64) -> Vec<Event> {
        self.st.roll(now_ms);
        self.tripped = false;
        self.evaluate(now_ms)
    }

    fn set_trips(&mut self, m: &Metrics) {
        let px = m.price;
        let (mut hi, mut lo) = (f64::INFINITY, f64::NEG_INFINITY);
        let mut consider = |p: f64| {
            if p > px {
                hi = hi.min(p);
            } else if p < px {
                lo = lo.max(p);
            }
        };
        if m.warm {
            for (i, w) in m.w.iter().enumerate().filter(|(_, w)| w.ok) {
                let thr = (JUMP_Z[i] * w.sigma_k).max(JUMP_MIN_MOVE);
                consider(w.start_px * thr.exp());
                consider(w.start_px * (-thr).exp());
            }
        }
        levels::trip_prices(&self.levels, self.st.atr()).for_each(&mut consider);
        self.rules
            .iter()
            .flat_map(|r| r.levels())
            .for_each(&mut consider);
        (self.trip_hi, self.trip_lo) = (hi, lo);
    }

    fn signals(&self, m: &Metrics, c: Option<&Candidate>) -> Signals {
        let win = c.map_or(1, |c| c.win);
        let w = &m.w[win];
        let z = |i: usize| m.w[i].ok.then(|| r4(m.w[i].z));
        let liq = m.liq_usd();
        Signals {
            price: m.price,
            z1: z(0),
            z5: z(1),
            z15: z(2),
            window_s: w.ok.then_some(WINDOWS_S[win]),
            move_pct: w.ok.then(|| r4(w.move_pct())),
            vol_x: w.ok.then(|| r4(w.vol_x)),
            taker_buy: (w.ok && w.vol > 0.0).then(|| r4(w.taker_buy)),
            taker_sell: (w.ok && w.vol > 0.0).then(|| r4(1.0 - w.taker_buy)),
            liq_usd: (liq > 0.0).then(|| liq.round()),
            liq_x: (liq > 0.0).then(|| r4(m.liq_x)),
            oi_z: m.oi_z.map(r4),
            oi_read: oi_read(m, c.map_or_else(|| Dir::of(w.ln_move), |c| c.dir)),
            funding: (m.funding != 0.0).then_some(m.funding),
            confirmers: c.map(|c| c.confirmers.clone()).unwrap_or_default(),
        }
    }

    fn event(&self, kind: EventKind, tier: u8, direction: Dir, signals: Signals) -> Event {
        Event {
            seq: 0,
            epoch: String::new(),
            ts: String::new(),
            symbol: self.symbol.clone(),
            tier,
            kind,
            direction,
            signals,
            levels_hit: Vec::new(),
            rule: None,
        }
    }

    pub fn evaluate(&mut self, now_ms: u64) -> Vec<Event> {
        let Some(m) = measure(&self.st, now_ms) else {
            return Vec::new();
        };
        self.gate.observe(&m);
        let mut hits: Vec<LevelHit> = Vec::new();
        levels::check(&mut self.levels, m.price, self.st.atr(), now_ms, &mut hits);
        let cand = if m.warm {
            detect(&m).filter(|c| self.gate.allow(c.dir, now_ms, m.price))
        } else {
            None
        };
        let mut fired: Vec<(RuleRef, Option<Dir>)> = Vec::new();
        let ctx = self
            .rules
            .iter()
            .any(RuleRt::has_formula)
            .then(|| formula::context(&m));
        for r in self.rules.iter_mut() {
            if r.eval(&m, ctx.as_ref()) {
                let s = &r.entry.spec;
                fired.push((
                    RuleRef {
                        rule_id: r.entry.id.clone(),
                        owner: s.owner.clone(),
                        note: s.note.clone(),
                        action: s.action,
                        plan: s.plan.clone(),
                        parent_seq: s.parent_seq,
                    },
                    s.when.direction(),
                ));
                if r.done {
                    self.fired_once.push(r.entry.id.clone());
                }
            }
        }
        self.rules
            .retain(|r| !r.done && now_ms < r.entry.expires_ms);
        self.set_trips(&m);

        let rule_tier = |r: &RuleRef| if r.action == Action::Wake { 2 } else { 1 };
        let mut fired = fired.into_iter();
        let mut out = Vec::new();
        let level_tier = if hits
            .iter()
            .any(|h| h.trade_id.is_some() || h.kind != levels::LevelKind::Thesis)
        {
            2
        } else {
            1
        };
        if let Some(c) = cand {
            let w = &m.w[c.win];
            self.gate.record(c.dir, now_ms, m.price, w.start_px, w.z);
            let kind = match c.kind {
                Kind::Jump => EventKind::Jump,
                Kind::Liq => EventKind::Liq,
                Kind::Oi => EventKind::Oi,
            };
            let mut e = self.event(kind, c.tier, c.dir, self.signals(&m, Some(&c)));
            if !hits.is_empty() {
                e.tier = e.tier.max(level_tier);
                e.levels_hit = std::mem::take(&mut hits);
            }
            e.rule = fired.next().map(|(r, _)| r);
            if let Some(r) = &e.rule {
                e.tier = e.tier.max(rule_tier(r));
            }
            out.push(e);
        } else if let Some(first) = hits.first() {
            let mut e = self.event(
                EventKind::Level,
                level_tier,
                first.direction,
                self.signals(&m, None),
            );
            e.levels_hit = std::mem::take(&mut hits);
            e.rule = fired.next().map(|(r, _)| r);
            if let Some(r) = &e.rule {
                e.tier = e.tier.max(rule_tier(r));
            }
            out.push(e);
        }
        for (r, d) in fired {
            let mut e = self.event(
                EventKind::Rule,
                rule_tier(&r),
                d.unwrap_or_else(|| Dir::of(m.w[1].ln_move)),
                self.signals(&m, None),
            );
            e.rule = Some(r);
            out.push(e);
        }
        out
    }

    pub fn snapshot(&self, now_ms: u64) -> SymbolSnapshot {
        let mut s = SymbolSnapshot {
            symbol: self.symbol.clone(),
            warm: self.st.warm(now_ms),
            warm_at: iso_ms(self.st.warm_from_ms),
            bars_1m: self.st.bars1m.len(),
            levels: self.levels.len(),
            rules: self.rules.len(),
            atr_1m: r4(self.st.base.atr),
            cooldown_until_up: self.gate.cooldown_until(Dir::Up, now_ms).map(iso_ms),
            cooldown_until_down: self.gate.cooldown_until(Dir::Down, now_ms).map(iso_ms),
            ..Default::default()
        };
        if let Some(m) = measure(&self.st, now_ms) {
            let ok = |i: usize, f: fn(&crate::detector::Window) -> f64| {
                m.w[i].ok.then(|| r4(f(&m.w[i])))
            };
            s.price = m.price;
            s.sigma_1m = m.sigma_1m;
            (s.z_1m, s.z_5m, s.z_15m) = (ok(0, |w| w.z), ok(1, |w| w.z), ok(2, |w| w.z));
            (s.vol_x_1m, s.vol_x_5m, s.vol_x_15m) =
                (ok(0, |w| w.vol_x), ok(1, |w| w.vol_x), ok(2, |w| w.vol_x));
            s.move_pct_5m = ok(1, |w| w.move_pct());
            s.taker_buy_share = ok(1, |w| w.taker_buy);
            s.liq_usd_60s = m.liq_usd().round();
            s.funding = m.funding;
            s.mark = m.mark;
            s.oi_z = m.oi_z.map(r4);
        }
        s
    }
}

pub async fn run(
    mut io: ShardIo,
    bus: Arc<EventBus>,
    fired: mpsc::UnboundedSender<String>,
    ct: CancellationToken,
) {
    let mut syms: HashMap<Sym, SymbolCtx> = HashMap::new();
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let emit = |ctx: &mut SymbolCtx, events: Vec<Event>, now: u64| {
        for e in events {
            if e.weak() {
                tracing::debug!(symbol = %e.symbol, kind = ?e.kind, "weak event skipped");
            } else {
                bus.push(e, now);
            }
        }
        for id in ctx.fired_once.drain(..) {
            let _ = fired.send(id);
        }
    };
    loop {
        tokio::select! {
            biased;
            _ = ct.cancelled() => break,
            Some(c) = io.ctrl.recv() => {
                let now = now_ms();
                match c {
                    Ctrl::Add(s) => {
                        if let Some(k) = Sym::new(&s) {
                            syms.entry(k).or_insert_with(|| SymbolCtx::new(k.as_str(), now));
                        }
                    }
                    Ctrl::Remove(s) => {
                        if let Some(k) = Sym::new(&s) {
                            syms.remove(&k);
                        }
                    }
                    Ctrl::Bars1m(s, bars) => {
                        if let Some(c) = Sym::new(&s).and_then(|k| syms.get_mut(&k)) {
                            c.st.merge_1m(&bars, now);
                        }
                    }
                    Ctrl::OiHist(s, pts) => {
                        if let Some(c) = Sym::new(&s).and_then(|k| syms.get_mut(&k)) {
                            c.st.set_oi_hist(pts);
                        }
                    }
                    Ctrl::Oi(s, ts, oi) => {
                        if let Some(c) = Sym::new(&s).and_then(|k| syms.get_mut(&k)) {
                            c.st.on_oi(ts, oi);
                        }
                    }
                    Ctrl::Levels(s, lv) => {
                        if let Some(c) = Sym::new(&s).and_then(|k| syms.get_mut(&k)) {
                            c.set_levels(lv);
                        }
                    }
                    Ctrl::Rules(s, rules) => {
                        if let Some(c) = Sym::new(&s).and_then(|k| syms.get_mut(&k)) {
                            c.set_rules(rules);
                        }
                    }
                    Ctrl::Gap(gap_ms) => {
                        if gap_ms > GAP_REWARM_MS {
                            syms.values_mut().for_each(|c| c.st.rewarm(now));
                        }
                    }
                    Ctrl::Query(s, tx) => {
                        let _ = tx.send(Sym::new(&s).and_then(|k| syms.get(&k)).map(|c| c.snapshot(now)));
                    }
                    Ctrl::Status(tx) => {
                        let _ = tx.send(ShardStatus { symbols: syms.len(), warm: syms.values().filter(|c| c.st.warm(now)).count() });
                    }
                }
            }
            Some(d) = io.data.recv() => match d {
                Data::Trade { sym, agg, px, qty, taker_sell, ts } => {
                    if let Some(c) = syms.get_mut(&sym) {
                        let now = now_ms();
                        let ev = c.trade(agg, px, qty, taker_sell, ts, now);
                        emit(c, ev, now);
                    }
                }
                Data::Mark { sym, mark, funding } => {
                    if let Some(c) = syms.get_mut(&sym) {
                        c.st.on_mark(mark, funding);
                    }
                }
                Data::Liq { sym, usd, long_liquidated } => {
                    if let Some(c) = syms.get_mut(&sym) {
                        c.st.on_liq(usd, long_liquidated);
                    }
                }
            },
            _ = tick.tick() => {
                let now = now_ms();
                for c in syms.values_mut() {
                    let ev = c.tick(now);
                    emit(c, ev, now);
                }
            }
        }
    }
}
