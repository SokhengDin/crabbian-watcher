use std::sync::Arc;

use crabbian_watcher::detector::consts::*;
use crabbian_watcher::detector::{Bar, Dir, measure};
use crabbian_watcher::events::{Event, EventKind};
use crabbian_watcher::ingest::{Parsed, parse_frame};
use crabbian_watcher::levels::{Level, LevelKind, Side, Touch};
use crabbian_watcher::rules::{Action, Condition, Owner, RuleEntry, RuleSpec, When};
use crabbian_watcher::shard::{Data, SymbolCtx};

const T0: u64 = 1_790_000_000_000 - 1_790_000_000_000 % MIN_MS;
const SIGMA: f64 = 0.001;
const VOL: f64 = 100.0;

struct Sim {
    ctx: SymbolCtx,
    now: u64,
    px: f64,
    agg: u64,
    seed: u64,
    events: Vec<Event>,
}

impl Sim {
    fn with_history(bars: Vec<Bar>) -> Self {
        let mut ctx = SymbolCtx::new("SOLUSDT", T0);
        ctx.st.merge_1m(&bars, T0);
        let px = ctx.st.last_px;
        Sim {
            ctx,
            now: T0,
            px,
            agg: 0,
            seed: 7,
            events: Vec::new(),
        }
    }

    fn new() -> Self {
        let mut s = Sim::with_history(Vec::new());
        let mut c = 150.0;
        let n = BACKFILL_1M;
        let bars = (0..n)
            .map(|i| {
                let o = c;
                c *= (s.normal() * SIGMA).exp();
                Bar {
                    t: T0 - (n - i) * MIN_MS,
                    o,
                    h: o.max(c) * (1.0 + SIGMA / 2.0),
                    l: o.min(c) * (1.0 - SIGMA / 2.0),
                    c,
                    vol: VOL,
                    buy_vol: VOL / 2.0,
                    trades: 120,
                    ..Default::default()
                }
            })
            .collect::<Vec<_>>();
        let mut sim = Sim::with_history(bars);
        sim.seed = s.seed;
        sim
    }

    fn uniform(&mut self) -> f64 {
        self.seed = self
            .seed
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.seed >> 11) as f64 / (1u64 << 53) as f64
    }

    fn normal(&mut self) -> f64 {
        (0..12).map(|_| self.uniform()).sum::<f64>() - 6.0
    }

    fn second(&mut self, trades: &[(f64, f64, bool)]) {
        let start = self.now;
        let n = trades.len().max(1) as u64;
        for (i, &(step, qty, taker_sell)) in trades.iter().enumerate() {
            self.px *= step.exp();
            self.agg += 1;
            let ts = start + i as u64 * SEC_MS / n + 1;
            let ev = self.ctx.trade(self.agg, self.px, qty, taker_sell, ts, ts);
            self.events.extend(ev);
        }
        self.now = start + SEC_MS;
        let ev = self.ctx.tick(self.now);
        self.events.extend(ev);
    }

    fn quiet(&mut self, secs: u64) {
        let s = SIGMA / 120f64.sqrt();
        for _ in 0..secs {
            let (a, b) = (self.normal() * s, self.normal() * s);
            self.second(&[(a, VOL / 120.0, false), (b, VOL / 120.0, true)]);
        }
    }

    fn push(&mut self, secs: u64, total_ln: f64, vol_mult: f64, buy_share: f64) {
        let per = 6usize;
        let buys = (per as f64 * buy_share).round() as usize;
        let step = total_ln / (secs as f64 * per as f64);
        let qty = VOL / 60.0 * vol_mult / per as f64;
        for _ in 0..secs {
            let trades: Vec<(f64, f64, bool)> = (0..per).map(|i| (step, qty, i >= buys)).collect();
            self.second(&trades);
        }
    }

    fn warm(&mut self) {
        self.quiet(WARMUP_MS / SEC_MS + 60);
        assert!(self.ctx.st.warm(self.now));
    }

    fn take(&mut self) -> Vec<Event> {
        std::mem::take(&mut self.events)
    }
}

fn jumps(ev: &[Event], d: Dir) -> usize {
    ev.iter()
        .filter(|e| e.kind == EventKind::Jump && e.direction == d)
        .count()
}

#[test]
fn real_jump_is_caught_fast() {
    let mut sim = Sim::new();
    sim.warm();
    assert!(sim.take().is_empty(), "quiet market raises nothing");
    let start = sim.now;
    let mut first_at = None;
    for _ in 0..30 {
        sim.push(1, 0.02 / 30.0, 10.0, 0.9);
        if first_at.is_none() && !sim.events.is_empty() {
            first_at = Some(sim.now - start);
        }
    }
    let ev = sim.take();
    assert!(
        first_at.is_some_and(|t| t <= 10_000),
        "first event after {first_at:?} ms"
    );
    let e = &ev[0];
    assert_eq!((e.kind, e.direction), (EventKind::Jump, Dir::Up));
    assert!(e.signals.move_pct.is_some_and(|m| m >= 0.3));
    assert!(!e.signals.confirmers.is_empty());
    assert!(
        ev.iter()
            .all(|e| e.kind == EventKind::Jump && e.direction == Dir::Up)
    );
    assert!(
        ev.len() <= 2,
        "a 2% move fires at most once plus one 1.5x extension, got {}",
        ev.len()
    );
    assert!(ev.iter().any(|e| e.tier == 2));
}

#[test]
fn thin_wick_is_rejected() {
    let mut sim = Sim::new();
    sim.warm();
    sim.take();
    sim.second(&[(0.02, 0.05, false), (-0.02, 0.05, true)]);
    sim.quiet(120);
    assert!(sim.take().is_empty());
}

#[test]
fn volatile_stretch_does_not_fire_repeatedly() {
    let mut sim = Sim::new();
    sim.warm();
    sim.take();
    for _ in 0..6 {
        sim.push(20, 0.008, 5.0, 0.85);
        sim.push(20, -0.008, 5.0, 0.15);
    }
    let ev = sim.take();
    assert!(!ev.is_empty(), "the first leg is a real move");
    assert!(jumps(&ev, Dir::Up) <= 1, "{ev:#?}");
    assert!(jumps(&ev, Dir::Down) <= 1, "{ev:#?}");
}

#[test]
fn cooldown_and_hysteresis() {
    let mut sim = Sim::new();
    sim.warm();
    sim.take();
    sim.push(10, 0.006, 10.0, 1.0);
    assert_eq!(jumps(&sim.take(), Dir::Up), 1);
    sim.quiet(60);
    sim.push(10, 0.002, 10.0, 1.0);
    assert_eq!(
        jumps(&sim.take(), Dir::Up),
        0,
        "small extension inside cooldown is suppressed"
    );
    sim.push(10, 0.008, 10.0, 1.0);
    assert_eq!(
        jumps(&sim.take(), Dir::Up),
        1,
        "1.5x extension past the last event re-fires"
    );
    sim.quiet(17 * 60);
    assert!(sim.ctx.gate.up.armed, "re-armed once |z| fell below 1.5");
    sim.take();
    sim.push(10, 0.006, 10.0, 1.0);
    assert_eq!(
        jumps(&sim.take(), Dir::Up),
        1,
        "fires again after cooldown and re-arm"
    );
}

#[test]
fn warm_up_suppresses_market_events() {
    let mut sim = Sim::new();
    sim.quiet(120);
    sim.push(30, 0.02, 10.0, 0.9);
    assert!(!sim.ctx.st.warm(sim.now));
    assert!(sim.take().is_empty());
}

#[test]
fn level_touch_fires_during_warm_up_as_protect() {
    let mut sim = Sim::new();
    let sl = sim.px * 0.995;
    sim.ctx.set_levels(vec![Level {
        symbol: "SOLUSDT".into(),
        price: sl,
        kind: LevelKind::Sl,
        side: Some(Side::Long),
        trade_id: Some("812".into()),
    }]);
    sim.quiet(5);
    sim.push(10, -0.01, 1.0, 0.5);
    let ev = sim.take();
    let cross = ev
        .iter()
        .find(|e| e.levels_hit.iter().any(|h| h.touch == Touch::Cross))
        .expect("sl cross raises an event");
    assert_eq!(
        (cross.kind, cross.tier, cross.direction),
        (EventKind::Level, 2, Dir::Down)
    );
    assert_eq!(cross.levels_hit[0].trade_id.as_deref(), Some("812"));
    assert_eq!(
        ev.iter()
            .filter(|e| e.levels_hit.iter().any(|h| h.touch == Touch::Cross))
            .count(),
        1
    );
}

#[test]
fn sigma_is_read_as_of_window_start() {
    let mut sim = Sim::new();
    sim.warm();
    sim.push(900, 0.03, 1.0, 0.5);
    let m = measure(&sim.ctx.st, sim.now).unwrap();
    let start_sigma = sim.ctx.st.sigma_at(sim.now - 900 * SEC_MS).unwrap();
    assert!(
        start_sigma < sim.ctx.st.base.ewma_var.sqrt(),
        "the move inflated the live sigma"
    );
    assert!((m.w[2].sigma_k - 15f64.sqrt() * start_sigma).abs() < 1e-15);
    assert!((m.w[2].z - m.w[2].ln_move / m.w[2].sigma_k).abs() < 1e-12);
}

#[test]
fn zero_volume_and_zero_sigma_are_guarded() {
    let bars = (0..BACKFILL_1M)
        .map(|i| Bar::flat(T0 - (BACKFILL_1M - i) * MIN_MS, 150.0))
        .collect();
    let mut sim = Sim::with_history(bars);
    assert_eq!(sim.ctx.st.base.ewma_var, 0.0);
    for _ in 0..(WARMUP_MS / SEC_MS + 60) {
        sim.second(&[(0.0, 0.0, false)]);
    }
    for _ in 0..30 {
        sim.second(&[(0.001, 0.0, false)]);
    }
    let m = measure(&sim.ctx.st, sim.now).unwrap();
    for w in &m.w {
        assert!(w.ok && w.z.is_finite() && w.sigma_k >= SIGMA_FLOOR);
        assert_eq!((w.vol_x, w.taker_buy), (0.0, 0.5));
    }
    assert!(m.liq_x.is_finite() && m.liq_x == 0.0);
    assert!(
        sim.take().is_empty(),
        "no volume, no imbalance: nothing fires"
    );
}

#[test]
fn once_rule_fires_once() {
    let mut sim = Sim::new();
    let spec = RuleSpec {
        rule_id: Some("r_1".into()),
        symbol: "SOLUSDT".into(),
        when: When {
            all: vec![Condition::PriceCross {
                level: sim.px * 1.005,
                dir: Dir::Up,
            }],
        },
        action: Action::Wake,
        note: "reclaim on strength".into(),
        plan: None,
        ttl_s: None,
        once: Some(true),
        owner: Owner {
            user_id: "42".into(),
            agent: "supervisor".into(),
        },
        parent_seq: Some(1041),
    };
    sim.ctx.set_rules(vec![Arc::new(RuleEntry {
        id: "r_1".into(),
        spec,
        expires_ms: u64::MAX,
    })]);
    sim.quiet(5);
    sim.push(5, 0.01, 1.0, 0.5);
    sim.push(5, -0.02, 1.0, 0.5);
    sim.push(5, 0.02, 1.0, 0.5);
    let ev = sim.take();
    assert_eq!(ev.len(), 1, "{ev:#?}");
    let r = ev[0].rule.as_ref().unwrap();
    assert_eq!(ev[0].direction, Dir::Up);
    assert_eq!(
        (ev[0].kind, ev[0].tier, r.rule_id.as_str(), r.parent_seq),
        (EventKind::Rule, 2, "r_1", Some(1041))
    );
    assert_eq!(sim.ctx.fired_once, vec!["r_1".to_string()]);
    assert!(sim.ctx.rules.is_empty());
}

#[test]
fn fixture_frames_replay_into_the_shard() {
    let text = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/frames.jsonl"
    ))
    .unwrap();
    let parsed: Vec<Option<Parsed>> = text.lines().map(parse_frame).collect();
    assert_eq!(parsed[0], Some(Parsed::Ack(1)));
    assert!(matches!(parsed[6], Some(Parsed::Ignored)));
    assert!(matches!(parsed[7], Some(Parsed::Error(Some(7), _))));
    assert!(parsed[8].is_none());

    let mut ctx = SymbolCtx::new("SOLUSDT", 1_790_000_000_000);
    for p in parsed.into_iter().flatten() {
        if let Parsed::Data(d) = p {
            match d {
                Data::Trade {
                    sym,
                    agg,
                    px,
                    qty,
                    taker_sell,
                    ts,
                } => {
                    assert_eq!(sym.as_str(), "SOLUSDT");
                    ctx.on_trade(agg, px, qty, taker_sell, ts);
                }
                Data::Mark { mark, funding, .. } => ctx.st.on_mark(mark, funding),
                Data::Liq {
                    usd,
                    long_liquidated,
                    ..
                } => ctx.st.on_liq(usd, long_liquidated),
            }
        }
    }
    let b = ctx.st.cur1s;
    assert_eq!(
        (b.trades, b.vol, b.buy_vol),
        (2, 15.5, 12.5),
        "duplicate aggTrade id is dropped"
    );
    assert_eq!(
        (ctx.st.last_px, ctx.st.mark, ctx.st.funding),
        (150.11, 150.115, 0.000125)
    );
    assert!((b.liq_long - 400.0 * 149.9).abs() < 1e-6 && b.liq_short == 0.0);
}
