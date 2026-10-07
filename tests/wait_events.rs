use std::sync::Arc;
use std::time::{Duration, Instant};

use crabbian_watcher::detector::Dir;
use crabbian_watcher::events::{Event, EventBus, EventKind, RING_CAP, Signals};
use crabbian_watcher::levels::{LevelHit, LevelKind, Touch};

fn ev() -> Event {
    Event {
        seq: 0,
        epoch: String::new(),
        ts: String::new(),
        symbol: "SOLUSDT".into(),
        tier: 1,
        kind: EventKind::Jump,
        direction: Dir::Up,
        signals: Signals::default(),
        levels_hit: Vec::new(),
        rule: None,
    }
}

const NOW: u64 = 1_790_000_000_000;

#[tokio::test]
async fn returns_at_once_when_newer_events_exist() {
    let bus = EventBus::new("e1");
    for _ in 0..3 {
        bus.push(ev(), NOW);
    }
    let r = bus.wait(1, Some("e1"), Duration::from_secs(5)).await;
    assert_eq!((r.next, r.dropped, r.events.len()), (3, 0, 2));
    assert_eq!(
        r.events.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(
        (r.events[0].epoch.as_str(), r.events[0].ts.as_str()),
        ("e1", "2026-09-21T14:13:20.000Z")
    );
}

#[tokio::test]
async fn times_out_with_the_same_cursor() {
    let bus = EventBus::new("e1");
    bus.push(ev(), NOW);
    let t = Instant::now();
    let r = bus.wait(1, Some("e1"), Duration::from_millis(150)).await;
    assert!(t.elapsed() >= Duration::from_millis(140));
    assert_eq!(
        (r.epoch.as_str(), r.next, r.dropped, r.events.len()),
        ("e1", 1, 0, 0)
    );
}

#[tokio::test]
async fn wakes_on_push() {
    let bus = Arc::new(EventBus::new("e1"));
    let b = bus.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        b.push(ev(), NOW);
    });
    let t = Instant::now();
    let r = bus.wait(0, Some("e1"), Duration::from_secs(10)).await;
    assert!(t.elapsed() < Duration::from_secs(2));
    assert_eq!((r.next, r.events.len()), (1, 1));
}

#[tokio::test]
async fn epoch_change_returns_the_whole_ring() {
    let bus = EventBus::new("new");
    let r = bus.wait(77, None, Duration::from_secs(5)).await;
    assert_eq!(
        (r.epoch.as_str(), r.next, r.dropped, r.events.len()),
        ("new", 0, 0, 0),
        "empty ring still returns at once"
    );
    bus.push(ev(), NOW);
    bus.push(ev(), NOW);
    let r = bus.wait(500, Some("old"), Duration::from_secs(5)).await;
    assert_eq!(
        (r.epoch.as_str(), r.next, r.dropped, r.events.len()),
        ("new", 2, 0, 2)
    );
}

#[tokio::test]
async fn reports_dropped_when_cursor_fell_out_of_the_ring() {
    let bus = EventBus::new("e1");
    for _ in 0..RING_CAP + 5 {
        bus.push(ev(), NOW);
    }
    let r = bus.wait(0, Some("e1"), Duration::from_secs(5)).await;
    assert_eq!(
        (r.dropped, r.events.len(), r.events[0].seq),
        (5, RING_CAP, 6)
    );
    assert_eq!(r.next, (RING_CAP + 5) as u64);
    let r = bus
        .wait(r.next - 1, Some("e1"), Duration::from_secs(5))
        .await;
    assert_eq!((r.dropped, r.events.len()), (0, 1));
    assert_eq!(bus.stats().total, (RING_CAP + 5) as u64);
}

#[test]
fn a_serialized_event_carries_every_property_its_schema_requires() {
    let schema = serde_json::to_value(schemars::schema_for!(Event)).unwrap();
    let json = serde_json::to_value(ev()).unwrap();
    let missing = |required: &serde_json::Value, obj: &serde_json::Value| -> Vec<String> {
        required
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|k| k.as_str())
            .filter(|k| obj.get(*k).is_none())
            .map(String::from)
            .collect()
    };
    assert_eq!(missing(&schema["required"], &json), Vec::<String>::new());
    assert_eq!(
        missing(&schema["$defs"]["Signals"]["required"], &json["signals"]),
        Vec::<String>::new()
    );
}

fn hit(kind: LevelKind) -> LevelHit {
    LevelHit {
        kind,
        trade_id: None,
        side: None,
        price: 150.0,
        touch: Touch::Near,
        direction: Dir::Up,
    }
}

#[test]
fn weak_heads_ups_on_thesis_levels_and_small_liquidations_are_skipped() {
    let liq = |z1: f64| Event {
        kind: EventKind::Liq,
        signals: Signals {
            window_s: Some(60),
            z1: Some(z1),
            ..Default::default()
        },
        ..ev()
    };
    assert!(liq(-2.4).weak());
    assert!(!liq(-3.2).weak());
    assert!(
        !Event {
            tier: 2,
            ..liq(-2.4)
        }
        .weak()
    );

    let level = |vol_x: Option<f64>, kind: LevelKind| Event {
        kind: EventKind::Level,
        signals: Signals {
            vol_x,
            ..Default::default()
        },
        levels_hit: vec![hit(kind)],
        ..ev()
    };
    assert!(level(Some(0.6), LevelKind::Thesis).weak());
    assert!(!level(Some(1.4), LevelKind::Thesis).weak());
    assert!(
        !level(None, LevelKind::Thesis).weak(),
        "missing volume is never dropped"
    );
    assert!(
        !level(Some(0.6), LevelKind::Sl).weak(),
        "trade levels always go out"
    );
    assert!(!ev().weak());
}
