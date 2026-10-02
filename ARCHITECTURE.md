# crabbian-watcher architecture

This document describes the system as built: what it does, how data moves through it, what it exposes, and the design
decisions behind each part. `CLAUDE.md` holds the rules for working on the code and the original plan; where the
implementation settled something the plan left open, this file records the result (see
[Decisions made during the build](#decisions-made-during-the-build)).

## 1. Purpose and scope

crabbian-watcher is a sensor for the paper-trading desk in `../bg-agent-bot`. It holds one WebSocket connection to
Binance USD-M futures, analyzes every trade in memory, and raises a numbered event the moment a symbol jumps, touches a
desk level, or matches an agent rule. The bot reads events by long-polling an MCP tool and decides what to do.

The desk's watcher agent only looks on `:00` and `:30`. This service closes that gap: a surge at 9:10 becomes an event
at 9:10, within milliseconds of the trade that confirmed it.

| It does | It never does |
|---|---|
| Stream public market data (trades, mark price, funding, liquidations) and poll open interest | Use API keys, call order endpoints, or trade |
| Measure each move against the symbol's own volatility | Run an LLM or decide what to trade |
| Assign a tier (heads-up / please check, protect implied) | Persist anything: no database, files, Redis or broker |
| Keep the last 1,000 events for replay | Throttle or budget first events |

All state lives in memory and is lost on restart by design. The bot is the source of truth and re-sends interest,
levels and rules within 60 seconds of seeing a new epoch.

## 2. System context

```
                       bg-agent-bot (Python)                              crabbian-watcher (Rust)
 ┌──────────────────────────────────────────────┐        ┌──────────────────────────────────────────────┐
 │ crabbian_sync (beat, 60s)                    │  MCP   │ control task: interest, levels, rules        │
 │   set_interest / set_levels / create_rule ───┼───────►│   drives SUBSCRIBE/UNSUBSCRIBE + shard state │
 │ agent @tools                                 │        │                                              │
 │   create/list/cancel_rule, symbol_state  ────┼───────►│ shards: per-symbol bars, detector, gate      │
 │ crabbian_pump (own process)                  │        │                                              │
 │   wait_events long-poll  ◄───────────────────┼────────┤ event ring (seq, epoch)                      │
 │   → Celery: protect / flash alert / check    │        └───────────▲──────────────────▲───────────────┘
 └──────────────────────────────────────────────┘                    │ WebSocket        │ REST
                                                         wss://fstream.binance.com   https://fapi.binance.com
                                                         /market/stream              klines, OI, exchangeInfo
```

There are two planes over one protocol. On the control plane the bot calls tools on this service; on the event plane
the bot pulls events out with `wait_events`. They meet at the rule: every rule carries its owner, note and optional plan,
so an event says whom to wake and why.

## 3. Runtime: tasks, channels, ownership

```
                         watch<Arc<BTreeSet<symbol>>>  (desired subscription set)
          ┌──────────────────────────────┬───────────────────────────────────────┐
          │                              ▼                                       ▼
 MCP ──Cmd──► control task ──Ctrl──► shard 0..3 ◄──Data (bounded, try_send)── ingest task ◄── Binance WS
 tools ◄─oneshot─┘   ▲  │               │  ▲                                      │
                     │  └──RestJob──► REST task ──Ctrl (bars, OI)──┘  └─Ctrl::Gap────┘
                     │                    │
                     └──Cmd::Known────────┘ (exchange symbols, hourly)
          shards ──rule id (once-rule fired)──► control task
          shards ──push──► EventBus (Mutex<VecDeque> + Notify) ◄──wait── MCP wait_events
```

| Task | Count | Owns | Talks to |
|---|---|---|---|
| ingest | 1 (2 briefly during rotation) | the socket, the set of subscribed streams | shards (data), REST (gap fill), stats |
| shard | 4 (`SHARDS`) | every `SymbolCtx` whose symbol hashes to it (FNV-1a % 4) | event bus, control (fired once-rules) |
| control | 1 | interest set, levels, rules, tombstones, known symbols | shards, ingest (watch), REST |
| REST | 1 | the Binance REST client | shards, control |
| MCP / axum | per request | nothing | control (oneshot), shards (query), event bus |

There are no locks on the trade path. Each shard exclusively owns its symbols' state, and every change from outside
arrives as a message. The shared structures are:

- `EventBus`: a `Mutex<VecDeque<Event>>` (last 1,000) plus `tokio::sync::Notify`. Pushes are rare, so the lock is not
  on a hot path.
- `FeedStats`: atomics written by ingest and read by `watcher_status`.
- The `watch` channel carrying the desired symbol set from control to ingest and REST.

Each shard has two channels:

- **Data** (`mpsc`, capacity 8,192): ingest uses `try_send`, so a full shard drops the tick and counts it
  (`dropped_ticks`) rather than blocking the socket reader.
- **Control** (unbounded): read first by a `biased` select, so level and rule changes are never starved by data.

## 4. The data path

```
frame ─► parse_frame (borrowed &str, no allocation beyond serde) ─► Data { sym: Sym, .. } ─► shard
      ─► SymbolCtx::trade ─► SymbolState::on_trade (dedupe by aggTrade id, roll bins, add to 1s and 1m bar)
      ─► price beyond a trip price? ─► evaluate now          (fast path, at most once per second)
1s tick ─► SymbolState::roll(now) ─► evaluate(now)           (every second, every symbol)

evaluate = measure ─► gate.observe ─► levels::check ─► detect (if warm) ─► gate.allow ─► rules.eval ─► compose events
```

### Fast path: trip prices

Heavy math runs once per second per symbol. To still react within milliseconds, each evaluation precomputes the nearest
price above and below the current price at which something could fire:

- each window's jump threshold: `start_px · e^(±max(z_k·σ_k, 0.3%))`
- each level and level ± 0.25 ATR
- each rule's `price_cross` level

A trade beyond either trip price runs a full evaluation immediately; otherwise the per-trade cost is two float
comparisons. A `tripped` flag allows one trip-evaluation per second, and the next tick clears it.

### Symbols and parsing

`Sym` is a 23-byte inline uppercase symbol, `Copy` and hashable, so routing and lookup never allocate. `parse_frame`
deserializes the combined-stream envelope with a borrowed `RawValue`, dispatches on the stream suffix, and parses each
price string to `f64` once.

## 5. Per-symbol state

`SymbolState` (in `src/detector/state.rs`) is pure: no I/O and no clock reads. Time is passed in as `now_ms`.

| Field | Contents | Size |
|---|---|---|
| `bars1s` + `cur1s` | OHLC, volume, taker-buy volume, trade count, long/short liquidation USD | 1,800 bars (30 min) |
| `bars1m` + `cur1m` | same | 10,080 bars (7 days) |
| `sigma1m` | EWMA σ of 1m log returns **after** each 1m bar, parallel to `bars1m` | 10,080 |
| `oi_hist` | 5-minute open interest points (7 days from REST, then live) | 2,100 |
| `oi_live` | 30s OI polls | 32 |
| `base` | EWMA variance, median window volume (1/5/15m), liquidation p95, ATR(14), OI change mean/σ | scalars |
| control | last price, last aggTrade id, mark, funding, `warm_from_ms` | scalars |

`Bar` is 80 bytes, so a symbol costs about 1.07 MB, or roughly 32 MB at 30 symbols. That is above the 0.7 MB estimate
in CLAUDE.md, but within the 50 MiB RSS target.

### Bins

Bins are keyed by trade time and roll forward only:

- A trade in a new second pushes the current bar.
- Missing seconds or minutes are filled with flat bars at the last close, so the rings stay contiguous and an index
  maps directly to a time.
- The 1s tick also rolls by wall clock, so a silent symbol still advances. A late trade lands in the current bin.

### Baselines

Baselines are recomputed when a 1m bar closes:

- EWMA variance, span 240 minutes.
- Median window volume, from non-overlapping 1/5/15-minute blocks over the last 24h.
- p95 of per-minute liquidation notional over 24h.
- ATR(14) on the last 120 1m bars, via `kand` (the only use of `kand`).

The OI mean and σ come from consecutive 5-minute log changes, and need at least 288 samples (one day).

### Backfill and merge

`merge_1m` merges REST klines into the 1m ring by open time:

- REST wins for duplicate minutes, but live liquidation fields are kept, because klines don't carry them.
- Unclosed and in-progress minutes are excluded.
- σ is then rebuilt from scratch.

The same path handles the initial backfill (1,500 bars) and gap fills after a reconnect.

## 6. Detector

Code: `src/detector/signals.rs`. `measure(state, now)` produces `Metrics`; `detect(&Metrics)` returns at most one
`Candidate`.

### Windows and z-scores

For each window k ∈ {60, 300, 900} seconds:

- `start_px` = close of the newest 1s bar that ended at or before `now − k`.
- `ln_move = ln(price / start_px)`.
- `σ_k = sqrt(k/60) · max(σ_at(now − k), 1e-5)`. Here `σ_at(t)` is the σ stored with the newest 1m bar that closed
  at or before `t`, so the move being measured never inflates its own σ.
- `z = ln_move / σ_k`.
- `vol_x` = window volume (1s bars + current bin) / median of same-length windows. It is 0 when the median is below
  the floor.
- `taker_buy` = taker-buy volume / volume, or 0.5 when there is no volume.

A window is `ok` only when both a start price and a σ exist.

### Signals

| Signal | Role | Fires when |
|---|---|---|
| Price jump | primary | \|z\| ≥ 4.0 / 3.5 / 3.0 (1m/5m/15m) and \|ln move\| ≥ 0.3%. Best window = largest \|z\|/threshold |
| Liquidation burst | primary and confirmer | 60s liquidation USD / max(24h p95 per minute, $50k) ≥ 2 |
| OI shock | primary and confirmer | (5-minute OI log change − mean) / max(σ, 1e-4) has \|z\| ≥ 3, once 1 day of OI history exists |
| Volume surge | confirmer | `vol_x` ≥ 3 in the candidate's window |
| Taker imbalance | confirmer | taker share on the move's side ≥ 70% in the candidate's window |

### Trigger rule

Primaries are checked in order: jump, then liquidation, then OI. The first one with enough confirmation becomes the
candidate.

- **Jump** needs volume or taker imbalance. A jump backed only by liquidations or OI is a thin-liquidity wick and is
  dropped.
- **Liquidation** (1m window, direction from the dominant side: long liquidations mean down) needs any other confirmer.
- **OI** (5m window, direction from the 5m move) needs any other confirmer.

A signal never confirms itself.

**Market tier:** 2 if any window has \|z\| ≥ 6 or there are at least 2 confirmers, otherwise 1.

**OI reading:** attached to the event when \|oi_z\| ≥ 1, and never used to gate. The four readings are:

| Price | OI | Reading |
|---|---|---|
| up | up | `new_longs` |
| down | up | `shorts_building` |
| up | down | `short_squeeze` |
| down | down | `longs_closing` |

## 7. Gate: cooldown, hysteresis, warm-up

Code: `src/detector/gate.rs`. There is one `DirGate` per direction (up, down) per symbol, shared by the jump,
liquidation and OI kinds.

- **First event:** always passes. Nothing ever delays or budgets it.
- **Cooldown:** 15 minutes per direction after each event.
- **Hysteresis:** a direction re-arms on the first evaluation where every window's z in that direction is below 1.5.
  After the cooldown, the direction fires again only once it has re-armed.
- **Extension:** inside the cooldown, a re-fire needs price to travel a further 1.5× the last event's move past the
  last event price. The move is measured from a reference that stays fixed for the whole cooldown episode. Example:
  the first event at +0.4% (from reference R) allows the next at +1.0%, then +2.5%. A steady 2% run therefore produces
  two events, not five.
- **Warm-up:** for 30 minutes after a symbol is added, and again after a feed gap longer than 60s:
  - No jump, liquidation or OI events.
  - Rule conditions that need baselines (`move_z`, `volume_x`, `taker_imbalance`, `liq_burst`, `oi_change_z`)
    evaluate false.
  - **Level touches and `price_cross` / `funding_above` conditions still fire.** Stop-loss protection must not go
    blind for 30 minutes after a restart.

## 8. Levels

Code: `src/levels.rs`. The bot sends levels with `set_levels`, which replaces them all. A level has a kind (`thesis`,
`sl`, `tp`, `liquidation`), an optional side and an optional `trade_id`.

Each level is watched with a small state machine. With ATR = ATR(14) on 1m bars (floored at 0.01% of price):

- **Cross:** price reaches or passes the level relative to the side it was last on. Fires once, then disarms.
- **Near:** price first comes within 0.25 ATR. Fires once.
- **Re-arm:** both re-arm when price moves more than 0.5 ATR away, so chatter around a level is suppressed.
- **New level:** takes the current side and fires nothing at creation. If price is already within the band, Near
  stays disarmed.
- **Replacement keeps state:** on `set_levels`, a level with the same kind, price and trade id keeps its state, so the
  bot's 60-second re-sync does not re-fire anything.

Each `levels_hit` entry carries `touch: cross | near` and `direction`.

**Level tier:** 2 when a touched level belongs to a trade (has a `trade_id`, or is sl/tp/liquidation); 1 for a pure
thesis level.

## 9. Agent rules

Code: `src/rules.rs`. Rules are typed data in a closed list of conditions combined with `all`. They are never code and
never an expression language.

| Condition | Fields | True when |
|---|---|---|
| `price_cross` | `level`, `dir` | price is beyond `level` in `dir` **and** has been on the other side since creation or the last fire |
| `move_z` | `min`, `window_s`, `dir?` | \|z\| of that window ≥ `min` (optionally in `dir`) |
| `volume_x` | `min`, `window_s` | `vol_x` of that window ≥ `min` |
| `taker_imbalance` | `min` (0.5–1], `side` buy\|sell, `window_s` | taker share on `side` ≥ `min` |
| `liq_burst` | `min` | 60s liquidation multiple ≥ `min` |
| `oi_change_z` | `min`, `dir?` | \|OI z\| ≥ `min` (optionally in `dir`) |
| `funding_above` | `rate` | current funding rate > `rate` |

`window_s` must be 60, 300 or 900. Adding a condition type means adding Rust code and a test.

**Lifecycle**

- `create_rule` validates the rule, and every error names the field (`invalid when.all[1].window_s: must be 60, 300
  or 900`).
- Plan direction is checked: a long needs `sl < entry < tp`, a short the reverse; with no entry, `sl < tp` for a long.
- The symbol must be a known USDT perpetual; it is added to the subscription for the rule's lifetime.
- **Rule ids:** the bot should pass its own `rule_id` and resend it on every sync. Resending a live id returns
  `exists` and changes nothing. Resending a fired or cancelled id returns `already_fired_or_cancelled` until the
  rule's original expiry, so a once-rule can't be resurrected by the sync loop. Without an id, the service assigns
  `r_<n>`.
- **Firing:**
  - `once` (default true): fires once, then the shard drops it and tells control, which tombstones the id.
  - `once: false`: fires at most once per 15 minutes.
- **Expiry:** `ttl_s` defaults to 86,400, with a range of 60 to 604,800. Control removes expired rules every 5 seconds;
  shards also check expiry before firing.
- **Bounds:** at most 1,000 rules in total and 8 conditions per rule. Per-user and per-symbol caps belong to the bot.
- **Ownership:** `cancel_rule` only succeeds for the owning `user_id`. The service trusts `owner` from an
  authenticated caller.
- **Traceability:** `parent_seq` links a rule to the event that inspired it, so rule-sets-rule loops are visible in
  logs and events.

**Rule tier:** `wake` = 2, `notify` = 1.

## 10. From one evaluation to events

One evaluation can produce several things at once. They are combined so each trigger is reported once:

1. If a jump, liquidation or OI candidate passes the gate: **one market event** (`kind` = jump|liq|oi). It also
   carries any `levels_hit` from this evaluation and the first matched rule. Tier = max(market, level, rule).
2. Else, if levels were touched: **one level event** carrying the first matched rule. Tier = max(level, rule).
3. Each remaining matched rule: **its own rule event**, with direction from the 5m move.

**Tier 0 (protect)** is not a separate tier value. It is implied by any `levels_hit` entry of kind `sl`, `tp` or
`liquidation`, and `touch: cross` means the price was reached. The event's `tier` therefore says how loudly to react
(1 heads-up, 2 please check), and `levels_hit` says whether to protect first.

## 11. Exposed surface

### HTTP

| Route | Auth | Purpose |
|---|---|---|
| `GET /healthz` | none | returns `ok`, for Docker health checks |
| `/mcp` (streamable HTTP, `rmcp`) | `x-api-key` must equal `CRABBIAN_API_KEY`, otherwise 401 | all tools |

The service is meant for the internal Docker network only. Every tool has typed arguments (`schemars`-derived structs
with named, described fields); none takes a bare JSON value.

### MCP tools

| Tool | Arguments | Returns |
|---|---|---|
| `set_interest` | `symbols: [string]` | `{added[], removed[], unknown[]}`. Replaces the interest set; unknown symbols are listed and the rest applied. Errors if the total subscription would exceed 511 symbols |
| `set_levels` | `levels: [{symbol, price, kind, side?, trade_id?}]` | `{count, ignored}`. Replaces all levels; unknown symbols and non-positive prices are counted in `ignored` |
| `create_rule` | a `RuleSpec` (section 9) | `{rule_id, status: created\|exists\|already_fired_or_cancelled}`, or an error naming the invalid field |
| `list_rules` | `owner: {user_id, agent}` | `{rules: [{rule_id, symbol, when, action, note, plan, once, seconds_left, parent_seq}]}` |
| `cancel_rule` | `rule_id`, `user_id` | `{cancelled}` |
| `symbol_state` | `symbol` | price, `sigma_1m`, `z_1m/5m/15m`, `move_pct_5m`, `vol_x_1m/5m/15m`, `taker_buy_share` (5m), `liq_usd_60s`, funding, mark, `oi_z`, `atr_1m`, `cooldown_until_up/down`, `warm`, `warm_at`, `bars_1m`, levels, rules. Errors if not watched |
| `wait_events` | `after: u64`, `epoch?: string`, `timeout_s: 1..=25` | `{epoch, next, dropped, events[]}` |
| `watcher_status` | none | epoch, uptime, `rss_mib`, `feed_connected`, `feed_age_ms`, `feed_down_s`, `msgs_per_sec`, `dropped_ticks`, `parse_errors`, `reconnects`, interest, symbols, `warm_symbols`, `warming_up`, levels, rules, `known_symbols`, `events_today`, `events_total`, `last_seq` |

### `wait_events` contract

This is the contract the bot's pump depends on. Code: `src/events.rs`.

- Returns at once if any event has `seq > after`; otherwise waits on `Notify` until `timeout_s`, then returns an empty
  list with `next = after`.
- If `epoch` is missing or differs from the current one, it returns at once with the current epoch, `dropped: 0`, and
  everything in the ring, even when the ring is empty. The pump resets its cursor to `next`.
- If `after` is older than the oldest event in the ring, `dropped` is the number of events missed.
- `next` is the highest `seq` returned. `seq` starts at 1 per process. Delivery is at-least-once; dedupe by `seq`.
- The ring holds the last 1,000 events; older ones drop silently apart from `dropped`.

### Event shape

```json
{ "seq": 1042, "epoch": "b7c1e2a9", "ts": "2026-10-02T14:03:07.412Z", "symbol": "SOLUSDT",
  "tier": 2, "kind": "jump", "direction": "down",
  "signals": { "price": 141.18, "z1": -6.1, "z5": -4.6, "z15": -2.9, "window_s": 60, "move_pct": -2.3,
               "vol_x": 5.2, "taker_buy": 0.22, "taker_sell": 0.78, "liq_usd": 3100000, "liq_x": 4.1,
               "oi_z": 1.8, "oi_read": "shorts_building", "funding": 0.0001, "confirmers": ["volume", "taker", "liq"] },
  "levels_hit": [ { "kind": "sl", "trade_id": 812, "side": "long", "price": 141.2, "touch": "cross", "direction": "down" } ],
  "rule": { "rule_id": "r_91", "owner": { "user_id": 42, "agent": "supervisor" }, "note": "...", "action": "wake",
            "plan": { "side": "long", "entry": null, "sl": 146.0, "tp": 158.0 }, "parent_seq": 1001 } }
```

- `kind` is one of `jump | level | liq | oi | rule`.
- Every `signals` field except `price` is omitted when it doesn't apply. `levels_hit` and `rule` are omitted when
  empty.
- Each event is also logged as one JSON line with `seq`, `symbol`, `tier`, `kind`, `direction`, `z5`, `move_pct`,
  `levels` and `rule`.

## 12. Binance integration

**WebSocket.** One connection to `wss://fstream.binance.com/market/stream?streams=!forceOrder@arr`.

- Binance split its futures WebSocket into `/public`, `/market` and `/private` routes. `aggTrade`, `markPrice` and
  `forceOrder` all belong to `/market`, and the unrouted URLs stopped carrying them on 2026-04-23.
- Per-symbol streams are `<symbol>@aggTrade` and `<symbol>@markPrice@1s` (which carries funding), added and removed
  live with `SUBSCRIBE` / `UNSUBSCRIBE`. Changing the interest set never needs a reconnect.

| Binance limit | How it is respected |
|---|---|
| 1,024 streams per connection | 2 per symbol + 1 global ⇒ at most 511 symbols, enforced by `set_interest`, `set_levels` and `create_rule` |
| 10 incoming messages per second | subscribe changes batched 100 streams per message and paced 250 ms apart (≤ 4/s) |
| 24h connection lifetime | at 23h a second connection is opened and subscribed while the first keeps streaming, then the first is closed. Duplicate trades in the overlap are dropped by aggTrade id. A failed rotation retries after 60s |
| Ping every 3 min, pong within 10 min | tungstenite answers pings while the read loop runs |

**REST** (via `binance-sdk`, no credentials)

| Call | When | Use |
|---|---|---|
| `exchangeInfo` | at start, then hourly | known symbols: `PERPETUAL`, quote `USDT`, status `TRADING` |
| `klines` 1m | when a symbol is added (last 1,500 minutes, pages of 1,000, continuing from the last candle received); after a reconnect (from the last message minus 2 min) | 1m ring, σ, medians, ATR |
| `openInterestHist` 5m | when a symbol is added (7 days, pages of 500) | OI baseline available immediately, instead of after a week |
| `openInterest` | every 30s per subscribed symbol | live OI change |

The REST task is sequential, which keeps request weight low. Adding 30 symbols at once takes on the order of a minute
to backfill, which is well inside the 30-minute warm-up.

## 13. Failure behavior

| Situation | Behavior |
|---|---|
| Socket drops or errors | reconnect with backoff from 1s doubling to 30s, resubscribe the full set, broadcast the gap to shards, and backfill 1m klines from the last message. A gap over 60s restarts warm-up for every symbol |
| No message for 5s while subscribed | treated as dead and reconnected. `feed_age_ms` and `feed_down_s` show it; the bot treats more than 60s as "fast protection is blind" |
| Process restart | new epoch, empty rings refilled from REST, 30-minute warm-up for market events. Levels and `price_cross` rules protect again as soon as the bot re-syncs |
| Bot or pump down | events wait in the ring; past 1,000 the oldest drop and `dropped` reports it |
| Shard channel full | the tick is dropped and counted (`dropped_ticks`); the socket reader never blocks |
| Unknown symbol in `set_interest` | listed in `unknown`; the rest are applied |
| Level or rule for a symbol outside the interest set | accepted; the symbol is subscribed for as long as the level or rule exists |
| `exchangeInfo` unavailable | symbols are accepted on shape alone (`^[A-Z0-9]+USDT$`) until it loads |
| REST call fails | logged and skipped; the next add, gap or poll retries. The detector runs on whatever history exists |
| SIGTERM / Ctrl-C | the cancellation token stops every task, and axum drains in-flight requests |

## 14. Configuration

| Env var | Default | Notes |
|---|---|---|
| `CRABBIAN_API_KEY` | required | must match the bot's `CRABBIAN_API_KEY` |
| `CRABBIAN_BIND` | `0.0.0.0:8080` | |
| `BINANCE_WS_URL` | `wss://fstream.binance.com/market/stream` | `?streams=` is appended |
| `LOG_LEVEL` | `info` | `tracing` env-filter syntax |
| `LOG_PROCESS` | `crabbian` | logged at start |

A `.env` file in the working directory is loaded at startup (`dotenvy`); real environment variables take precedence.

Thresholds are **not** configuration. They are `const`s in `src/detector/consts.rs`:

| Constant | Value | Meaning |
|---|---|---|
| `JUMP_Z` | 4.0 / 3.5 / 3.0 | jump threshold per window |
| `JUMP_MIN_MOVE` | 0.003 | minimum absolute log move |
| `TIER2_Z`, `TIER2_CONFIRMERS` | 6.0, 2 | "please check" line |
| `EWMA_SPAN_MIN`, `SIGMA_FLOOR` | 240, 1e-5 | σ of 1m returns |
| `VOL_SURGE_X`, `TAKER_SHARE` | 3.0, 0.70 | confirmers |
| `LIQ_BURST_X`, `LIQ_P95_FLOOR_USD` | 2.0, 50,000 | liquidation burst |
| `OI_SHOCK_Z`, `OI_READ_Z`, `OI_MIN_SAMPLES` | 3.0, 1.0, 288 | open interest |
| `LEVEL_NEAR_ATR`, `LEVEL_REARM_ATR`, `ATR_PERIOD` | 0.25, 0.5, 14 | levels |
| `COOLDOWN_MS`, `REARM_Z`, `EXTEND_X` | 15 min, 1.5, 1.5 | gate |
| `WARMUP_MS`, `GAP_REWARM_MS` | 30 min, 60 s | warm-up |
| `BARS_1S`, `BARS_1M`, `BACKFILL_1M` | 1,800, 10,080, 1,500 | ring sizes and backfill |

Change values only in `consts.rs`, and pin the new behavior with a test.

## 15. Code map

```
src/
  main.rs              wiring: config, tracing, channels, task spawns, axum + rmcp, SIGTERM
  lib.rs               module list (the binary and the integration tests use the library)
  config.rs            env + .env
  mcp.rs               tool argument/result types, #[tool] handlers, x-api-key middleware
  control.rs           interest/levels/rules store, tombstones, expiry, subscription fan-out
  ingest.rs            WebSocket connect/rotate/reconnect, SUBSCRIBE diffing, watchdog, parse_frame, FeedStats
  rest.rs              exchangeInfo, klines, openInterest, openInterestHist; REST task
  shard.rs             Sym, Router, SymbolCtx (pure evaluation and event composition), shard task
  events.rs            Event types, EventBus ring, wait_events logic
  levels.rs            Level types and touch state machine
  rules.rs             RuleSpec/Condition types, validation, RuleRt matching
  ring.rs              fixed-capacity ring buffer
  detector/
    consts.rs          every threshold
    state.rs           SymbolState: bars, bins, merge, σ history, OI
    baselines.rs       EWMA, quantiles, window medians, liquidation p95, ATR, OI stats
    signals.rs         Metrics, measure, detect, OI reading
    gate.rs            cooldown, hysteresis, extension
tests/
  detector_replay.rs   replays through SymbolCtx with an injected clock
  wait_events.rs       cursor, epoch, dropped and wake-up semantics
  fixtures/frames.jsonl  Binance frames in the documented combined-stream format
```

Everything under `detector/`, plus `levels.rs`, `rules.rs` and `SymbolCtx`, is pure: no I/O, no tokio, and time is
passed in. That is what makes the replay tests deterministic.

## 16. Testing

The pre-commit hook runs the suite. Run only the targeted test for what you change, e.g. `cargo test --test
detector_replay`.

| Case | Test |
|---|---|
| Real jump caught within 10s of the move starting, at most one extension re-fire on a 2% run | `real_jump_is_caught_fast` |
| Thin wick (2% on tiny, balanced volume) rejected | `thin_wick_is_rejected` |
| Volatile chop fires at most once per direction | `volatile_stretch_does_not_fire_repeatedly` |
| Cooldown, small extension suppressed, 1.5× extension re-fires, re-arm after cooldown | `cooldown_and_hysteresis`, `gate::tests` |
| Warm-up blocks market events; SL cross still fires during warm-up as tier 2 | `warm_up_suppresses_market_events`, `level_touch_fires_during_warm_up_as_protect` |
| σ read as of window start | `sigma_is_read_as_of_window_start` |
| Zero volume and zero σ guarded (finite metrics, no events) | `zero_volume_and_zero_sigma_are_guarded` |
| One event per `once` rule, `parent_seq` carried | `once_rule_fires_once`, `rules::tests` |
| Frame parsing, aggTrade dedupe, mark, funding and liquidation applied | `fixture_frames_replay_into_the_shard` |
| `wait_events` immediate, timeout, wake-up, epoch change, dropped | `tests/wait_events.rs` |

The market scenarios are synthetic, generated from a seeded random walk calibrated to σ = 0.1% per minute and 100 units
of volume per minute. Recorded Binance traffic should be added to `tests/fixtures/` during Phase A, and the thresholds
tuned against it.

## 17. Decisions made during the build

These settle points the plan left open or describe differently. Each was chosen deliberately and is pinned by a test.

1. **Extension rule.** A re-fire inside the cooldown needs price to move a further 1.5× the last event's move past the
   last event, from a reference fixed for the cooldown episode. Measuring the total move from the window start
   instead would re-fire geometrically along a single steady move (0.4%, 0.6%, 0.9%, 1.35% …).
2. **Tier 0 is implied, not a value.** `tier` is 1 or 2. Protection is signaled by an sl/tp/liquidation entry in
   `levels_hit` with `touch: cross`. This matches the example event in CLAUDE.md (tier 2 with an sl hit).
3. **Levels and price-only rules ignore warm-up.** Market events wait 30 minutes; protection does not.
4. **`touch` and `direction` on level hits.** These separate an approach from an actual cross, so the bot protects
   only on `cross`.
5. **Idempotent `create_rule` by caller-chosen `rule_id`, with tombstones**, so the 60-second sync can resend every
   rule without duplicating it or re-arming a fired once-rule.
6. **`taker_imbalance` carries `side` and `window_s`**, and `move_z` / `oi_change_z` take an optional `dir`.
7. **OI baseline from `openInterestHist`.** It is available immediately instead of after 7 days of live polling.
8. **Backfill is 1,500 1m bars**, not 7 days. No baseline needs more than 24h, and it keeps REST weight low. The 1m
   ring still holds 7 days of live data.
9. **Per-trade trip prices** give millisecond reaction while keeping heavy math to once per second.
10. **`/market` route.** Required since Binance's route split; the spike's spot URL and the unrouted futures URL no
    longer carry these streams.

## 18. Open items

- **Performance targets** (per-message cost, RSS, trade-to-event latency, CPU) are designed for but not yet measured.
  `watcher_status` reports RSS and message rate; measure in Phase A.
- **Fixtures are synthetic.** Record real `aggTrade`, `forceOrder` and `markPrice` lines from the VPS.
- **Thresholds are first guesses**, especially the "very high" line (\|z\| ≥ 6, two confirmers). Phase D tunes them from
  logged events against what price did next.
- **Bot side is not built yet**: `crabbian_client.py`, `crabbian_sync.py`, `crabbian_pump.py`, the Tier 0/1/2 handlers
  and `crabbian_tools.py` in `../bg-agent-bot`. When the pump sees a once-rule event, it should delete that rule from
  Redis.
- **Reachability.** Confirm the VPS can reach `fstream.binance.com` and `fapi.binance.com`.
