# crabbian-watcher

A small Rust service that holds WebSocket connections to Binance USD-M futures, analyzes every trade in memory, and
raises numbered events the moment something jumps. It is an MCP server: the sibling Python repo `../bg-agent-bot` (the
paper-trading desk) controls it through tool calls and receives events through a long-poll tool. It exists because the
desk's watcher agent only looks on `:00` and `:30`, so a volume surge at 9:10 is first seen at 9:30, after most of the
move and with no time to decide on an entry. Target: surge at 9:10 -> flash alert at 9:10 -> watcher-agent
recommendation (entry, SL, TP) within a minute.

crabbian-watcher is the sensor. The Python "watcher agent" in bg-agent-bot is the judge. Never call this crate "the watcher".

## Status

The Rust service is built: Phase 0, the Phase A service side, tiers 0/1/2, and rules (Phase D service side). See
`ARCHITECTURE.md` for the system as built and the decisions made during the build. Still open: the bot side in
`../bg-agent-bot`, recorded fixtures, measured performance, and threshold tuning.

## Hard rules (do not break, do not ask to loosen)

- **Memory only.** No database, no Redis, no file writes, no message broker. All state is lost on restart by design; the
  bot is the source of truth and re-sends it (see "Restart behavior").
- **No LLM, no trading, no credentials.** Binance public market data only. No API keys, no order endpoints, ever.
- **No budgets or caps on events.** The goal is speed to a decision. The first event for a symbol is never delayed or
  dropped. Cooldown and hysteresis only suppress repeats. Do not add per-hour/per-day wake limits, throttles that delay
  a first event, or "fuses" without the user asking. (Decided with the user; budgets were removed on purpose.)
- **Thresholds are `const`s in `src/detector/consts.rs`, not config and not tool arguments.** Agents choose values
  inside rules (price level, min volume multiple); they never choose detector logic or thresholds.
- **Hot path stays cheap.** No allocation, no locks, no `.await` on slow I/O, no per-message heavy math. Heavy math runs
  once per second per symbol or once per closed 1m bar.
- **The detector is pure.** `src/detector/` has no I/O, no clock reads (time is passed in), no tokio. Everything in it is
  replayable from recorded ticks in a unit test.
- **Typed schemas on every MCP tool.** No bare `serde_json::Value` parameters. Use `schemars`-derived structs with named
  fields (the Python side learned this the hard way: property-less objects arrive empty on some model APIs).
- **Auth on every tool call.** `x-api-key` header must match `CRABBIAN_API_KEY`. The service listens on the internal
  Docker network only.

## Decisions already made (do not re-litigate)

| Decision | Choice | Why |
|---|---|---|
| Transport | MCP, streamable HTTP | Agents control it, and the bot already speaks MCP to trade-ml |
| Event delivery | `wait_events` long-poll with a cursor | Rare events, ms latency, no inbound port on the bot, replay for free, multiple consumers |
| Not chosen | WebSocket between services, HTTP webhook, Redis stream, MCP notifications as the mechanism | Each adds protocol, retry or coupling and is no faster. Notifications may be added later as a wake-up hint only |
| Rule storage | Memory here; durable copy in the bot's Redis with the rule's TTL | No migration, expires on its own, re-sent after restart |
| Alert levels | Tier 0 protect, Tier 1 heads up, Tier 2 please check | Matches how the desk already works |
| Opening positions | Never here. The bot's watcher agent recommends; a watch that already has autopilot on can open through the bot's existing executor under its signed-off limits | The bot owns all trade logic and limits |

## Architecture

```
 bg-agent-bot (Python)                              crabbian-watcher (Rust, this repo)
 ---------------------                              ----------------------------------
 crabbian_sync  --MCP set_interest / set_levels-->  1 Interest + levels + rules   (in memory)
 agent @tools   --MCP create_rule/list/cancel--->        |  drives WS SUBSCRIBE / UNSUBSCRIBE
 agent @tools   --MCP symbol_state------------->         v
                                          Binance WS -> 2 Ingest -> shard tasks (own their symbols' state)
                                          Binance REST -> OI poll (30s), kline backfill (start/reconnect)
                                                         |
                                                         v
                                                    3 Detector (pure)   4 Level + rule matcher
                                                         \_____________  ____________/
                                                                       v
                                                              5 Gate (cooldown, hysteresis) -> event ring (seq, epoch)
 crabbian_pump  <--MCP wait_events (long-poll)-------------------------'
   -> Celery tasks over the bot's Redis: protect / flash alert / check_jump_event
```

Two planes over one protocol: control (tool calls into this service) and events (`wait_events` out of it). They meet at
the rule: every rule carries its owner and a note, so an event says whom to wake and why.

### Tasks and ownership

- One **ingest task** per WebSocket connection: reads frames, parses into small structs, sends to the shard that owns the
  symbol over a bounded `mpsc`. No parsing allocations beyond what `serde` needs.
- **N shard tasks** (start with 4, `hash(symbol) % N`). Each shard exclusively owns the state of its symbols, so there are
  no locks. A shard ticks once per second to close the 1s bin, run the detector, and push events.
- One **control task** applies `set_interest` / `set_levels` / rules to the shards by message, never by shared state.
- One **REST task**: OI poll every 30s for the interest set; kline backfill when a symbol is added or after a gap.
- The **event ring** is the only shared structure: `Mutex<VecDeque<Event>>` + `tokio::sync::Notify`, last 1,000 events.
  Pushes are rare, so the lock is not a hot path.

### Per-symbol state (fixed-size rings, about 0.7 MB per symbol)

| Ring | Contents | Size |
|---|---|---|
| 1s bars | open/high/low/close, volume, taker-buy volume, trade count | last 30 min (1,800) |
| 1m bars | same fields | last 7 days (10,080) |
| Baselines | EWMA σ of 1m log returns, median window volume (1/5/15 min), p95 per-minute liquidation notional, OI 5-min-change mean/σ | scalars, refreshed on each 1m bar close |
| Control | cooldown timers per direction, last-event price and z, current funding rate, mark price | scalars |

Baselines are recomputed when a 1m bar closes and cached. The per-second path only compares against cached values.

## Detector spec

Windows are 1, 5 and 15 minutes built from the 1s bars. Every move is measured against the symbol's own volatility.

| Signal | Source | Computation | Fires when |
|---|---|---|---|
| Price jump | aggTrade | `ln(P_now / P_{now-k})` divided by `sigma_k`. `sigma_k = sqrt(k) * EWMA stdev of 1m returns`, read as of the window start so the move never inflates its own σ | `\|z\|` >= 4.0 (1m) / 3.5 (5m) / 3.0 (15m) AND `\|move\|` >= 0.3% |
| Volume surge | aggTrade | window volume / median of same-length windows over the last 24h | >= 3x |
| Taker imbalance | aggTrade `m` flag | share of taker volume on the move's side | >= 70% |
| Liquidation burst | forceOrder | notional in 60s / 24h p95 of per-minute notional | >= 2x |
| OI shock | REST openInterest | 5-minute change z-score vs 7 days | `\|z\|` >= 3 |
| Level touch | levels from the bot | price crosses a level, or first comes within 0.25 ATR of it | any level (SL, TP, liquidation always count) |
| Rule match | agent rules | every condition in a rule is true | rule fires once per its `once` / cooldown |

- **Primary signals:** price jump, liquidation burst, OI shock, level touch. **Confirmers:** volume surge, taker
  imbalance, liquidation burst, OI shock (a signal never confirms itself).
- **Trigger rule:** one primary + at least one confirmer. A price jump with neither volume nor imbalance is dropped
  (thin-liquidity wicks). Level touches and rule matches skip the confirmer requirement.
- **OI reads the move:** price up + OI up = new longs; price down + OI up = shorts building; price moves + OI down =
  closing/squeeze, more likely to retrace. Attach the reading to the event; do not gate on it.
- **Cooldown:** 15 minutes per symbol and direction. **Hysteresis:** re-arm when `\|z\|` < 1.5; a re-fire inside the
  cooldown needs the move to extend 1.5x past the last event. **Warm-up:** no events for 15 minutes after start or a
  reconnect gap > 60s (baselines rebuilt from REST klines first).
- ATR for the level-proximity test is ATR(14) on 1m bars; `kand` is allowed for that and nothing else. EWMA, z-score and
  medians are hand-written.

### Tiers (computed here, acted on in the bot)

| Tier | Name | Condition | Bot action |
|---|---|---|---|
| 0 | Protect | price reaches an open trade's SL, TP or liquidation level | run `_enforce_protection` for that trade now |
| 1 | Heads up | primary + one confirmer | flash alert via `post_to_desk` (also lands in the desk timeline so every agent sees it); no agent run |
| 2 | Please check | any of: `\|z\|` >= 6 on any window; confirmers from two or more independent families (volume + taker imbalance count once); touches a level of an open trade or an agent rule | everything in Tier 1 + a watcher-agent run with the trigger in its prompt |

Tier 2 implies Tier 1; `levels_hit` with SL/TP/liquidation additionally implies Tier 0. The "very high" line (z >= 6, two
confirmers) is a first guess; Phase A tunes it.

## Agent rules

Typed data in a closed condition list. Never code, never a free-form expression language.

```json
{ "symbol": "SOLUSDT",
  "when": { "all": [ { "type": "price_cross", "level": 150.0, "dir": "up" },
                     { "type": "volume_x", "min": 2.0, "window_s": 300 } ] },
  "action": "wake",
  "note": "Thesis: a reclaim of 150 on volume invalidates the short",
  "plan": { "side": "long", "sl": 146.0, "tp": 158.0 },
  "ttl_s": 86400, "once": true,
  "owner": { "user_id": "0b6c1f7e-2d3a-4c5b-9e8f-7a6b5c4d3e2f", "agent": "supervisor" } }
```

- Ids from the bot are UUID strings (`owner.user_id`, `trade_id`), not integers.
- Conditions: `price_cross`, `move_z`, `volume_x`, `taker_imbalance`, `liq_burst`, `oi_change_z`, `funding_above`; combined
  with `all` only. Adding a condition type means adding Rust code and a test, never a config entry.
- `action`: `notify` (Tier 1 behavior) or `wake` (Tier 2). No action opens a trade.
- `plan` is optional and passive: it travels in the event so the woken agent confirms a decision made earlier.
- `ttl_s` default 86,400, maximum 604,800. Expired rules are dropped silently. A sanity bound of 1,000 rules total protects
  memory; per-user and per-symbol caps are enforced by the bot, not here.
- This service never decides who may create rules. It trusts `owner` from an authenticated caller; the bot fills it from
  its `AgentContext`.

## MCP surface

Streamable HTTP via `rmcp` on `axum`, route `/mcp/`, plus a plain `GET /healthz` for Docker. Tool descriptions are
LLM-facing and required.

| Tool | Arguments | Returns |
|---|---|---|
| `set_interest` | `symbols: Vec<String>` (USDT perp symbols) | `{added, removed, unknown[]}`; diffs and sends SUBSCRIBE/UNSUBSCRIBE; refuses beyond the stream limit |
| `set_levels` | `levels: Vec<Level>` where `Level {symbol, price, kind: thesis\|sl\|tp\|liquidation, side?, trade_id?}` | `{count}`; replaces all levels |
| `create_rule` | a `Rule` (above) | `{rule_id}` or an error naming the invalid field |
| `list_rules` | `owner: Owner` | that owner's active rules with seconds left |
| `cancel_rule` | `rule_id`, `user_id` | `{cancelled}`; only the owning user |
| `symbol_state` | `symbol` | `{sigma_1m, vol_x_5m, z_1m/5m/15m, taker_buy_share, funding, mark, oi_z, cooldown_until, warm}` |
| `wait_events` | `after: u64`, `epoch: Option<String>`, `timeout_s: u32` (1..=25) | `{epoch, next, dropped, events[]}` |
| `watcher_status` | none | feed age, symbols, msgs/sec, events today, epoch, rss (via `memory-stats`), warm-up state |

**`wait_events` semantics (the contract the bot's pump depends on):**
- Returns immediately if any event has `seq > after`; otherwise waits on `Notify` up to `timeout_s`, then returns an empty
  list with the same `next`.
- `epoch` is a random id generated at process start. If the caller's `epoch` differs (or is missing), return
  `dropped: 0`, the current epoch, and everything still in the ring; the pump resets its cursor and re-syncs.
- If `after` is older than the oldest event in the ring, set `dropped` to the number of missed events so the pump can log it.
- `next` is the highest `seq` returned (or `after` if none). Delivery is at-least-once; the pump dedupes by `seq`.

**Event shape:**

```json
{ "seq": 1042, "epoch": "b7c1", "ts": "2026-10-02T14:03:07.412Z", "symbol": "SOLUSDT",
  "tier": 2, "kind": "jump", "direction": "down",
  "signals": { "z5": -4.6, "move_pct": -2.3, "vol_x": 5.2, "taker_sell": 0.78, "liq_usd": 3100000, "oi_read": "shorts_building" },
  "levels_hit": [ { "kind": "sl", "trade_id": "6f1a2b3c-5d4e-4f60-8a7b-9c0d1e2f3a4b", "price": 141.2 } ],
  "rule": { "rule_id": "a1b2c3d4e5f6", "owner": { "user_id": "0b6c1f7e-2d3a-4c5b-9e8f-7a6b5c4d3e2f", "agent": "supervisor" }, "note": "...", "plan": { "side": "long", "sl": 146.0, "tp": 158.0 } } }
```

`kind` is `jump | level | liq | oi | rule`. `levels_hit` and `rule` are omitted when empty.

## Binance specifics (verify against current docs before relying on any of this)

- USD-M futures only. Base `wss://fstream.binance.com`. Use the combined-stream form and `SUBSCRIBE`/`UNSUBSCRIBE` for
  changes so no reconnect is needed when the interest set changes. The spike's spot URL (`stream.binance.com:9443`) is wrong
  for this service.
- Streams per symbol: `<symbol>@aggTrade`, `<symbol>@markPrice@1s` (carries funding). Global: `!forceOrder@arr`.
  Open interest has no stream: REST `openInterest` every 30s. Klines come from REST (max 1,000 per page; continue from the
  last candle received, not the requested chunk).
- Connection limits to respect: 24-hour connection lifetime (rotate proactively, overlap the new connection before dropping
  the old), an incoming-message rate limit for SUBSCRIBE calls, and a per-connection stream cap. Answer server pings (the
  tungstenite loop does this while you keep reading).
- Binance blocks some regions. Confirm the VPS can reach `fstream.binance.com` before building further.
- `aggTrade` fields used: `s` symbol, `p` price, `q` quantity, `T` trade time, `m` buyer-is-maker (taker side is sell when
  true). Prices arrive as strings; parse once into `f64` in the ingest task.
- Dependencies already in `Cargo.toml`: use `binance-sdk` for REST (klines, open interest) if it covers them, otherwise add
  `reqwest`. Keep raw `tokio-tungstenite` for the hot WebSocket path so parsing stays under our control.

## Failure behavior

| Situation | Behavior |
|---|---|
| Binance socket drops | Reconnect with backoff 1s -> 30s, resubscribe the whole interest set, backfill the gap from REST klines. Gap > 60s = warm-up again |
| No Binance message for 5s | Treat as dead, reconnect. `watcher_status` reports feed age; down > 60s is visible to the bot, which posts that fast protection is blind and relies on the 30-minute tick |
| Process restarts | New `epoch`, empty rings refilled from REST klines, 30-minute warm-up. The bot's sync re-sends interest, levels and rules within 60s (immediately when the pump sees the epoch change) |
| Bot or pump is down | Events wait in the ring. If more than 1,000 accumulate, the oldest drop and `dropped` reports it. The pump discards events older than 60s when it finally reads them |
| `set_interest` with an unknown symbol | Listed in `unknown`, the rest are applied |
| Rule or level for a symbol not in the interest set | Accepted; the symbol is added to the subscription for the rule's lifetime |
| Shard channel full | Drop the tick, count it in `watcher_status`; never block the socket reader |

## Conventions

These mirror `../bg-agent-bot/CLAUDE.md`, adapted to Rust. Follow without being asked.

- **No comments and no doc comments, except the `#[tool(description = ...)]` text and the schema field descriptions that the
  LLM sees.** If a reviewer would understand the code without the comment, do not write it.
- **Compact code.** Short expressions inline; break lines only when a line is genuinely long. No new abstraction layers
  where a function will do.
- **Logging:** `tracing` with JSON output to stdout only. No log files, no `println!`/`eprintln!`. One line per event with the
  fields the user will grep (`symbol`, `tier`, `kind`, `z5`, `seq`). Level from `LOG_LEVEL` (default `info`), process tag
  `LOG_PROCESS=crabbian`.
- **Errors:** typed errors (`thiserror`) inside modules, `anyhow` only in `main` and task boundaries. No `unwrap`/`expect`
  on the hot path or on anything fed by the network. `unwrap` on startup config is fine.
- **Time is injected** into the detector (`now_ms: u64` parameters) so tests are deterministic.
- **Floats:** `f64`. Guard every division (zero σ, zero volume) with the documented floors in `consts.rs`.
- Edition 2024, `cargo clippy -D warnings` clean, `cargo fmt` default.
- `margin x leverage = notional` and SL/TP direction rules live in the bot. Nothing here computes position size, but any
  rule `plan` validation must check direction: long needs SL below and TP above entry; short is the reverse.

## Planned layout

```
Cargo.toml
Dockerfile                      multi-stage; final image debian-slim or distroless
src/
  main.rs                       wiring only: config, tracing, tasks, axum + rmcp server
  config.rs                     env: CRABBIAN_API_KEY, CRABBIAN_BIND, BINANCE_WS_URL, LOG_LEVEL, LOG_PROCESS
  mcp.rs                        tool structs, #[tool] handlers, auth middleware
  ingest.rs                     WebSocket connect/rotate/reconnect, SUBSCRIBE diffing, watchdog, frame parsing
  rest.rs                       klines, openInterest
  shard.rs                      shard task: owns SymbolState, 1s tick, applies control messages
  control.rs                    interest/levels/rules store + fan-out to shards
  ring.rs                       fixed-size ring buffer
  events.rs                     Event types, event ring, wait_events logic, epoch/seq
  detector/
    mod.rs                      detect(&SymbolState, now_ms) -> Vec<Candidate>
    baselines.rs                EWMA sigma, window-volume median, liquidation p95, OI stats
    signals.rs                  one function per signal
    gate.rs                     cooldown, hysteresis, warm-up, tier assignment
    consts.rs                   every threshold in this file's tables
  rules.rs                      Rule/Condition types, validation, matching
tests/
  fixtures/                     recorded aggTrade/forceOrder/markPrice JSON lines
  detector_replay.rs            replay fixtures -> assert events
  wait_events.rs                cursor/epoch/dropped semantics at function level
```

## Build plan

Each phase ends in something verifiable. Do not start a phase before the previous acceptance holds.

**Phase 0: clean base**
- [ ] Replace the spike: config, `tracing` JSON logging, graceful shutdown. Remove `println!` output.
- [ ] Add dependencies: `tracing`, `tracing-subscriber` (json), `axum`, `schemars`, `uuid`, `thiserror`, `anyhow`. Switch `rmcp`
  to its streamable-HTTP server feature (check the feature names for the pinned version) and drop `transport-io` unless a
  stdio entry is wanted.
- [ ] Accept: binary starts, `GET /healthz` is 200, an authenticated MCP `watcher_status` call returns, an unauthenticated one is refused.

**Phase A: shadow (service + log-only pump)**
- [ ] `ingest.rs` against USD-M futures with SUBSCRIBE diffing, 24h rotation, reconnect, 5s watchdog.
- [ ] `ring.rs`, `shard.rs`, 1s/1m bars, baselines, REST kline backfill, OI poll.
- [ ] `detector/` complete with `consts.rs`; events pushed to the ring; `wait_events` and `set_interest`/`set_levels` working.
- [ ] Bot side: `crabbian_client.py`, `crabbian_sync.py`, `crabbian_pump.py` in log-only mode (logs each event, sends no tasks).
- [ ] Accept: a week of real events logged per symbol per day; measured end-to-end latency (trade time to pump log line);
  unit tests replay a recorded jump, a thin wick and a volatile stretch and produce the expected events.

**Phase B: protect and heads-up**
- [ ] Bot handlers: Tier 0 `_enforce_protection` on the event, Tier 1 flash alert through `post_to_desk`.
- [ ] Accept: a paper trade's SL is closed within seconds of the touch, not at the next half hour; heads-ups appear in Discord and in the agents' "Recent desk activity".

**Phase C: please check (wake the watcher agent)**
- [ ] Bot: `check_jump_event` on the `event` queue (concurrency 2) with the trigger block in the watcher prompt; takes the existing `watch-lock:*`.
- [ ] Tier 2 defined as above; first event never delayed.
- [ ] Accept: a real surge produces a flash alert within a second and a recommendation with entry/SL/TP within a minute, as a reply under the alert.

**Phase D: agent rules and tuning**
- [ ] `rules.rs`, `create_rule` / `list_rules` / `cancel_rule`; bot `app/tools/crabbian_tools.py` wrappers and Redis-with-TTL storage.
- [ ] Tune constants by hand from the logged events against what price did next (the bot already stores 1h candles). Change values in `consts.rs` only, with a test that pins the new behavior.
- [ ] Accept: an agent-set rule fires with its note and plan and wakes the owning agent; a loop of rule-sets-rule is visible in the log with parent event ids.

## Bot-side contract (implemented in `../bg-agent-bot`, not here)

- `app/core/crabbian_client.py`: same pattern as `app/core/mcp_client.py` (one session per process on its own event-loop
  thread, one retry after a stale session), settings `CRABBIAN_MCP_URL` and `CRABBIAN_API_KEY`.
- `app/tasks/crabbian_sync.py`: Celery beat every 60s builds the interest set (watches + open trades + analyst coverage) and
  levels (`AssetStrategy.key_levels`, open-trade SL/TP, `TradeService.liquidation_price`), plus every live rule from Redis, and
  calls `set_interest`, `set_levels`, `create_rule`. Also called right after a watch or trade changes.
- `app/tasks/crabbian_pump.py`: its own supervisord program. Loops on `wait_events`, tracks cursor and epoch, drops events
  older than 60s, dedupes by `seq`, sends Celery tasks (protect, flash alert, `check_jump_event`).
- `app/tools/crabbian_tools.py`: `set_market_alert`, `list_market_alerts`, `cancel_market_alert`, `live_market_state` as
  `@tool`s that fill `owner` from `AgentContext`. Allowed for supervisor, watcher agent and analyst; not the quant agent.
- Every Discord post still goes through `post_to_desk`, so the timeline records both the heads-up and the verdict.
- The watcher agent still never opens trades. Autopilot, when a watch already has it on, runs through the bot's existing
  `try_open_from_watch` with all its fixed limits.

## Testing and workflow

- The pre-commit hook runs the tests. Write them; do not run the whole suite by hand, and do not start the server or a
  local smoke script. Run only a targeted `cargo test <name>` for the module you changed.
- Detector tests are replays: feed fixture lines through the shard logic with an injected clock and assert the emitted events.
  Required cases: real jump caught; thin wick rejected; volatile stretch does not fire repeatedly; cooldown and hysteresis;
  warm-up; σ is read as of window start; zero-volume and zero-σ guards; one event per `once` rule.
- Prefer Edit/Write for code changes so they show as clean IDE diffs. Do not rewrite files with scripts.
- The user handles deploys. When a change needs a new env var or compose change, say so in one line; do not write a deploy walkthrough.

## Performance targets (verify in Phase A, do not assume)

- Per-message handling after parse: low single-digit microseconds.
- RSS at 30 symbols: under 50 MiB (`watcher_status` reports it).
- Trade-to-event latency inside the service: under 50 ms; trade-to-pump under 150 ms.
- CPU: a small fraction of one core at 30 symbols on the VPS.

## Glossary

- **Event:** a numbered record in the ring. **Tier:** how loudly the bot should react (0 protect, 1 heads up, 2 please check).
- **Primary / confirmer:** the signal that proposes an event / an independent signal that backs it up.
- **Epoch:** random id per process start; a change tells the pump the cursor is meaningless.
- **Interest set:** the symbols the bot wants watched (watches, open trades, analyst coverage).
- **Levels:** prices that matter to the desk (thesis levels, SL, TP, liquidation).
- **Rule:** an agent-created typed condition with an owner, a note, an optional plan and a TTL.
- **The watcher agent:** the Python LangGraph agent in bg-agent-bot. Not this service.
