pub const SEC_MS: u64 = 1_000;
pub const MIN_MS: u64 = 60_000;

pub const BARS_1S: usize = 1_800;
pub const BARS_1M: usize = 10_080;
pub const BACKFILL_1M: u64 = 1_500;
pub const DAY_1M: usize = 1_440;

pub const WINDOWS_S: [u64; 3] = [60, 300, 900];
pub const JUMP_Z: [f64; 3] = [4.0, 3.5, 3.0];
pub const JUMP_MIN_MOVE: f64 = 0.003;
pub const TIER2_Z: f64 = 6.0;
pub const TIER2_CONFIRMERS: usize = 2;

pub const EWMA_SPAN_MIN: f64 = 240.0;
pub const SIGMA_FLOOR: f64 = 1e-5;

pub const VOL_SURGE_X: f64 = 3.0;
pub const VOL_FLOOR: f64 = 1e-12;
pub const TAKER_SHARE: f64 = 0.70;

pub const LIQ_WINDOW_S: u64 = 60;
pub const LIQ_BURST_X: f64 = 2.0;
pub const LIQ_P95_FLOOR_USD: f64 = 50_000.0;

pub const OI_HIST_CAP: usize = 2_100;
pub const OI_LIVE_CAP: usize = 32;
pub const OI_STEP_MS: u64 = 5 * MIN_MS;
pub const OI_SHOCK_Z: f64 = 3.0;
pub const OI_READ_Z: f64 = 1.0;
pub const OI_SIGMA_FLOOR: f64 = 1e-4;
pub const OI_MIN_SAMPLES: usize = 288;

pub const ATR_PERIOD: usize = 14;
pub const ATR_BARS: usize = 120;
pub const ATR_FLOOR_FRAC: f64 = 1e-4;
pub const LEVEL_NEAR_ATR: f64 = 0.25;
pub const LEVEL_REARM_ATR: f64 = 0.5;

pub const COOLDOWN_MS: u64 = 15 * MIN_MS;
pub const REARM_Z: f64 = 1.5;
pub const EXTEND_X: f64 = 1.5;
pub const WARMUP_MS: u64 = 15 * MIN_MS;
pub const GAP_REWARM_MS: u64 = 60_000;
