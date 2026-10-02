pub mod baselines;
pub mod consts;
pub mod gate;
pub mod signals;
pub mod state;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub use gate::Gate;
pub use signals::{Candidate, Kind, Metrics, Window, detect, measure, oi_read};
pub use state::{Bar, SymbolState};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum Dir {
    Up,
    Down,
}

impl Dir {
    pub fn of(x: f64) -> Self {
        if x >= 0.0 { Dir::Up } else { Dir::Down }
    }

    pub fn sign(self) -> f64 {
        match self {
            Dir::Up => 1.0,
            Dir::Down => -1.0,
        }
    }
}

pub fn ratio(a: f64, b: f64, floor: f64) -> f64 {
    if b.is_finite() && b > floor && a.is_finite() {
        a / b
    } else {
        0.0
    }
}
