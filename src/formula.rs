use evalexpr::{
    ContextWithMutableVariables, DefaultNumericTypes, HashMapContext, Node, Value,
    build_operator_tree,
};
use schemars::JsonSchema;
use serde::Serialize;

use crate::detector::Metrics;

pub const MAX_LEN: usize = 500;

pub const VARS: &[(&str, &str)] = &[
    ("price", "last trade price"),
    ("mark", "mark price"),
    ("funding", "current funding rate, 0.0001 = 0.01%"),
    ("sigma_1m", "EWMA standard deviation of 1m log returns"),
    (
        "z_1m",
        "move over the last 1m in standard deviations (signed)",
    ),
    (
        "z_5m",
        "move over the last 5m in standard deviations (signed)",
    ),
    (
        "z_15m",
        "move over the last 15m in standard deviations (signed)",
    ),
    ("move_1m", "percent move over the last 1m (signed)"),
    ("move_5m", "percent move over the last 5m (signed)"),
    ("move_15m", "percent move over the last 15m (signed)"),
    ("vol_x_1m", "volume over the last 1m vs its 24h median"),
    ("vol_x_5m", "volume over the last 5m vs its 24h median"),
    ("vol_x_15m", "volume over the last 15m vs its 24h median"),
    (
        "taker_buy_1m",
        "share of taker volume that bought over the last 1m, 0 to 1",
    ),
    (
        "taker_buy_5m",
        "share of taker volume that bought over the last 5m, 0 to 1",
    ),
    (
        "taker_buy_15m",
        "share of taker volume that bought over the last 15m, 0 to 1",
    ),
    ("liq_usd", "liquidations in the last 60s, USD"),
    ("liq_long_usd", "long liquidations in the last 60s, USD"),
    ("liq_short_usd", "short liquidations in the last 60s, USD"),
    ("liq_x", "liq_usd vs the 24h p95 per minute"),
    (
        "oi_z",
        "5-minute open interest change z-score vs 7 days (signed)",
    ),
    ("oi_chg", "5-minute open interest log change"),
];

const LIVE: &[&str] = &[
    "price",
    "mark",
    "funding",
    "liq_usd",
    "liq_long_usd",
    "liq_short_usd",
];

pub const FUNCS: &[&str] = &[
    "math::abs",
    "min",
    "max",
    "floor",
    "round",
    "ceil",
    "math::sqrt",
    "math::ln",
    "math::exp",
    "math::pow",
    "if",
];

pub type Formula = Node<DefaultNumericTypes>;
pub type Ctx = HashMapContext<DefaultNumericTypes>;

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct Variable {
    pub name: String,
    pub meaning: String,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct Catalog {
    pub variables: Vec<Variable>,
    pub functions: Vec<String>,
    pub operators: String,
    pub notes: Vec<String>,
}

pub fn catalog() -> Catalog {
    Catalog {
        variables: VARS
            .iter()
            .map(|(n, m)| Variable {
                name: n.to_string(),
                meaning: m.to_string(),
            })
            .collect(),
        functions: FUNCS.iter().map(|f| f.to_string()).collect(),
        operators: "+ - * / % ^, == != < <= > >=, && || !, parentheses".into(),
        notes: vec![
            "The formula must evaluate to true or false, e.g. `math::abs(z_5m) >= 3.5 && vol_x_5m >= 2 && taker_buy_5m >= 0.65`".into(),
            "It is checked every second and fires when it becomes true (it must have been false first)".into(),
            "Until the symbol is warmed up (about 15 minutes after it is first watched) every variable except price, mark, funding and liquidations is NaN, and any comparison with NaN is false".into(),
            "Write numbers as decimals when dividing (2.0 / 3.0); integer division truncates".into(),
        ],
    }
}

fn names() -> String {
    VARS.iter().map(|(n, _)| *n).collect::<Vec<_>>().join(", ")
}

pub fn compile(expr: &str) -> Result<Formula, String> {
    let expr = expr.trim();
    if expr.is_empty() || expr.len() > MAX_LEN {
        return Err(format!("must be 1 to {MAX_LEN} characters"));
    }
    let node = build_operator_tree::<DefaultNumericTypes>(expr).map_err(|e| e.to_string())?;
    if let Some(v) = node
        .iter_variable_identifiers()
        .find(|v| !VARS.iter().any(|(n, _)| n == v))
    {
        return Err(format!("unknown variable `{v}`; use {}", names()));
    }
    if let Some(f) = node
        .iter_function_identifiers()
        .find(|f| !FUNCS.contains(f))
    {
        return Err(format!("unknown function `{f}`; use {}", FUNCS.join(", ")));
    }
    node.eval_boolean_with_context(&context(&Metrics::default()))
        .map_err(|e| format!("must evaluate to true or false: {e}"))?;
    Ok(node)
}

pub fn context(m: &Metrics) -> Ctx {
    let mut c = Ctx::new();
    let w = |i: usize, f: fn(&crate::detector::Window) -> f64| {
        if m.w[i].ok { f(&m.w[i]) } else { f64::NAN }
    };
    let values = [
        ("price", m.price),
        ("mark", m.mark),
        ("funding", m.funding),
        ("sigma_1m", m.sigma_1m),
        ("z_1m", w(0, |w| w.z)),
        ("z_5m", w(1, |w| w.z)),
        ("z_15m", w(2, |w| w.z)),
        ("move_1m", w(0, |w| w.move_pct())),
        ("move_5m", w(1, |w| w.move_pct())),
        ("move_15m", w(2, |w| w.move_pct())),
        ("vol_x_1m", w(0, |w| w.vol_x)),
        ("vol_x_5m", w(1, |w| w.vol_x)),
        ("vol_x_15m", w(2, |w| w.vol_x)),
        ("taker_buy_1m", w(0, |w| w.taker_buy)),
        ("taker_buy_5m", w(1, |w| w.taker_buy)),
        ("taker_buy_15m", w(2, |w| w.taker_buy)),
        ("liq_usd", m.liq_usd()),
        ("liq_long_usd", m.liq_long),
        ("liq_short_usd", m.liq_short),
        ("liq_x", m.liq_x),
        ("oi_z", m.oi_z.unwrap_or(f64::NAN)),
        ("oi_chg", m.oi_chg.unwrap_or(f64::NAN)),
    ];
    for (k, v) in values {
        let v = if m.warm || LIVE.contains(&k) {
            v
        } else {
            f64::NAN
        };
        let _ = c.set_value(k.into(), Value::Float(v));
    }
    c
}

pub fn holds(f: &Formula, ctx: &Ctx) -> bool {
    f.eval_boolean_with_context(ctx).unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::detector::Window;

    fn warm() -> Metrics {
        let w = Window {
            ok: true,
            start_px: 100.0,
            ln_move: 0.02,
            sigma_k: 0.005,
            z: 4.0,
            vol: 10.0,
            vol_x: 3.0,
            taker_buy: 0.8,
        };
        Metrics {
            price: 102.0,
            w: [w, w, w],
            warm: true,
            ..Default::default()
        }
    }

    #[test]
    fn a_formula_reads_the_live_metrics() {
        let f = compile("math::abs(z_5m) >= 3.5 && vol_x_5m >= 2 && taker_buy_5m >= 0.65").unwrap();
        assert!(holds(&f, &context(&warm())));
        let f = compile("z_5m <= -3.5").unwrap();
        assert!(!holds(&f, &context(&warm())));
    }

    #[test]
    fn baseline_variables_are_nan_until_warm() {
        let cold = Metrics {
            warm: false,
            ..warm()
        };
        assert!(!holds(&compile("z_5m >= 3").unwrap(), &context(&cold)));
        assert!(holds(&compile("price > 101").unwrap(), &context(&cold)));
    }

    #[test]
    fn bad_formulas_are_refused_with_the_reason() {
        assert!(compile("").is_err());
        assert!(
            compile("rsi > 70")
                .unwrap_err()
                .contains("unknown variable `rsi`")
        );
        assert!(
            compile("str::to_uppercase(\"a\") == \"A\"")
                .unwrap_err()
                .contains("unknown function")
        );
        assert!(compile("z_5m + 1").unwrap_err().contains("true or false"));
        assert!(compile("price = 3").is_err());
    }
}
