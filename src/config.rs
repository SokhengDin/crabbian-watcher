use std::env;

#[derive(Debug, Clone)]
pub struct Config {
    pub api_key: String,
    pub bind: String,
    pub binance_ws_url: String,
    pub log_level: String,
    pub log_process: String,
}

impl Config {
    pub fn from_env() -> Self {
        dotenvy::dotenv().ok();
        let var = |k: &str, d: &str| env::var(k).unwrap_or_else(|_| d.to_string());
        Self {
            api_key: env::var("CRABBIAN_API_KEY").expect("CRABBIAN_API_KEY is required"),
            bind: var("CRABBIAN_BIND", "0.0.0.0:8080"),
            binance_ws_url: var("BINANCE_WS_URL", "wss://fstream.binance.com/market/stream"),
            log_level: var("LOG_LEVEL", "info"),
            log_process: var("LOG_PROCESS", "crabbian"),
        }
    }
}
