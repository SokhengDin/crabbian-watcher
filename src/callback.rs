use std::sync::Arc;
use std::time::Duration;

use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio_util::sync::CancellationToken;

use crate::events::{Event, EventBus};
use crate::shard::now_ms;

pub const SIGNATURE_HEADER: &str = "x-crabbian-signature";
const TIMEOUT_S: u64 = 10;
const WAIT_S: u64 = 25;
const RETRY_MIN_MS: u64 = 500;
const RETRY_MAX_MS: u64 = 15_000;
const GIVE_UP_MS: u64 = 5 * 60_000;

pub fn sign(secret: &str, body: &[u8]) -> String {
    let mut mac =
        Hmac::<Sha256>::new_from_slice(secret.as_bytes()).expect("hmac accepts any key length");
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

async fn deliver(
    http: &reqwest::Client,
    url: &str,
    secret: &str,
    e: &Event,
    ct: &CancellationToken,
) {
    let body = serde_json::to_vec(e).unwrap_or_default();
    let signature = sign(secret, &body);
    let deadline = now_ms() + GIVE_UP_MS;
    let mut wait = RETRY_MIN_MS;
    loop {
        let sent = http
            .post(url)
            .header("content-type", "application/json")
            .header(SIGNATURE_HEADER, &signature)
            .body(body.clone())
            .send()
            .await;
        let status = match sent {
            Ok(r) if r.status().is_success() => {
                tracing::info!(seq = e.seq, symbol = %e.symbol, tier = e.tier, "event delivered");
                return;
            }
            Ok(r) if r.status().is_client_error() => {
                tracing::error!(
                    seq = e.seq,
                    status = r.status().as_u16(),
                    "callback refused the event, not retried"
                );
                return;
            }
            Ok(r) => r.status().to_string(),
            Err(err) => err.to_string(),
        };
        if now_ms() > deadline {
            tracing::error!(seq = e.seq, symbol = %e.symbol, error = %status, "callback unreachable for 5 minutes, event dropped");
            return;
        }
        tracing::warn!(seq = e.seq, error = %status, retry_ms = wait, "callback failed");
        tokio::select! {
            _ = ct.cancelled() => return,
            _ = tokio::time::sleep(Duration::from_millis(wait)) => {}
        }
        wait = (wait * 2).min(RETRY_MAX_MS);
    }
}

pub async fn run(bus: Arc<EventBus>, url: String, secret: String, ct: CancellationToken) {
    let http = match reqwest::Client::builder()
        .timeout(Duration::from_secs(TIMEOUT_S))
        .build()
    {
        Ok(h) => h,
        Err(e) => {
            tracing::error!(error = %e, "callback client could not start; events are not delivered");
            return;
        }
    };
    let epoch = bus.epoch().to_string();
    let mut cursor = 0;
    loop {
        let r = tokio::select! {
            _ = ct.cancelled() => break,
            r = bus.wait(cursor, Some(&epoch), Duration::from_secs(WAIT_S)) => r,
        };
        if r.dropped > 0 {
            tracing::warn!(
                dropped = r.dropped,
                "events fell out of the ring before they were delivered"
            );
        }
        for e in &r.events {
            deliver(&http, &url, &secret, e, &ct).await;
            cursor = e.seq;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signature_is_hex_hmac_sha256_of_the_body() {
        assert_eq!(
            sign("key", b"The quick brown fox jumps over the lazy dog"),
            "sha256=f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }
}
