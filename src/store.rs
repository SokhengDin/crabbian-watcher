use std::collections::BTreeMap;
use std::time::Duration;

use redis::aio::ConnectionManager;
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::levels::Level;
use crate::rules::RuleSpec;

pub const KEY_INTEREST: &str = "crabbian:interest";
pub const KEY_LEVELS: &str = "crabbian:levels";
pub const KEY_RULES: &str = "crabbian:rules";
const RETRY_MIN_MS: u64 = 500;
const RETRY_MAX_MS: u64 = 15_000;

pub type LevelGroups = BTreeMap<String, Vec<Level>>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SavedRule {
    pub spec: RuleSpec,
    pub expires_ms: u64,
}

#[derive(Debug, Default)]
pub struct Saved {
    pub interest: Vec<String>,
    pub levels: LevelGroups,
    pub rules: Vec<(String, SavedRule)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Persist {
    Interest(Vec<String>),
    Levels(String, Vec<Level>),
    DropLevels(String),
    AllLevels(LevelGroups),
    Rule(String, Box<SavedRule>),
    Drop(String),
}

pub type Store = (Option<mpsc::UnboundedSender<Persist>>, Saved);

pub async fn connect(url: &str) -> anyhow::Result<ConnectionManager> {
    Ok(redis::Client::open(url)?.get_connection_manager().await?)
}

fn parse<T: for<'de> Deserialize<'de> + Default>(key: &str, raw: Option<String>) -> T {
    raw.and_then(|s| {
        serde_json::from_str(&s)
            .map_err(|e| tracing::warn!(key, error = %e, "unreadable saved state ignored"))
            .ok()
    })
    .unwrap_or_default()
}

pub async fn load(con: &mut ConnectionManager) -> anyhow::Result<Saved> {
    let interest: Option<String> = redis::cmd("GET").arg(KEY_INTEREST).query_async(con).await?;
    let levels: Vec<(String, String)> = redis::cmd("HGETALL")
        .arg(KEY_LEVELS)
        .query_async(con)
        .await?;
    let rules: Vec<(String, String)> = redis::cmd("HGETALL")
        .arg(KEY_RULES)
        .query_async(con)
        .await?;
    Ok(Saved {
        interest: parse(KEY_INTEREST, interest),
        levels: entries("levels", levels).collect(),
        rules: entries("rule", rules).collect(),
    })
}

fn entries<T: for<'de> Deserialize<'de>>(
    what: &'static str,
    raw: Vec<(String, String)>,
) -> impl Iterator<Item = (String, T)> {
    raw.into_iter()
        .filter_map(move |(id, raw)| match serde_json::from_str(&raw) {
            Ok(r) => Some((id, r)),
            Err(e) => {
                tracing::warn!(what, id = %id, error = %e, "unreadable saved entry ignored");
                None
            }
        })
}

fn json<T: Serialize>(v: &T) -> String {
    serde_json::to_string(v).unwrap_or_default()
}

pub fn pipeline(p: &Persist) -> redis::Pipeline {
    let mut pipe = redis::pipe();
    pipe.atomic();
    match p {
        Persist::Interest(s) => pipe.cmd("SET").arg(KEY_INTEREST).arg(json(s)),
        Persist::Levels(key, l) => pipe.cmd("HSET").arg(KEY_LEVELS).arg(key).arg(json(l)),
        Persist::DropLevels(key) => pipe.cmd("HDEL").arg(KEY_LEVELS).arg(key),
        Persist::AllLevels(groups) => {
            pipe.cmd("DEL").arg(KEY_LEVELS);
            for (key, l) in groups {
                pipe.cmd("HSET").arg(KEY_LEVELS).arg(key).arg(json(l));
            }
            &mut pipe
        }
        Persist::Rule(id, r) => pipe.cmd("HSET").arg(KEY_RULES).arg(id).arg(json(r)),
        Persist::Drop(id) => pipe.cmd("HDEL").arg(KEY_RULES).arg(id),
    };
    pipe
}

async fn backoff(ms: &mut u64, ct: &CancellationToken) -> bool {
    tokio::select! {
        _ = ct.cancelled() => return false,
        _ = tokio::time::sleep(Duration::from_millis(*ms)) => {}
    }
    *ms = (*ms * 2).min(RETRY_MAX_MS);
    true
}

pub async fn run_writer(
    mut con: ConnectionManager,
    mut rx: mpsc::UnboundedReceiver<Persist>,
    ct: CancellationToken,
) {
    loop {
        let p = tokio::select! {
            _ = ct.cancelled() => break,
            Some(p) = rx.recv() => p,
        };
        let pipe = pipeline(&p);
        let mut wait = RETRY_MIN_MS;
        loop {
            match pipe.query_async::<()>(&mut con).await {
                Ok(()) => break,
                Err(e) => {
                    tracing::warn!(error = %e, retry_ms = wait, "saving state to redis failed");
                    if !backoff(&mut wait, &ct).await {
                        return;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rules::tests::spec;

    fn packed(p: Persist) -> String {
        String::from_utf8_lossy(&pipeline(&p).get_packed_pipeline()).into_owned()
    }

    #[test]
    fn a_saved_rule_round_trips_and_maps_to_the_rules_hash() {
        let r = SavedRule {
            spec: spec(),
            expires_ms: 1_790_000_000_000,
        };
        let back: SavedRule = serde_json::from_str(&json(&r)).unwrap();
        assert_eq!(back, r);
        let set = packed(Persist::Rule("a1".into(), Box::new(r)));
        assert!(set.contains("HSET") && set.contains(KEY_RULES) && set.contains("a1"));
        let del = packed(Persist::Drop("a1".into()));
        assert!(del.contains("HDEL") && del.contains(KEY_RULES));
        let interest = packed(Persist::Interest(vec!["SOLUSDT".into()]));
        assert!(interest.contains(KEY_INTEREST) && interest.contains("[\"SOLUSDT\"]"));
    }

    #[test]
    fn level_groups_are_fields_of_one_hash() {
        let put = packed(Persist::Levels("trade:7".into(), Vec::new()));
        assert!(put.contains("HSET") && put.contains(KEY_LEVELS) && put.contains("trade:7"));
        let all = packed(Persist::AllLevels(LevelGroups::from([(
            "a".into(),
            Vec::new(),
        )])));
        assert!(all.contains("DEL") && all.contains("MULTI") && all.contains("EXEC"));
    }

    #[test]
    fn unreadable_state_falls_back_to_empty() {
        assert_eq!(
            parse::<Vec<String>>(KEY_INTEREST, Some("{oops".into())),
            Vec::<String>::new()
        );
        assert_eq!(
            parse::<Vec<String>>(KEY_INTEREST, None),
            Vec::<String>::new()
        );
    }
}
