use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use schemars::JsonSchema;
use serde::Serialize;
use tokio::sync::{mpsc, oneshot, watch};
use tokio_util::sync::CancellationToken;

use crate::levels::Level;
use crate::rest::RestJob;
use crate::rules::{Action, MAX_RULES, Owner, Plan, RuleEntry, RuleSpec, When, valid_symbol};
use crate::shard::{Ctrl, Router, now_ms};
use crate::store::{LevelGroups, Persist, Saved, SavedRule, Store};

pub const MAX_SYMBOLS: usize = (1024 - 1) / 2;
pub const SET_LEVELS_KEY: &str = "set_levels";

pub type Wanted = Arc<BTreeSet<String>>;

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct InterestResult {
    pub added: Vec<String>,
    pub removed: Vec<String>,
    pub unknown: Vec<String>,
    #[schemars(description = "The full interest list after the change")]
    pub interest: Vec<String>,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct LevelsResult {
    pub count: usize,
    #[schemars(
        description = "Levels skipped because the symbol is unknown or the price is not positive"
    )]
    pub ignored: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CreateStatus {
    Created,
    Exists,
    AlreadyFiredOrCancelled,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct CreateResult {
    pub rule_id: String,
    pub status: CreateStatus,
}

#[derive(Debug, Clone, Serialize, JsonSchema)]
pub struct RuleView {
    pub rule_id: String,
    pub symbol: String,
    pub owner: Owner,
    pub when: When,
    pub action: Action,
    pub note: String,
    pub plan: Option<Plan>,
    pub once: bool,
    pub seconds_left: u64,
    pub parent_seq: Option<u64>,
}

#[derive(Debug, Clone, Default)]
pub struct RuleFilter {
    pub user_id: String,
    pub agent: Option<String>,
    pub symbol: Option<String>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ControlStatus {
    pub interest: usize,
    pub subscribed: usize,
    pub levels: usize,
    pub rules: usize,
    pub known_symbols: usize,
}

pub enum Cmd {
    SetInterest(Vec<String>, oneshot::Sender<Result<InterestResult, String>>),
    UpdateInterest(
        Vec<String>,
        Vec<String>,
        oneshot::Sender<Result<InterestResult, String>>,
    ),
    SetLevels(Vec<Level>, oneshot::Sender<Result<LevelsResult, String>>),
    PutLevels(
        String,
        Vec<Level>,
        oneshot::Sender<Result<LevelsResult, String>>,
    ),
    RemoveLevels(String, oneshot::Sender<Result<bool, String>>),
    CreateRule(Box<RuleSpec>, oneshot::Sender<Result<CreateResult, String>>),
    ListRules(RuleFilter, oneshot::Sender<Vec<RuleView>>),
    CancelRule(String, String, oneshot::Sender<bool>),
    Known(HashSet<String>),
    Status(oneshot::Sender<ControlStatus>),
}

struct Control {
    known: HashSet<String>,
    interest: BTreeSet<String>,
    groups: LevelGroups,
    levels: BTreeMap<String, Vec<Level>>,
    rules: BTreeMap<String, Arc<RuleEntry>>,
    tombs: HashMap<String, u64>,
    next_id: u64,
    active: BTreeSet<String>,
    router: Router,
    want: watch::Sender<Wanted>,
    rest: mpsc::UnboundedSender<RestJob>,
    store: Option<mpsc::UnboundedSender<Persist>>,
}

impl Control {
    fn persist(&self, p: Persist) {
        if let Some(s) = &self.store {
            let _ = s.send(p);
        }
    }

    fn known(&self, s: &str) -> bool {
        valid_symbol(s) && (self.known.is_empty() || self.known.contains(s))
    }

    fn desired_with(
        &self,
        interest: &BTreeSet<String>,
        levels: &BTreeMap<String, Vec<Level>>,
        extra: Option<&str>,
    ) -> BTreeSet<String> {
        let mut d: BTreeSet<String> = interest.iter().chain(levels.keys()).cloned().collect();
        d.extend(self.rules.values().map(|r| r.spec.symbol.clone()));
        d.extend(extra.map(String::from));
        d
    }

    fn rules_for(&self, sym: &str) -> Vec<Arc<RuleEntry>> {
        self.rules
            .values()
            .filter(|r| r.spec.symbol == sym)
            .cloned()
            .collect()
    }

    fn sync(&mut self) {
        let desired = self.desired_with(&self.interest, &self.levels, None);
        for s in self.active.difference(&desired) {
            self.router.ctrl(s, Ctrl::Remove(s.clone()));
        }
        for s in desired.difference(&self.active) {
            self.router.ctrl(s, Ctrl::Add(s.clone()));
            self.router.ctrl(
                s,
                Ctrl::Levels(s.clone(), self.levels.get(s).cloned().unwrap_or_default()),
            );
            self.router
                .ctrl(s, Ctrl::Rules(s.clone(), self.rules_for(s)));
            let _ = self.rest.send(RestJob::Backfill(s.clone()));
        }
        if desired != self.active {
            tracing::info!(symbols = desired.len(), "subscription set changed");
            self.active = desired.clone();
            let _ = self.want.send(Arc::new(desired));
        }
    }

    fn push_rules(&self, sym: &str) {
        self.router
            .ctrl(sym, Ctrl::Rules(sym.to_string(), self.rules_for(sym)));
    }

    fn set_interest(&mut self, symbols: Vec<String>) -> Result<InterestResult, String> {
        let mut unknown = Vec::new();
        let mut next = BTreeSet::new();
        for s in symbols.iter().map(|s| s.trim().to_ascii_uppercase()) {
            if self.known(&s) {
                next.insert(s);
            } else {
                unknown.push(s);
            }
        }
        let total = self.desired_with(&next, &self.levels, None).len();
        if total > MAX_SYMBOLS {
            return Err(format!(
                "{total} symbols exceeds the stream limit of {MAX_SYMBOLS} per connection"
            ));
        }
        let added = next.difference(&self.interest).cloned().collect();
        let removed = self.interest.difference(&next).cloned().collect();
        self.interest = next;
        self.sync();
        let interest: Vec<String> = self.interest.iter().cloned().collect();
        self.persist(Persist::Interest(interest.clone()));
        Ok(InterestResult {
            added,
            removed,
            unknown,
            interest,
        })
    }

    fn update_interest(
        &mut self,
        add: Vec<String>,
        remove: Vec<String>,
    ) -> Result<InterestResult, String> {
        let remove: BTreeSet<String> = remove
            .iter()
            .map(|s| s.trim().to_ascii_uppercase())
            .collect();
        let next = self
            .interest
            .iter()
            .cloned()
            .chain(add)
            .filter(|s| !remove.contains(&s.trim().to_ascii_uppercase()))
            .collect();
        self.set_interest(next)
    }

    fn clean(&self, levels: Vec<Level>) -> (Vec<Level>, usize) {
        let total = levels.len();
        let ok: Vec<Level> = levels
            .into_iter()
            .map(|mut l| {
                l.symbol = l.symbol.trim().to_ascii_uppercase();
                l
            })
            .filter(|l| self.known(&l.symbol) && l.price.is_finite() && l.price > 0.0)
            .collect();
        let ignored = total - ok.len();
        (ok, ignored)
    }

    fn apply_groups(&mut self, groups: LevelGroups) -> Result<(), String> {
        let mut by: BTreeMap<String, Vec<Level>> = BTreeMap::new();
        for l in groups.values().flatten() {
            by.entry(l.symbol.clone()).or_default().push(l.clone());
        }
        let total = self.desired_with(&self.interest, &by, None).len();
        if total > MAX_SYMBOLS {
            return Err(format!(
                "{total} symbols exceeds the stream limit of {MAX_SYMBOLS} per connection"
            ));
        }
        self.groups = groups;
        let old = std::mem::replace(&mut self.levels, by);
        self.sync();
        for s in old
            .keys()
            .chain(self.levels.keys())
            .collect::<BTreeSet<_>>()
        {
            if old.get(s) != self.levels.get(s) {
                self.router.ctrl(
                    s,
                    Ctrl::Levels(s.clone(), self.levels.get(s).cloned().unwrap_or_default()),
                );
            }
        }
        Ok(())
    }

    fn set_levels(&mut self, levels: Vec<Level>) -> Result<LevelsResult, String> {
        let (levels, ignored) = self.clean(levels);
        let count = levels.len();
        let groups = if levels.is_empty() {
            LevelGroups::new()
        } else {
            LevelGroups::from([(SET_LEVELS_KEY.to_string(), levels)])
        };
        self.apply_groups(groups.clone())?;
        self.persist(Persist::AllLevels(groups));
        Ok(LevelsResult { count, ignored })
    }

    fn put_levels(&mut self, key: String, levels: Vec<Level>) -> Result<LevelsResult, String> {
        let key = key.trim().to_string();
        if key.is_empty() || key.len() > 100 {
            return Err("invalid key: use 1 to 100 characters".into());
        }
        let (levels, ignored) = self.clean(levels);
        let count = levels.len();
        if levels.is_empty() {
            self.remove_levels(key)?;
            return Ok(LevelsResult { count, ignored });
        }
        let mut groups = self.groups.clone();
        groups.insert(key.clone(), levels.clone());
        self.apply_groups(groups)?;
        self.persist(Persist::Levels(key, levels));
        Ok(LevelsResult { count, ignored })
    }

    fn remove_levels(&mut self, key: String) -> Result<bool, String> {
        let mut groups = self.groups.clone();
        if groups.remove(key.trim()).is_none() {
            return Ok(false);
        }
        self.apply_groups(groups)?;
        self.persist(Persist::DropLevels(key.trim().to_string()));
        Ok(true)
    }

    fn create_rule(&mut self, mut spec: RuleSpec) -> Result<CreateResult, String> {
        spec.normalize();
        if let Some(id) = &spec.rule_id {
            if self.rules.contains_key(id) {
                return Ok(CreateResult {
                    rule_id: id.clone(),
                    status: CreateStatus::Exists,
                });
            }
            if self.tombs.contains_key(id) {
                return Ok(CreateResult {
                    rule_id: id.clone(),
                    status: CreateStatus::AlreadyFiredOrCancelled,
                });
            }
        }
        spec.validate(|s| self.known(s))
            .map_err(|e| e.to_string())?;
        if self.rules.len() >= MAX_RULES {
            return Err(format!("rule limit of {MAX_RULES} reached"));
        }
        if self
            .desired_with(&self.interest, &self.levels, Some(&spec.symbol))
            .len()
            > MAX_SYMBOLS
        {
            return Err(format!(
                "symbol would exceed the stream limit of {MAX_SYMBOLS}"
            ));
        }
        let id = spec.rule_id.clone().unwrap_or_else(|| {
            self.next_id += 1;
            format!("r_{}", self.next_id)
        });
        let entry = RuleEntry {
            id: id.clone(),
            expires_ms: now_ms() + spec.ttl_s() * 1000,
            spec,
        };
        self.persist(Persist::Rule(
            id.clone(),
            Box::new(SavedRule {
                spec: entry.spec.clone(),
                expires_ms: entry.expires_ms,
            }),
        ));
        self.insert(entry, "rule created");
        Ok(CreateResult {
            rule_id: id,
            status: CreateStatus::Created,
        })
    }

    fn insert(&mut self, entry: RuleEntry, msg: &str) {
        let (id, sym) = (entry.id.clone(), entry.spec.symbol.clone());
        tracing::info!(rule_id = %id, symbol = %sym, user_id = %entry.spec.owner.user_id, agent = %entry.spec.owner.agent, parent_seq = entry.spec.parent_seq, "{msg}");
        self.rules.insert(id, Arc::new(entry));
        let was_active = self.active.contains(&sym);
        self.sync();
        if was_active {
            self.push_rules(&sym);
        }
    }

    fn restore(&mut self, saved: Saved) {
        let now = now_ms();
        let (n_rules, n_groups) = (saved.rules.len(), saved.levels.len());
        let store = self.store.take();
        if let Err(e) = self.set_interest(saved.interest) {
            tracing::warn!(error = %e, "saved interest not restored");
        }
        self.store = store;
        let groups = saved
            .levels
            .into_iter()
            .map(|(k, l)| (k, self.clean(l).0))
            .filter(|(_, l)| !l.is_empty())
            .collect();
        if let Err(e) = self.apply_groups(groups) {
            tracing::warn!(error = %e, "saved levels not restored");
        }
        for (id, r) in saved.rules {
            if r.expires_ms <= now {
                self.persist(Persist::Drop(id));
                continue;
            }
            if let Some(n) = id.strip_prefix("r_").and_then(|n| n.parse::<u64>().ok()) {
                self.next_id = self.next_id.max(n);
            }
            self.insert(
                RuleEntry {
                    id,
                    spec: r.spec,
                    expires_ms: r.expires_ms,
                },
                "rule restored",
            );
        }
        tracing::info!(
            interest = self.interest.len(),
            level_groups = n_groups,
            rules = self.rules.len(),
            saved_rules = n_rules,
            "state restored from redis"
        );
    }

    fn drop_rule(&mut self, id: &str, why: &str) -> bool {
        let Some(r) = self.rules.remove(id) else {
            return false;
        };
        tracing::info!(rule_id = %id, symbol = %r.spec.symbol, why, "rule removed");
        self.persist(Persist::Drop(id.to_string()));
        self.tombs.insert(id.to_string(), r.expires_ms);
        self.push_rules(&r.spec.symbol);
        self.sync();
        true
    }

    fn expire(&mut self) {
        let now = now_ms();
        self.tombs.retain(|_, exp| *exp > now);
        let expired: Vec<String> = self
            .rules
            .values()
            .filter(|r| r.expires_ms <= now)
            .map(|r| r.id.clone())
            .collect();
        for id in expired {
            self.drop_rule(&id, "expired");
        }
    }

    fn list(&self, f: &RuleFilter) -> Vec<RuleView> {
        let now = now_ms();
        let symbol = f.symbol.as_ref().map(|s| s.trim().to_ascii_uppercase());
        self.rules
            .values()
            .filter(|r| {
                let o: &Owner = &r.spec.owner;
                o.user_id == f.user_id
                    && f.agent.as_ref().is_none_or(|a| *a == o.agent)
                    && symbol.as_ref().is_none_or(|s| *s == r.spec.symbol)
            })
            .map(|r| RuleView {
                rule_id: r.id.clone(),
                symbol: r.spec.symbol.clone(),
                owner: r.spec.owner.clone(),
                when: r.spec.when.clone(),
                action: r.spec.action,
                note: r.spec.note.clone(),
                plan: r.spec.plan.clone(),
                once: r.spec.once(),
                seconds_left: r.expires_ms.saturating_sub(now) / 1000,
                parent_seq: r.spec.parent_seq,
            })
            .collect()
    }
}

pub async fn run(
    mut cmds: mpsc::UnboundedReceiver<Cmd>,
    mut fired: mpsc::UnboundedReceiver<String>,
    router: Router,
    want: watch::Sender<Wanted>,
    rest: mpsc::UnboundedSender<RestJob>,
    (store, saved): Store,
    ct: CancellationToken,
) {
    let mut c = Control {
        known: HashSet::new(),
        interest: BTreeSet::new(),
        groups: LevelGroups::new(),
        levels: BTreeMap::new(),
        rules: BTreeMap::new(),
        tombs: HashMap::new(),
        next_id: 0,
        active: BTreeSet::new(),
        router,
        want,
        rest,
        store,
    };
    c.restore(saved);
    let mut tick = tokio::time::interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = ct.cancelled() => break,
            Some(id) = fired.recv() => { c.drop_rule(&id, "fired"); }
            _ = tick.tick() => c.expire(),
            Some(cmd) = cmds.recv() => match cmd {
                Cmd::SetInterest(s, tx) => { let _ = tx.send(c.set_interest(s)); }
                Cmd::UpdateInterest(add, remove, tx) => { let _ = tx.send(c.update_interest(add, remove)); }
                Cmd::SetLevels(l, tx) => { let _ = tx.send(c.set_levels(l)); }
                Cmd::PutLevels(key, l, tx) => { let _ = tx.send(c.put_levels(key, l)); }
                Cmd::RemoveLevels(key, tx) => { let _ = tx.send(c.remove_levels(key)); }
                Cmd::CreateRule(spec, tx) => { let _ = tx.send(c.create_rule(*spec)); }
                Cmd::ListRules(f, tx) => { let _ = tx.send(c.list(&f)); }
                Cmd::CancelRule(id, user_id, tx) => {
                    let mine = c.rules.get(&id).is_some_and(|r| r.spec.owner.user_id == user_id);
                    let _ = tx.send(mine && c.drop_rule(&id, "cancelled"));
                }
                Cmd::Known(set) => {
                    c.known = set;
                }
                Cmd::Status(tx) => {
                    let _ = tx.send(ControlStatus {
                        interest: c.interest.len(),
                        subscribed: c.active.len(),
                        levels: c.levels.values().map(Vec::len).sum(),
                        rules: c.rules.len(),
                        known_symbols: c.known.len(),
                    });
                }
            }
        }
    }
}
