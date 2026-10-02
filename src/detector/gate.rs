use super::consts::*;
use super::{Dir, Metrics};

#[derive(Debug, Clone, Copy, Default)]
pub struct DirGate {
    pub fired: bool,
    pub armed: bool,
    pub cooldown_until: u64,
    pub ref_px: f64,
    pub event_px: f64,
    pub z: f64,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct Gate {
    pub up: DirGate,
    pub down: DirGate,
}

impl Gate {
    pub fn get(&self, d: Dir) -> &DirGate {
        match d {
            Dir::Up => &self.up,
            Dir::Down => &self.down,
        }
    }

    fn get_mut(&mut self, d: Dir) -> &mut DirGate {
        match d {
            Dir::Up => &mut self.up,
            Dir::Down => &mut self.down,
        }
    }

    pub fn observe(&mut self, m: &Metrics) {
        for d in [Dir::Up, Dir::Down] {
            let peak =
                m.w.iter()
                    .filter(|w| w.ok)
                    .map(|w| w.z * d.sign())
                    .fold(0.0, f64::max);
            if peak < REARM_Z {
                self.get_mut(d).armed = true;
            }
        }
    }

    pub fn allow(&self, d: Dir, now_ms: u64, px: f64) -> bool {
        let g = self.get(d);
        if !g.fired {
            return true;
        }
        let last = (g.event_px - g.ref_px) * d.sign();
        let extended = last > 0.0 && (px - g.event_px) * d.sign() >= EXTEND_X * last;
        (now_ms >= g.cooldown_until && g.armed) || extended
    }

    pub fn record(&mut self, d: Dir, now_ms: u64, px: f64, ref_px: f64, z: f64) {
        let g = self.get(d);
        let ref_px = if g.fired && now_ms < g.cooldown_until {
            g.ref_px
        } else {
            ref_px
        };
        *self.get_mut(d) = DirGate {
            fired: true,
            armed: false,
            cooldown_until: now_ms + COOLDOWN_MS,
            ref_px,
            event_px: px,
            z,
        };
    }

    pub fn cooldown_until(&self, d: Dir, now_ms: u64) -> Option<u64> {
        let g = self.get(d);
        (g.fired && g.cooldown_until > now_ms).then_some(g.cooldown_until)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::detector::Window;

    fn m(z: f64) -> Metrics {
        let w = Window {
            ok: true,
            z,
            ..Default::default()
        };
        Metrics {
            w: [w; 3],
            ..Default::default()
        }
    }

    #[test]
    fn first_event_passes_then_cooldown_and_hysteresis() {
        let mut g = Gate::default();
        assert!(g.allow(Dir::Up, 0, 102.0));
        g.record(Dir::Up, 0, 102.0, 100.0, 5.0);
        assert!(!g.allow(Dir::Up, 60_000, 102.5));
        assert!(g.allow(Dir::Down, 60_000, 99.0));
        assert!(!g.allow(Dir::Up, 60_000, 104.9));
        assert!(
            g.allow(Dir::Up, 60_000, 105.0),
            "moving 1.5x the last move past the last event re-fires inside cooldown"
        );
        g.record(Dir::Up, 60_000, 105.0, 104.0, 6.0);
        assert_eq!(
            g.up.ref_px, 100.0,
            "reference stays fixed inside the cooldown episode"
        );
        assert!(!g.allow(Dir::Up, 120_000, 112.0));
        assert!(g.allow(Dir::Up, 120_000, 112.5));
        let after = 60_000 + COOLDOWN_MS;
        g.observe(&m(3.0));
        assert!(
            !g.allow(Dir::Up, after, 102.0),
            "not re-armed while |z| stays above 1.5"
        );
        g.observe(&m(1.0));
        assert!(g.allow(Dir::Up, after, 102.0));
        assert_eq!(g.cooldown_until(Dir::Up, 1), Some(after));
        assert_eq!(g.cooldown_until(Dir::Down, 1), None);
    }
}
