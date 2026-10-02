#[derive(Debug, Clone)]
pub struct Ring<T> {
    buf: Vec<T>,
    head: usize,
    len: usize,
}

impl<T: Copy + Default> Ring<T> {
    pub fn new(cap: usize) -> Self {
        Self {
            buf: vec![T::default(); cap.max(1)],
            head: 0,
            len: 0,
        }
    }

    pub fn cap(&self) -> usize {
        self.buf.len()
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn clear(&mut self) {
        self.head = 0;
        self.len = 0;
    }

    pub fn push(&mut self, v: T) {
        self.buf[self.head] = v;
        self.head = (self.head + 1) % self.buf.len();
        self.len = (self.len + 1).min(self.buf.len());
    }

    pub fn back(&self, k: usize) -> Option<&T> {
        (k < self.len).then(|| &self.buf[(self.head + self.buf.len() - 1 - k) % self.buf.len()])
    }

    pub fn newest(&self) -> Option<&T> {
        self.back(0)
    }

    pub fn newest_mut(&mut self) -> Option<&mut T> {
        if self.len == 0 {
            return None;
        }
        let i = (self.head + self.buf.len() - 1) % self.buf.len();
        Some(&mut self.buf[i])
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = &T> + '_ {
        (0..self.len).rev().filter_map(move |k| self.back(k))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ring_wraps_and_indexes_from_newest() {
        let mut r = Ring::new(3);
        assert!(r.newest().is_none());
        for v in 1..=5 {
            r.push(v);
        }
        assert_eq!(r.len(), 3);
        assert_eq!(r.back(0), Some(&5));
        assert_eq!(r.back(2), Some(&3));
        assert_eq!(r.back(3), None);
        assert_eq!(r.iter().copied().collect::<Vec<_>>(), vec![3, 4, 5]);
        *r.newest_mut().unwrap() = 9;
        assert_eq!(r.newest(), Some(&9));
    }
}
