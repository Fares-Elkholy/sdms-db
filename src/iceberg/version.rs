// This file will be replaced by the runner

#[derive(Debug, Default, PartialEq, PartialOrd, Ord, Eq, Hash, Clone, Copy)]
pub struct Version {
    inner: u64,
}

impl Version {
    pub fn new(version: u64) -> Self {
        Self { inner: version }
    }

    pub fn is_immediate_predecessor(&self, other: &Version) -> bool {
        self.inner.wrapping_add(1) == other.inner
    }

    pub fn successor(&self) -> Self {
        Self {
            inner: self.inner.wrapping_add(1),
        }
    }
}

impl Into<u64> for Version {
    fn into(self) -> u64 {
        self.inner
    }
}
