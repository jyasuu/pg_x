use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct Lsn(pub u64);

impl Lsn {
    pub const ZERO: Lsn = Lsn(0);

    pub fn parse(s: &str) -> anyhow::Result<Lsn> {
        let (hi_str, lo_str) = s
            .split_once('/')
            .ok_or_else(|| anyhow::anyhow!("invalid LSN '{}': missing '/'", s))?;
        let hi = u64::from_str_radix(hi_str, 16)
            .map_err(|_| anyhow::anyhow!("invalid LSN high part '{}' in '{}'", hi_str, s))?;
        let lo = u64::from_str_radix(lo_str, 16)
            .map_err(|_| anyhow::anyhow!("invalid LSN low part '{}' in '{}'", lo_str, s))?;
        Ok(Lsn((hi << 32) | lo))
    }

    #[inline]
    pub fn is_zero(self) -> bool {
        self.0 == 0
    }
    #[inline]
    pub fn as_u64(self) -> u64 {
        self.0
    }
    #[inline]
    pub fn from_u64(v: u64) -> Self {
        Lsn(v)
    }
}

impl fmt::Display for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{:X}/{:X}",
            (self.0 >> 32) as u32,
            (self.0 & 0xFFFF_FFFF) as u32
        )
    }
}

impl FromStr for Lsn {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Lsn::parse(s)
    }
}

impl From<u64> for Lsn {
    fn from(v: u64) -> Self {
        Lsn(v)
    }
}
impl From<Lsn> for u64 {
    fn from(l: Lsn) -> Self {
        l.0
    }
}
