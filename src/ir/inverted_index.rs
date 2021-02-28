use std::convert::TryFrom;
use std::num::TryFromIntError;
use std::ops::Add;
use std::ops::Sub;

#[derive(Clone, Copy, Debug, PartialOrd, PartialEq, Ord, Eq, Hash)]
pub struct Position(usize);

pub(crate) const INFINITY: Position = Position::new(usize::MAX);
pub(crate) const NEG_INFINITY: Position = Position::new(0);

impl Position {
    pub const fn new(v: usize) -> Position {
        Position(v)
    }

    pub fn is_inf(&self) -> bool {
        return self.0 == INFINITY.0;
    }

    pub fn is_neg_inf(&self) -> bool {
        return self.0 == NEG_INFINITY.0;
    }
}

impl TryFrom<i32> for Position {
    type Error = TryFromIntError;

    fn try_from(from: i32) -> Result<Self, TryFromIntError> {
        match usize::try_from(from) {
            Ok(v) => Ok(Position::new(v)),
            Err(e) => Err(e),
        }
    }
}

impl Add<Position> for Position {
    type Output = Position;

    fn add(self, rhs: Position) -> Self::Output {
        match self.0.checked_add(rhs.0) {
            Some(v) => Self::Output { 0: v },
            None => INFINITY,
        }
    }
}

impl Sub<Position> for Position {
    type Output = Position;

    fn sub(self, rhs: Position) -> Self::Output {
        match self.0.checked_sub(rhs.0) {
            Some(v) => Self::Output { 0: v },
            None => NEG_INFINITY,
        }
    }
}

pub trait InvertedIndex {
    fn first<T: AsRef<str>>(&self, term: T) -> Position;
    fn last<T: AsRef<str>>(&self, term: T) -> Position;
    fn next<T: AsRef<str>>(&self, term: T, current: &Position) -> Position;
    fn prev<T: AsRef<str>>(&self, term: T, current: &Position) -> Position;
}

