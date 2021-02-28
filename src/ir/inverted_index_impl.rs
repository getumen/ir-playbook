use std::collections::HashMap;

use crate::ir::inverted_index;
use crate::ir::inverted_index::Position;

struct BinarySearchInvertedIndex {
    index: HashMap<String, Vec<inverted_index::Position>>
}

impl inverted_index::InvertedIndex for BinarySearchInvertedIndex {
    fn first<T: AsRef<str>>(&self, term: T) -> Position {
        unimplemented!()
    }

    fn last<T: AsRef<str>>(&self, term: T) -> Position {
        unimplemented!()
    }

    fn next<T: AsRef<str>>(&self, term: T, current: &Position) -> Position {
        let posting_list = match self.index.get(term.as_ref()) {
            Some(p) => p,
            None => return inverted_index::INFINITY,
        };
        if posting_list.len() == 0 {
            return inverted_index::INFINITY;
        }
        if posting_list.last().expect("never happen") <= current {
            return inverted_index::INFINITY;
        }
        let first = posting_list.first().expect("never happen");
        if first > current {
            return *first;
        }
        return match posting_list.binary_search(current) {
            Ok(p) => posting_list[p],
            Err(p) => posting_list[p],
        };
    }

    fn prev<T: AsRef<str>>(&self, term: T, current: &Position) -> Position {
        unimplemented!()
    }
}

#[cfg(test)]
mod test {
    use std::convert::TryFrom;

    use crate::ir::inverted_index::InvertedIndex;

    use super::*;

    fn create_index() -> BinarySearchInvertedIndex {
        let mut map: HashMap<String, Vec<Position>> = HashMap::new();
        let posting_list: Vec<Position> = vec![1, 3, 9, 27]
            .iter()
            .map(|x| Position::try_from(*x).unwrap())
            .collect();
        map.insert(String::from("a"), posting_list);
        BinarySearchInvertedIndex {
            index: map,
        }
    }

    #[test]
    fn test_next() {
        let index = create_index();
        let test_cases = vec![
            (inverted_index::NEG_INFINITY, Position::new(1)),
            (Position::new(2), Position::new(3)),
            (Position::new(4), Position::new(9)),
            (Position::new(10), Position::new(27)),
            (inverted_index::INFINITY, inverted_index::INFINITY),
        ];
        for (input, expected) in test_cases {
            assert_eq!(expected, index.next("a", &input));
        }
    }
}