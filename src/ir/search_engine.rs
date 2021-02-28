use super::inverted_index;

pub struct SearchEngine<Index: inverted_index::InvertedIndex> {
    index: Box<Index>
}


struct Range {
    start: inverted_index::Position,
    end: inverted_index::Position,
}

impl Range {
    pub const fn new(start: inverted_index::Position, end: inverted_index::Position) -> Range {
        Range { start, end }
    }
}

impl<Index> SearchEngine<Index> where Index: inverted_index::InvertedIndex {
    fn next_phrase<T: AsRef<str>>(&self, terms: &[T], position: &inverted_index::Position) -> Range {
        let mut v = *position;
        for term in terms {
            v = self.index.next(term, &v);
        }
        if v.is_inf() {
            return Range { start: inverted_index::INFINITY, end: inverted_index::INFINITY };
        }

        let mut u = v;
        // TODO: remove this
        for term in terms.iter().rev().skip(1) {
            u = self.index.prev(term, &u);
        }
        return if v - u == inverted_index::Position::new(terms.len() - 1) {
            Range { start: u, end: v }
        } else {
            self.next_phrase(terms, &u)
        };
    }
}
