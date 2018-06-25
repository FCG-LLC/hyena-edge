use hyena_common::ty::Value;
use ty::{ColumnId, fragment::{Fragment, FragmentIter, FragmentRef}};
use scanner::ScanResult;
use mutator::Append;

use prettytable::Table;
use prettytable::format::Alignment;
use std::fmt::Display;
use hyena_common::collections::HashMap;
use std::iter::{once, Enumerate, Peekable};
use std::ops::Deref;


macro_rules! table {
    ($fragmap: expr, $header: expr, $offset: expr) => {{
        use prettytable::Table;
        use prettytable::row::Row;
        use prettytable::cell::Cell;
        use term::{color, Attr};


        let mut table = Table::new();

        let fm = $fragmap;
        let header = $header;

        if !header.is_empty() {
            let iter = header.iter().map(|h| h.as_ref());

            let header = Row::new(
                once("rowidx")
                    .chain(iter)
                    .map(|title| {
                        let mut cell = Cell::new(title)
                            .with_style(Attr::ForegroundColor(color::GREEN))
                            .with_style(Attr::Bold);
                        cell.align(Alignment::CENTER);
                        cell
                    })
                    .collect(),
            );

            table.add_row(header);
        }

        if !fm.is_empty() {
            let mut keys = fm.get_columns();
            keys.sort_by_key(|&(k, _)| k);

            let sparse_map = keys.iter()
                .map(|&(colid, _)| fm.is_sparse(colid))
                .collect::<Vec<_>>();

            let iters = keys.into_iter()
                .map(|(_, fragiter)| fragiter.peekable())
                .collect::<Vec<_>>();

            let tsiter = fm.get_index();

            let riter = RowIter {
                ts_iter: Box::new(tsiter.enumerate()),
                col_iter: iters,
                sparse_map,
                offset: $offset,
            };

            for (ridx, row) in riter.enumerate() {
                let trow = Row::new(
                    once(&Value::from(ridx))
                        .map(|value| {
                            let mut cell = Cell::new(&value.to_string())
                                .with_style(Attr::ForegroundColor(color::BLUE))
                                .with_style(Attr::Bold);
                            cell.align(Alignment::CENTER);
                            cell
                        })
                        .chain(row.iter().map(|value| Cell::new(&value.to_string())))
                        .collect(),
                );

                table.add_row(trow);
            }
        }

        table
    }};

    ($fragmap: expr, $header: expr) => {
        table!($fragmap, $header, 0)
    };

    ($fragmap: expr) => {
        table!($fragmap, Vec::<&str>::new())
    };
}

pub struct RowIter<'frag> {
    ts_iter: Box<Enumerate<FragmentIter<'frag>>>,
    col_iter: Vec<Peekable<FragmentIter<'frag>>>,
    sparse_map: Vec<bool>,
    offset: usize,
}

impl<'frag> Iterator for RowIter<'frag> {
    type Item = Vec<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        let &mut Self {
            ref mut ts_iter,
            ref mut col_iter,
            ref sparse_map,
            offset,
        } = self;

        ts_iter
            .map(|(rowid, _)| {
                col_iter
                    .iter_mut()
                    .zip(sparse_map)
                    .map(|(col, is_sparse)| {
                        if let Some(&(crowid, _)) = col.peek() {
                            let crowid = if *is_sparse {
                                crowid.saturating_sub(offset)
                            } else {
                                crowid
                            };

                            if crowid == rowid {
                                let (_, cval) = col.next().unwrap();
                                cval
                            } else {
                                Value::Null
                            }
                        } else {
                            Value::Null
                        }
                    })
                    .collect()
            })
            .next()
    }
}

pub trait DebugTable {
    fn to_table(&self) -> Table {
        self.to_table_with_sparse_offset(None::<Vec<&'static str>>, 0)
    }

    fn to_table_with_columns<T: Display>(&self, columns: Vec<T>) -> Table {
        self.to_table_with_sparse_offset(Some(columns), 0)
    }

    fn to_table_with_sparse_offset<T: Display>(&self, columns: Option<Vec<T>>, offset: usize)
    -> Table {
        if let Some(columns) = columns {
            table!(
                self,
                columns.iter().map(|c| c.to_string()).collect::<Vec<_>>(),
                offset
            )
        } else {
            table!(self, Vec::<&str>::new(), offset)
        }
    }

    fn get_index(&self) -> FragmentIter;
    fn get_columns(&self) -> Vec<(ColumnId, FragmentIter)>;
    fn is_empty(&self) -> bool;
    fn is_sparse(&self, column: ColumnId) -> bool;
}

impl DebugTable for HashMap<ColumnId, Option<Fragment>> {
    fn get_index(&self) -> FragmentIter {
        self.get(&0)
            .expect("Main dense ts index column not found")
            .as_ref()
            .unwrap()
            .iter()
    }

    fn get_columns(&self) -> Vec<(ColumnId, FragmentIter)> {
        self.iter()
            .map(|(colid, frag)| (*colid, frag.as_ref().unwrap().iter()))
            .collect()
    }

    fn is_empty(&self) -> bool {
        (**self).is_empty()
    }

    fn is_sparse(&self, column: ColumnId) -> bool {
        self.get(&column).unwrap().as_ref().unwrap().is_sparse()
    }
}

impl DebugTable for HashMap<ColumnId, Fragment> {
    fn get_index(&self) -> FragmentIter {
        self.get(&0)
            .expect("Main dense ts index column not found")
            .iter()
    }

    fn get_columns(&self) -> Vec<(ColumnId, FragmentIter)> {
        self.iter()
            .map(|(colid, frag)| (*colid, frag.iter()))
            .collect()
    }

    fn is_empty(&self) -> bool {
        (**self).is_empty()
    }

    fn is_sparse(&self, column: ColumnId) -> bool {
        self.get(&column).unwrap().is_sparse()
    }
}

impl DebugTable for Vec<Fragment> {
    fn get_index(&self) -> FragmentIter {
        self.get(0)
            .expect("Main dense ts index column not found")
            .iter()
    }

    fn get_columns(&self) -> Vec<(ColumnId, FragmentIter)> {
        self.iter()
            .enumerate()
            .map(|(colid, frag)| (colid, frag.iter()))
            .collect()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn is_sparse(&self, column: ColumnId) -> bool {
        self.get(column).unwrap().is_sparse()
    }
}

impl DebugTable for ScanResult {
    fn to_table_with_sparse_offset<T: Display>(&self, columns: Option<Vec<T>>, offset: usize)
    -> Table {
        self.deref().to_table_with_sparse_offset(columns, offset)
    }

    fn get_index(&self) -> FragmentIter {
        self.deref().get_index()
    }

    fn get_columns(&self) -> Vec<(ColumnId, FragmentIter)> {
        self.deref().get_columns()
    }

    fn is_empty(&self) -> bool {
        self.deref().is_empty()
    }

    fn is_sparse(&self, column: ColumnId) -> bool {
        self.deref().is_sparse(column)
    }
}

impl<'frag> DebugTable for HashMap<ColumnId, FragmentRef<'frag>> {
    fn get_index(&self) -> FragmentIter {
        self.get(&0)
            .expect("Main dense ts index column not found")
            .iter()
    }

    fn get_columns(&self) -> Vec<(ColumnId, FragmentIter)> {
        self.iter()
            .map(|(colid, frag)| (*colid, frag.iter()))
            .collect()
    }

    fn is_empty(&self) -> bool {
        (**self).is_empty()
    }

    fn is_sparse(&self, column: ColumnId) -> bool {
        self.get(&column).unwrap().is_sparse()
    }
}

impl DebugTable for Append {
    fn get_index(&self) -> FragmentIter {
        FragmentRef::from(&self.ts).iter()
    }

    fn get_columns(&self) -> Vec<(ColumnId, FragmentIter)> {
        ::std::iter::once((0, FragmentRef::from(&self.ts).iter()))
            .chain(self.data.iter().map(|(colid, frag)| (*colid, frag.iter())))
            .collect()
    }

    fn is_empty(&self) -> bool {
        self.ts.is_empty()
    }

    fn is_sparse(&self, column: ColumnId) -> bool {
        if column == 0 {
            false
        } else {
            self.data.get(&column).unwrap().is_sparse()
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use ty::fragment::Fragment;
    use hyena_common::collections::HashMap;

    #[test]
    fn rowiter() {
        let mut data: HashMap<usize, Option<Fragment>> = HashMap::new();

        let ts = Fragment::from((1..20).into_iter().collect::<Vec<u64>>());
        let d1 = Fragment::from((1..20).into_iter().collect::<Vec<u8>>());
        let s1 = Fragment::from((
            (1..5).into_iter().collect::<Vec<u32>>(),
            (1..5).into_iter().map(|v| v * 4).collect::<Vec<u32>>(),
        ));

        data.insert(0, Some(ts));
        data.insert(1, Some(d1));
        data.insert(2, Some(s1));

        // compile check
        data.to_table_with_sparse_offset(Some(vec!["ts", "d1", "s1"]), 0);
    }

    #[test]
    fn rowiter2() {
        let mut data: HashMap<usize, Fragment> = HashMap::new();

        let ts = Fragment::from((1..50).into_iter().collect::<Vec<u64>>());
        let d1 = Fragment::from(
            (1..50)
                .into_iter()
                .enumerate()
                .map(|(idx, v)| (v * idx) as u32)
                .collect::<Vec<u32>>(),
        );
        let s1 = Fragment::from((
            (1..5).into_iter().collect::<Vec<u32>>(),
            (1..5).into_iter().map(|v| v * 4).collect::<Vec<u32>>(),
        ));

        data.insert(0, ts);
        data.insert(1, d1);
        data.insert(2, s1);

        // compile check
        data.to_table_with_sparse_offset(Some(vec!["ts", "d1", "s1"]), 0);
    }

    #[test]
    fn rowiter3() {
        let mut data: HashMap<usize, Fragment> = HashMap::new();

        let ts = Fragment::from((0..50).into_iter().collect::<Vec<u64>>());
        let s1 = Fragment::from((
            vec![0_u32, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12],
            vec![0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48],
        ));

        let s2 = Fragment::from((
            vec![0_u8, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
            vec![2, 6, 10, 14, 18, 22, 26, 30, 34, 38, 42, 46],
        ));

        data.insert(0, ts);
        data.insert(2, s1);
        data.insert(3, s2);

        // compile check
        data.to_table_with_sparse_offset(Some(vec!["ts", "s1", "s2"]), 0);
    }
}
