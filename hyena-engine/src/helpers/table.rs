use ty::{ColumnId, Value};
use ty::fragment::{Fragment, FragmentIter};
use scanner::ScanResult;
use prettytable::Table;
use prettytable::row::Row;
use prettytable::cell::Cell;
use prettytable::format::Alignment;
use term::{Attr, color};
use std::fmt::Display;
use std::collections::HashMap;
use std::iter::{Peekable, Enumerate, once};


macro_rules! table {
    ($fragmap: expr, $header: expr) => {{
        use prettytable::Table;
        use prettytable::row::Row;
        use prettytable::cell::Cell;

        let mut table = Table::new();

        let fm = $fragmap;
        let header = $header;

        if !header.is_empty() {

            let iter = header.iter().map(|h| h.as_ref());

            let header = Row::new(once("rowidx").chain(iter).map(|title| {
                let mut cell = Cell::new(title)
                    .with_style(Attr::ForegroundColor(color::GREEN))
                    .with_style(Attr::Bold);
                cell.align(Alignment::CENTER);
                cell
            }).collect());

            table.add_row(header);
        }

        if !fm.is_empty() {

            let mut keys = fm.get_columns();
            keys.sort_by_key(|&(k, _)| k);

            let mut iters = keys.into_iter().map(|(k, fragiter)| {
                fragiter.peekable()
            })
            .collect::<Vec<_>>();

            let tsiter = fm.get_index();

            let mut riter = RowIter {
                ts_iter: Box::new(tsiter.enumerate()),
                col_iter: iters,
            };

            for (ridx, row) in riter.enumerate() {
                let trow =
                Row::new(once(&Value::from(ridx))
                    .map(|value| {
                        let mut cell = Cell::new(&value.to_string())
                            .with_style(Attr::ForegroundColor(color::BLUE))
                            .with_style(Attr::Bold);
                        cell.align(Alignment::CENTER);
                        cell
                    })
                    .chain(
                        row.iter()
                            .map(|value| Cell::new(&value.to_string()))
                    )
                    .collect());

                table.add_row(trow);
            }
        }

        table
    }};

    ($fragmap: expr) => {
        table!($fragmap, Vec::<&str>::new())
    };
}

pub(crate) struct RowIter<'frag> {
    ts_iter: Box<Enumerate<FragmentIter<'frag>>>,
    col_iter: Vec<Peekable<FragmentIter<'frag>>>,
}

impl<'frag> Iterator for RowIter<'frag> {
    type Item = Vec<Value>;

    fn next(&mut self) -> Option<Self::Item> {

        let &mut Self { ref mut ts_iter, ref mut col_iter } = self;

        ts_iter
            .map(|(rowid, ts)| {
                col_iter.iter_mut()
                    .map(|mut col| {
                        if let Some(&(crowid, _)) = col.peek() {
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

pub(crate) trait ToDisplayTable {
    fn to_display_table<T: Display>(&self, columns: Option<Vec<T>>) -> Table {
        if let Some(columns) = columns {
            table!(self, columns.iter().map(|c| c.to_string()).collect::<Vec<_>>())
        } else {
            table!(self)
        }
    }

    fn get_index(&self) -> FragmentIter;
    fn get_columns(&self) -> Vec<(ColumnId, FragmentIter)>;
    fn is_empty(&self) -> bool;
}

impl ToDisplayTable for HashMap<ColumnId, Option<Fragment>> {
    fn get_index(&self) -> FragmentIter {
        self.get(&0).expect("Main dense ts index column not found").as_ref().unwrap().iter()
    }

    fn get_columns(&self) -> Vec<(ColumnId, FragmentIter)> {
        self.iter().map(|(colid, frag)| (*colid, frag.as_ref().unwrap().iter())).collect()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl ToDisplayTable for HashMap<ColumnId, Fragment> {
    fn get_index(&self) -> FragmentIter {
        self.get(&0).expect("Main dense ts index column not found").iter()
    }

    fn get_columns(&self) -> Vec<(ColumnId, FragmentIter)> {
        self.iter().map(|(colid, frag)| (*colid, frag.iter())).collect()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl ToDisplayTable for Vec<Fragment> {
    fn get_index(&self) -> FragmentIter {
        self.get(0).expect("Main dense ts index column not found").iter()
    }

    fn get_columns(&self) -> Vec<(ColumnId, FragmentIter)> {
        self.iter().enumerate().map(|(colid, frag)| (colid, frag.iter())).collect()
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl ToDisplayTable for ScanResult {
    fn to_display_table<T: Display>(&self, columns: Option<Vec<T>>) -> Table {
        self.data.to_display_table(columns)
    }

    fn get_index(&self) -> FragmentIter {
        self.data.get_index()
    }

    fn get_columns(&self) -> Vec<(ColumnId, FragmentIter)> {
        self.data.get_columns()
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ty::fragment::Fragment;
    use std::collections::HashMap;
    use scanner::ScanResult;


    #[test]
    fn rowiter() {
        let mut data: HashMap<usize, Option<Fragment>> = HashMap::new();

        let ts = Fragment::from((1..20).into_iter().collect::<Vec<u64>>());
        let d1 = Fragment::from((1..20).into_iter().collect::<Vec<u8>>());
        let s1 = Fragment::from(((1..5).into_iter().collect::<Vec<u32>>(),
                                (1..5).into_iter().map(|v| v * 4).collect::<Vec<u32>>()));

        data.insert(0, Some(ts));
        data.insert(1, Some(d1));
        data.insert(2, Some(s1));

        // compile check
        data.to_display_table(Some(vec!["ts", "d1", "s1"]));
    }

    #[test]
    fn rowiter2() {
        let mut data: HashMap<usize, Fragment> = HashMap::new();

        let ts = Fragment::from((1..50).into_iter().collect::<Vec<u64>>());
        let d1 = Fragment::from((1..50).into_iter()
            .enumerate()
            .map(|(idx, v)| (v * idx) as u32 ).collect::<Vec<u32>>());
        let s1 = Fragment::from(((1..5).into_iter().collect::<Vec<u32>>(),
                                (1..5).into_iter().map(|v| v * 4).collect::<Vec<u32>>()));

        data.insert(0, ts);
        data.insert(1, d1);
        data.insert(2, s1);

        // compile check
        data.to_display_table(Some(vec!["ts", "d1", "s1"]));
    }
}
