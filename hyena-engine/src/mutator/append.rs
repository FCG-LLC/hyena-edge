use crate::ty::{SourceId, TimestampFragment};
use super::BlockData;


#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Append {
    pub(crate) ts: TimestampFragment,
    pub(crate) source_id: SourceId,
    pub(crate) data: BlockData,
}

impl Append {
    pub fn new(ts: TimestampFragment, source_id: SourceId, data: BlockData) -> Append {
        Append {
            ts,
            source_id,
            data,
        }
    }

    pub fn len(&self) -> usize {
        // return count of the ts records
        self.ts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ts.is_empty()
    }

    pub fn into_parts(self) -> (TimestampFragment, SourceId, BlockData) {
        let Append { ts, source_id, data } = self;

        (ts, source_id, data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::*;
    use hyena_test::random::timestamp::RandomTimestampGen;
    use rand::{thread_rng, Rng};
    use crate::block::SparseIndex;
    use std::iter::repeat;
    use crate::ty::Fragment;
    use hyena_common::ty::Timestamp;

    #[test]
    fn serialize() {
        let buf = RandomTimestampGen::iter()
            .take(100)
            .collect::<Vec<Timestamp>>();

        let frag = TimestampFragment::from(buf);

        let mut rng = thread_rng();

        let u64frag = Fragment::from(
            repeat(())
                .take(100)
                .map(|_| rng.gen())
                .collect::<Vec<u64>>(),
        );

        let u64sfrag = Fragment::from((
            repeat(())
                .take(100)
                .enumerate()
                .map(|(_idx, _)| rng.gen())
                .collect::<Vec<u64>>(),

            repeat(())
                .take(100)
                .enumerate()
                .map(|(idx, _)| (idx as SparseIndex) * 3)
                .collect::<Vec<SparseIndex>>(),
        ));

        let ap = Append {
            ts: frag,
            source_id: 1,
            data: hashmap! {
                2 => u64frag,
                8 => u64sfrag,
            },
        };

        let tf = tempfile!();

        let f = tf.1;

        carry!(serialize!(file & f, &ap)).unwrap();

        let de = carry!(deserialize!(file & f, Append)).unwrap();

        assert_eq!(&de, &ap);
    }
}
