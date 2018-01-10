use hyena_common::ty::Timestamp;
use std::ops::Deref;

use super::PartitionId;
use super::partition::Partition;

#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Eq, Ord, Serialize, Deserialize, Hash)]
pub(crate) struct PartitionMeta {
    pub(super) id: PartitionId,

    pub(super) ts_min: Timestamp,
    pub(super) ts_max: Timestamp,
}

impl PartitionMeta {
    #[allow(unused)]
    fn new<TS>(id: PartitionId, ts_min: TS, ts_max: TS) -> PartitionMeta
    where
        Timestamp: From<TS>,
    {
        PartitionMeta {
            id,
            ts_min: ts_min.into(),
            ts_max: ts_max.into(),
        }
    }
}

impl<'a, 'part> From<&'a Partition<'part>> for PartitionMeta {
    fn from(partition: &'a Partition<'part>) -> PartitionMeta {
        let (ts_min, ts_max) = partition.get_ts();

        PartitionMeta {
            id: partition.get_id(),
            ts_min,
            ts_max,
        }
    }
}

impl Deref for PartitionMeta {
    type Target = PartitionId;

    fn deref(&self) -> &Self::Target {
        &self.id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use hyena_test::random::timestamp::{RandomTimestamp, RandomTimestampGen};

    #[test]
    fn deref() {
        let id = Partition::gen_id();
        let ts_min = RandomTimestampGen::random::<u64>();
        let ts_max = RandomTimestampGen::random_from(ts_min);

        let pm = PartitionMeta::new(id, ts_min, ts_max);

        assert_eq!(*pm, id);
    }
}
