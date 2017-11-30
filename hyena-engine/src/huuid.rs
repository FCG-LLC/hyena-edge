use uuid::Uuid as OrigUuid;

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct Uuid {
    hi: i64,
    lo: i64,
}

impl Uuid {
    pub fn new(hi: i64, lo: i64) -> Self {
        Uuid { hi: hi, lo: lo }
    }
}

impl From<OrigUuid> for Uuid {
    fn from(uuid: OrigUuid) -> Uuid {
        use byteorder::{NativeEndian, ReadBytesExt};

        let bytes = uuid.as_bytes();
        let mut lower = &bytes[0..8];
        let mut higher = &bytes[8..16];

        Uuid {
            hi: higher.read_i64::<NativeEndian>().unwrap(),
            lo: lower.read_i64::<NativeEndian>().unwrap(),
        }
    }
}

impl From<Uuid> for OrigUuid {
    fn from(uuid: Uuid) -> OrigUuid {
        use byteorder::{NativeEndian, WriteBytesExt};

        let mut buf: Vec<u8> = vec![];

        buf.write_i64::<NativeEndian>(uuid.lo).unwrap();
        buf.write_i64::<NativeEndian>(uuid.hi).unwrap();

        OrigUuid::from_bytes(buf.as_slice()).unwrap()
    }
}

mod test {

    #[test]
    fn to_from() {
        use super::*;
        use std;

        let uuid = OrigUuid::new_v4();
        let u: Uuid = std::convert::From::from(uuid);
        let to_from: OrigUuid = std::convert::From::from(u);

        assert_eq!(uuid, to_from);
    }
}
