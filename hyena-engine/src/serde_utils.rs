use error::*;
use bincode;


pub(crate) mod ser {
    use super::*;
    use serde::Serialize;
    use std::io::Write;

    #[allow(unused)]
    pub(crate) fn serialize<T>(value: &T) -> Result<Vec<u8>>
    where
        T: ?Sized + Serialize,
    {
        bincode::serialize(value, bincode::Infinite).chain_err(|| "Serialization failed")
    }

    pub(crate) fn serialize_into<T, W>(value: &T, writer: &mut W) -> Result<()>
    where
        T: ?Sized + Serialize,
        W: ?Sized + Write,
    {
        bincode::serialize_into(writer, value, bincode::Infinite)
            .chain_err(|| "Serialization failed")
    }
}

pub(crate) mod de {
    use super::*;
    use std::io::Read;
    use serde::Deserialize;

    #[allow(unused)]
    pub(crate) fn deserialize<'de, T>(data: &'de [u8]) -> Result<T>
    where
        T: Deserialize<'de>,
    {
        bincode::deserialize(data).chain_err(|| "Deserialization failed")
    }

    pub(crate) fn deserialize_from<T, R>(reader: &mut R) -> Result<T>
    where
        for<'de> T: Deserialize<'de>,
        R: Read,
    {
        bincode::deserialize_from(reader, bincode::Infinite).chain_err(|| "Deserialization failed")
    }
}

macro_rules! deserialize {
    (file $name: expr, $T: ty) => {{
        use serde_utils::de::deserialize_from;
        use std::fs::File;

        let mut file = File::open($name)
            .chain_err(|| "Failed to open file for deserialization")?;

        deserialize_from::<$T, _>(&mut file)
    }};

    (file $name: expr) => {{
        use serde_utils::de::deserialize_from;
        use std::fs::File;

        let mut file = File::open($name)
            .chain_err(|| "Failed to open file for deserialization")?;

        deserialize_from(&mut file)
    }};

    (buf $buf: expr, $T: ty) => {{
        use serde_utils::de::deserialize;

        let mut buf = $buf;

        deserialize::<$T, _>(&mut buf)
    }};

    (buf $buf: expr) => {{
        use serde_utils::de::deserialize;

        let mut buf = $buf;

        deserialize(&mut buf)
    }};

}

macro_rules! serialize {
    (file $name: expr, $value: expr) => {{
        use serde_utils::ser::serialize_into;
        use std::fs::File;

        let mut file = File::create($name)
            .chain_err(|| "Failed to open file {:?} for serialization")?;

        let value = $value;

        serialize_into(&value, &mut file)
    }};

    (buf $value: expr) => {{
        use serde_utils::ser::serialize;

        let value = $value;

        serialize(&value)
    }};

}
