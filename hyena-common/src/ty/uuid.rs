use uuid::Uuid as UuidImpl;

use std::fmt::{Display, Formatter, Result as FmtResult};

/// Byte buffer for use by the `Uuid` type
pub type UuidBytes = [u8; BUFFER_SIZE];
const BUFFER_SIZE: usize = 16;

use serde::ser::{Serialize, SerializeTuple, Serializer};
use serde::de::{Deserialize, Deserializer};

/// Uuid type for use in Hyena
///
/// Currently this is a wrapper for `uuid::Uuid` type
///
/// # Examples
///
/// ```rust
///
/// extern crate hyena_common;
///
/// use hyena_common::ty::Uuid;
///
/// fn main() {
///
///     let uuid = Uuid::new();
///
///     println!("{}", uuid);
/// }
///
/// ```
/// ```rust
///
/// extern crate hyena_common;
///
/// use hyena_common::ty::{Uuid, UuidBytes};
///
/// fn main() {
///
///     let buffer: UuidBytes = [4, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87];
///     let uuid = Uuid::from(buffer);
///     let uuid_buffer: UuidBytes = uuid.into();
///
///     assert_eq!(buffer, uuid_buffer);
/// }
///
/// ```
#[derive(Copy, Clone, PartialEq, PartialOrd, Ord, Debug, Hash, Eq, Default)]
pub struct Uuid(UuidImpl);

impl Uuid {
    /// Create new random Uuid value
    ///
    /// It's not guarantted which uuid variant would be used
    /// but for now it's internally v4
    pub fn new() -> Self {
        Uuid(UuidImpl::new_v4())
    }
}

impl From<UuidBytes> for Uuid {
    fn from(buffer: UuidBytes) -> Uuid {
        Uuid(UuidImpl::from_bytes(&buffer[..]).expect("unable to create Uuid from given buffer"))
    }
}

impl<'buf> From<&'buf UuidBytes> for Uuid {
    fn from(buffer: &UuidBytes) -> Uuid {
        Uuid(UuidImpl::from_bytes(buffer).unwrap())
    }
}

impl<'uuid> From<&'uuid Uuid> for UuidBytes {
    fn from(uuid: &Uuid) -> UuidBytes {
        *uuid.0.as_bytes()
    }
}

impl From<Uuid> for UuidBytes {
    fn from(uuid: Uuid) -> UuidBytes {
        *uuid.0.as_bytes()
    }
}

impl From<UuidImpl> for Uuid {
    fn from(uuid: UuidImpl) -> Uuid {
        Uuid(uuid)
    }
}

impl<'uuid> From<&'uuid UuidImpl> for Uuid {
    fn from(uuid: &UuidImpl) -> Uuid {
        Uuid::from(*uuid)
    }
}

impl From<Uuid> for UuidImpl {
    fn from(uuid: Uuid) -> UuidImpl {
        uuid.0
    }
}

impl AsRef<UuidImpl> for Uuid {
    fn as_ref(&self) -> &UuidImpl {
        &self.0
    }
}

impl Display for Uuid {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        <UuidImpl as Display>::fmt(&self.0, f)
    }
}

// uuid::Uuid serializes to String and we want a simple byte slice

impl Serialize for Uuid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // serialize as tuple and not using seemingly preferred serialize_bytes
        // as bincode's implementation of serialize_bytes encodes the slice's length
        // while we want just 16 bytes

        let mut ser = serializer.serialize_tuple(BUFFER_SIZE)?;

        for byte in self.0.as_bytes() {
            ser.serialize_element(byte)?;
        }

        ser.end()
    }
}

impl<'de> Deserialize<'de> for Uuid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let buffer = UuidBytes::deserialize(deserializer)?;

        Ok(Uuid::from(&buffer))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEST_BYTES: UuidBytes = [4, 54, 67, 12, 43, 2, 98, 76, 32, 50, 87, 5, 1, 33, 43, 87];

    #[test]
    fn to_from() {
        let orig_uuid = UuidImpl::new_v4();

        let this_uuid: Uuid = orig_uuid.into();
        let back_orig: UuidImpl = this_uuid.into();

        assert_eq!(orig_uuid, back_orig);
    }

    #[test]
    fn create() {
        let wrapped_uuid = UuidImpl::from_bytes(&TEST_BYTES).unwrap();

        let uuid = Uuid::from(&TEST_BYTES);

        assert_eq!(uuid.0, wrapped_uuid);
    }

    #[test]
    fn serialize() {
        use crate::serde_utils::serialize;

        let uuid = Uuid::from(&TEST_BYTES);

        let serialized = serialize(&uuid).expect("serialization failed");

        assert_eq!(&serialized[..], &TEST_BYTES);
    }

    #[test]
    fn deserialize() {
        use crate::serde_utils::deserialize;

        let uuid = Uuid::from(&TEST_BYTES);

        let deserialized = deserialize::<Uuid>(&TEST_BYTES).expect("deserialization failed");

        assert_eq!(deserialized, uuid);
    }
}
