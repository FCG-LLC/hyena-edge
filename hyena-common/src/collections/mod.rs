//! Custom newtype wrappers over std::collections::HashMap and std::collections::HashSet
//! using FxHash in place of standard hasher (currently SipHash 1-3).
//! While being significantly faster (1-3x), FxHash doesn't provide resistance against hash DoS
//! attacks, but as we're mostly dealing with the data produced internally,
//! this shouldn't be a major concern.

pub mod hash_map;
pub mod hash_set;
pub mod hash;

pub use self::hash::Hasher;

pub use self::hash_map::HashMap;
pub use self::hash_set::HashSet;
