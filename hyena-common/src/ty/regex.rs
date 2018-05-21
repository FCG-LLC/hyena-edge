use error::*;
use regex::Regex as RegexImpl;
use std::str::FromStr;
use std::ops::Deref;
use std::fmt::{Display, Formatter, self};
use std::cmp::PartialEq;
use serde::{Serialize, Deserialize, Serializer, Deserializer, de::Error as DeError};


#[derive(Debug, Clone)]
pub struct Regex(RegexImpl);

impl Regex {
    pub fn new(regex: &str) -> Result<Regex> {
        Ok(Regex(RegexImpl::new(regex)?))
    }
}

impl From<RegexImpl> for Regex {
    fn from(source: RegexImpl) -> Regex {
        Regex(source)
    }
}

impl FromStr for Regex {
    type Err = Error;

    fn from_str(regex: &str) -> ::std::result::Result<Regex, Self::Err> {
        Regex::new(regex)
    }
}

impl Deref for Regex {
    type Target = RegexImpl;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for Regex {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl PartialEq for Regex {
    fn eq(&self, other: &Regex) -> bool {
        self.0.as_str() == other.0.as_str()
    }
}

impl Serialize for Regex {
    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
        where S: Serializer
    {
        self.0.as_str().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Regex {
    fn deserialize<D>(deserializer: D) -> ::std::result::Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        let rx = <&str>::deserialize(deserializer)?;

        Ok(Regex::new(rx).map_err(DeError::custom)?)
    }
}
