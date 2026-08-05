//! Partition identity and canonical UUID spelling.

use std::fmt;
use std::str::FromStr;

use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PartitionId([u8; 16]);

impl PartitionId {
    #[cfg(test)]
    #[must_use]
    pub(crate) const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl TryFrom<[u8; 16]> for PartitionId {
    type Error = PartitionIdError;

    fn try_from(bytes: [u8; 16]) -> Result<Self, Self::Error> {
        if bytes == [0; 16] {
            return Err(PartitionIdError::Nil);
        }
        Ok(Self(bytes))
    }
}

impl FromStr for PartitionId {
    type Err = PartitionIdError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.len() != 36 {
            return Err(PartitionIdError::Malformed);
        }
        let mut octets = [0u8; 16];
        let mut octet_target = octets.iter_mut();
        let mut high_nibble = None;
        for (position, character) in input.bytes().enumerate() {
            if matches!(position, 8 | 13 | 18 | 23) {
                if character != b'-' {
                    return Err(PartitionIdError::Malformed);
                }
                continue;
            }
            let nibble = hex_nibble(character).ok_or(PartitionIdError::Malformed)?;
            match high_nibble.take() {
                None => high_nibble = Some(nibble),
                Some(high) => {
                    let octet = high
                        .checked_mul(16)
                        .and_then(|shifted| shifted.checked_add(nibble))
                        .expect("two hexadecimal nibbles always fit in one octet");
                    *octet_target
                        .next()
                        .expect("a canonical UUID contains exactly sixteen octets") = octet;
                }
            }
        }
        if high_nibble.is_some() || octet_target.next().is_some() {
            return Err(PartitionIdError::Malformed);
        }
        Self::try_from(octets)
    }
}

impl fmt::Display for PartitionId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for (index, octet) in self.0.iter().enumerate() {
            if matches!(index, 4 | 6 | 8 | 10) {
                formatter.write_str("-")?;
            }
            write!(formatter, "{octet:02x}")?;
        }
        Ok(())
    }
}

impl Serialize for PartitionId {
    fn serialize<Serializer: serde::Serializer>(
        &self,
        serializer: Serializer,
    ) -> Result<Serializer::Ok, Serializer::Error> {
        self.0.serialize(serializer)
    }
}

const fn hex_nibble(character: u8) -> Option<u8> {
    match character {
        b'0'..=b'9' => character.checked_sub(b'0'),
        b'a'..=b'f' => match character.checked_sub(b'a') {
            Some(offset) => offset.checked_add(10),
            None => None,
        },
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionIdError {
    Malformed,
    Nil,
}

impl fmt::Display for PartitionIdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed => {
                formatter.write_str("partition id is not a lowercase hyphenated UUID")
            }
            Self::Nil => formatter.write_str("partition id is nil"),
        }
    }
}

impl std::error::Error for PartitionIdError {}
