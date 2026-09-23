// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

/// Where in a client's registered memory a transfer should land or read from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionRef {
    /// The client endpoint's fabric address. Sent at handshake, not per transfer.
    pub address: Vec<u8>,
    /// The key a peer presents to access the registered region.
    pub remote_key: u64,
    /// The buffer's virtual address on `FI_MR_VIRT_ADDR` providers like efa, and 0
    /// where addressing is by offset into the region, like tcp.
    pub remote_address: u64,
}

impl RegionRef {
    /// Command arguments a region reference occupies.
    pub const ARG_COUNT: usize = 2;

    /// The arguments a transfer carries: `<rkey> <remote-address>`.
    pub fn to_args(&self) -> [String; Self::ARG_COUNT] {
        [self.remote_key.to_string(), self.remote_address.to_string()]
    }

    /// Parse the two transfer fields
    pub fn from_fields(address: Vec<u8>, fields: &[&[u8]]) -> Result<Self, RegionRefError> {
        let [remote_key, remote_address] = fields else {
            return Err(RegionRefError::Arity);
        };
        Ok(Self {
            address,
            remote_key: parse_ascii(remote_key)?,
            remote_address: parse_ascii(remote_address)?,
        })
    }
}

/// Errors decoding a region reference.
#[derive(Debug, thiserror::Error)]
pub enum RegionRefError {
    /// Wrong number of fields.
    #[error("expected: rkey remote-address")]
    Arity,
    /// A hex field did not decode.
    #[error("invalid hex in address")]
    Hex,
    /// An integer field did not parse.
    #[error("invalid integer in region_ref")]
    Integer,
}

/// Parse a decimal integer from an ASCII argument.
pub(crate) fn parse_ascii<T: std::str::FromStr>(raw: &[u8]) -> Result<T, RegionRefError> {
    std::str::from_utf8(raw)
        .ok()
        .and_then(|text| text.parse().ok())
        .ok_or(RegionRefError::Integer)
}

/// Hex-encode opaque bytes for a RESP argument or reply.
pub fn encode_hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(DIGITS[usize::from(byte >> 4)] as char);
        output.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    output
}

/// Decode an [`encode_hex`] string back to bytes.
pub fn decode_hex(text: &[u8]) -> Result<Vec<u8>, RegionRefError> {
    if !text.len().is_multiple_of(2) {
        return Err(RegionRefError::Hex);
    }
    text.chunks_exact(2)
        .map(|pair| {
            let high = (pair[0] as char).to_digit(16).ok_or(RegionRefError::Hex)?;
            let low = (pair[1] as char).to_digit(16).ok_or(RegionRefError::Hex)?;
            Ok((high << 4 | low) as u8)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{RegionRef, decode_hex, encode_hex};

    #[test]
    fn round_trips_through_args() {
        let region_ref = RegionRef {
            address: vec![0x01, 0x00, 0x7f, 0xff, 0xab],
            remote_key: 42,
            remote_address: 0x7f00_1234_5678,
        };
        let args = region_ref.to_args();
        let fields: Vec<&[u8]> = args.iter().map(|arg| arg.as_bytes()).collect();
        assert_eq!(
            RegionRef::from_fields(region_ref.address.clone(), &fields).unwrap(),
            region_ref
        );
    }

    #[test]
    fn rejects_a_short_region_ref() {
        let fields: Vec<&[u8]> = vec![b"1"];
        assert!(RegionRef::from_fields(Vec::new(), &fields).is_err());
    }

    #[test]
    fn hex_round_trips() {
        let bytes = vec![0x00, 0x0f, 0xf0, 0xff, 0xab, 0xcd];
        assert_eq!(encode_hex(&bytes), "000ff0ffabcd");
        assert_eq!(decode_hex(b"000ff0ffabcd").unwrap(), bytes);
    }

    #[test]
    fn rejects_odd_length_and_non_hex() {
        assert!(decode_hex(b"abc").is_err());
        assert!(decode_hex(b"zz").is_err());
    }

    #[test]
    fn rejects_a_non_numeric_rkey() {
        let fields: Vec<&[u8]> = vec![b"not-a-number", b"0"];
        assert!(RegionRef::from_fields(Vec::new(), &fields).is_err());
    }
}
