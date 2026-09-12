// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! The `DMA.*` command vocabulary, independent of any RESP client.

use crate::error::DmaError;

/// Description of a transfer's result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransferReceipt {
    /// Byte length transferred.
    pub bytes_written: usize,
    /// CRC-32c, when requested.
    pub checksum: Option<u32>,
}

/// A transfer reply, once a client has destructured its own RESP value type.
///
/// The destructuring differs per client - each RESP library has its own value enum -
/// but the range checks do not, so they live here rather than once per client.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferReply {
    /// Nil: the key was absent, and no transfer took place.
    Missing,
    /// A bare byte count.
    Bytes(i64),
    /// A `[bytes, checksum]` pair.
    BytesAndChecksum {
        /// Byte length transferred.
        bytes: i64,
        /// CRC-32c the server computed over those bytes.
        checksum: i64,
    },
}

impl TransferReply {
    /// Range-check the reply into a receipt. `None` for a missing key.
    pub fn receipt(self) -> Result<Option<TransferReceipt>, DmaError> {
        let (bytes_written, checksum) = match self {
            Self::Missing => return Ok(None),
            Self::Bytes(bytes_written) => (bytes_written, None),
            Self::BytesAndChecksum { bytes, checksum } => {
                let checksum = u32::try_from(checksum)
                    .map_err(|_| DmaError::Protocol(format!("checksum out of range {checksum}")))?;
                (bytes, Some(checksum))
            }
        };
        let bytes_written = usize::try_from(bytes_written)
            .map_err(|_| DmaError::Protocol(format!("negative byte count {bytes_written}")))?;
        Ok(Some(TransferReceipt {
            bytes_written,
            checksum,
        }))
    }
}

/// Options for `DMA.SET`.
#[derive(Debug, Clone, Copy, Default)]
pub struct DmaSetOptions {
    checksum: Option<u32>,
}

impl DmaSetOptions {
    /// Send `checksum` for the server to verify against the bytes it reads. A
    /// mismatch aborts the set.
    pub fn with_checksum(mut self, checksum: u32) -> Self {
        self.checksum = Some(checksum);
        self
    }
}

/// Options for `DMA.GET`.
#[derive(Debug, Clone, Copy, Default)]
pub struct DmaGetOptions {
    checksum: bool,
}

impl DmaGetOptions {
    /// Ask the server to return a CRC-32c of the value.
    pub fn with_checksum(mut self) -> Self {
        self.checksum = true;
        self
    }

    /// Whether a checksum was asked for.
    pub fn checksum_requested(&self) -> bool {
        self.checksum
    }
}

/// A `DMA.*` command: its name, and its arguments in wire order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DmaCommand {
    name: &'static str,
    arguments: Vec<Vec<u8>>,
}

impl DmaCommand {
    /// The command name, as sent.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// The arguments, in the order they go on the wire.
    pub fn arguments(&self) -> &[Vec<u8>] {
        &self.arguments
    }
}

/// `DMA.HELLO`
pub fn hello() -> DmaCommand {
    DmaCommand {
        name: "DMA.HELLO",
        arguments: Vec::new(),
    }
}

/// `DMA.INFO` - dump diagnostics about the module's environment.
pub fn info() -> DmaCommand {
    DmaCommand {
        name: "DMA.INFO",
        arguments: Vec::new(),
    }
}

/// `DMA.SET <address> <rkey> <remote-address> <length> <key> [<crc>]`
pub fn set(
    advertisement: &crate::Advertisement,
    key: &[u8],
    length: usize,
    options: &DmaSetOptions,
) -> DmaCommand {
    let mut command = transfer("DMA.SET", advertisement, key, length);
    if let Some(checksum) = options.checksum {
        command.arguments.push(number(u64::from(checksum)));
    }
    command
}

/// `DMA.GET <address> <rkey> <remote-address> <capacity> <key> [<crc-flag>]`
pub fn get(
    advertisement: &crate::Advertisement,
    key: &[u8],
    capacity: usize,
    options: &DmaGetOptions,
) -> DmaCommand {
    let mut command = transfer("DMA.GET", advertisement, key, capacity);
    if options.checksum {
        // The flag's presence is what asks for a checksum; the value is unread.
        command.arguments.push(number(0));
    }
    command
}

fn transfer(
    name: &'static str,
    advertisement: &crate::Advertisement,
    key: &[u8],
    length: usize,
) -> DmaCommand {
    let mut arguments: Vec<Vec<u8>> = advertisement
        .to_args()
        .into_iter()
        .map(String::into_bytes)
        .collect();
    arguments.push(number(length as u64));
    arguments.push(key.to_vec());
    DmaCommand { name, arguments }
}

/// Decimal ASCII, matching how a RESP client renders an integer argument.
fn number(value: u64) -> Vec<u8> {
    value.to_string().into_bytes()
}

#[cfg(test)]
mod tests {
    use super::{DmaCommand, DmaGetOptions, DmaSetOptions, TransferReply, get, hello, info, set};
    use crate::Advertisement;

    fn advertisement() -> Advertisement {
        Advertisement {
            address: vec![0xde, 0xad],
            remote_key: 7,
            remote_address: 0x1000,
        }
    }

    fn arguments(command: &DmaCommand) -> Vec<String> {
        command
            .arguments()
            .iter()
            .map(|argument| String::from_utf8_lossy(argument).into_owned())
            .collect()
    }

    #[test]
    fn set_lays_out_arguments_in_wire_order() {
        let command = set(&advertisement(), b"key", 64, &DmaSetOptions::default());
        assert_eq!(command.name(), "DMA.SET");
        assert_eq!(arguments(&command), ["dead", "7", "4096", "64", "key"]);
    }

    #[test]
    fn get_lays_out_arguments_in_wire_order() {
        let command = get(&advertisement(), b"key", 64, &DmaGetOptions::default());
        assert_eq!(command.name(), "DMA.GET");
        assert_eq!(arguments(&command), ["dead", "7", "4096", "64", "key"]);
    }

    #[test]
    fn set_appends_the_checksum_last() {
        let options = DmaSetOptions::default().with_checksum(0xabcd);
        let command = set(&advertisement(), b"key", 64, &options);
        assert_eq!(arguments(&command).last().unwrap(), "43981");
    }

    #[test]
    fn get_appends_a_flag_only_when_a_checksum_is_wanted() {
        let plain = get(&advertisement(), b"key", 64, &DmaGetOptions::default());
        assert_eq!(plain.arguments().len(), 5);

        let wanted = get(
            &advertisement(),
            b"key",
            64,
            &DmaGetOptions::default().with_checksum(),
        );
        assert_eq!(wanted.arguments().len(), 6);
    }

    /// A key is bytes, not text, and must survive unaltered.
    #[test]
    fn a_key_is_carried_as_raw_bytes() {
        let command = set(
            &advertisement(),
            &[0x00, 0xff],
            1,
            &DmaSetOptions::default(),
        );
        assert_eq!(command.arguments().last().unwrap().as_slice(), [0x00, 0xff]);
    }

    #[test]
    fn a_missing_key_has_no_receipt() {
        assert_eq!(TransferReply::Missing.receipt().unwrap(), None);
    }

    #[test]
    fn a_byte_count_becomes_a_receipt() {
        let receipt = TransferReply::Bytes(1024).receipt().unwrap().unwrap();
        assert_eq!(receipt.bytes_written, 1024);
        assert_eq!(receipt.checksum, None);
    }

    #[test]
    fn a_checksum_rides_along_with_the_byte_count() {
        let reply = TransferReply::BytesAndChecksum {
            bytes: 1024,
            checksum: 0xE306_9283,
        };
        let receipt = reply.receipt().unwrap().unwrap();
        assert_eq!(receipt.bytes_written, 1024);
        assert_eq!(receipt.checksum, Some(0xE306_9283));
    }

    /// The server cannot have moved a negative number of bytes, so this is a corrupt
    /// reply rather than a huge one.
    #[test]
    fn a_negative_byte_count_is_rejected() {
        assert!(TransferReply::Bytes(-1).receipt().is_err());
    }

    #[test]
    fn a_checksum_outside_u32_is_rejected() {
        let reply = TransferReply::BytesAndChecksum {
            bytes: 1,
            checksum: i64::from(u32::MAX) + 1,
        };
        assert!(reply.receipt().is_err());
    }

    #[test]
    fn handshake_commands_carry_no_arguments() {
        assert_eq!(hello().name(), "DMA.HELLO");
        assert!(hello().arguments().is_empty());
        assert_eq!(info().name(), "DMA.INFO");
        assert!(info().arguments().is_empty());
    }
}
