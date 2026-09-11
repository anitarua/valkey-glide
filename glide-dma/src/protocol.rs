// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

use redis::{Cmd, RedisError, Value, cmd};

use crate::advertisement::{Advertisement, decode_hex};
use crate::command::{
    self, DmaCommand, DmaGetOptions, DmaSetOptions, TransferReceipt, TransferReply,
};
use crate::error::DmaError;

/// Build a redis-rs [`Cmd`] from a [`DmaCommand`].
pub fn redis_command(command: &DmaCommand) -> Cmd {
    let mut built = cmd(command.name());
    for argument in command.arguments() {
        built.arg(argument.as_slice());
    }
    built
}

/// `DMA.HELLO`
pub fn hello_command() -> Cmd {
    redis_command(&command::hello())
}

/// `DMA.INFO` - dump diagnostics about the module's environment.
pub fn info_command() -> Cmd {
    redis_command(&command::info())
}

/// `DMA.SET <address> <rkey> <remote-address> <length> <key> [<crc>]`
pub fn set_command(
    advertisement: &Advertisement,
    key: &[u8],
    length: usize,
    options: &DmaSetOptions,
) -> Cmd {
    redis_command(&command::set(advertisement, key, length, options))
}

/// `DMA.GET <address> <rkey> <remote-address> <capacity> <key> [<crc-flag>]`
pub fn get_command(
    advertisement: &Advertisement,
    key: &[u8],
    capacity: usize,
    options: &DmaGetOptions,
) -> Cmd {
    redis_command(&command::get(advertisement, key, capacity, options))
}

/// Parse a transfer reply: nil for a missing key, a byte count, or a
/// `[bytes, checksum]` pair.
pub fn parse_receipt(reply: Value) -> Result<Option<TransferReceipt>, DmaError> {
    let reply = match reply {
        Value::Nil => TransferReply::Missing,
        Value::Int(bytes) => TransferReply::Bytes(bytes),
        Value::Array(items) => match items.as_slice() {
            [Value::Int(bytes), Value::Int(checksum)] => TransferReply::BytesAndChecksum {
                bytes: *bytes,
                checksum: *checksum,
            },
            other => {
                return Err(DmaError::Protocol(format!(
                    "expected [bytes, checksum], got {} elements",
                    other.len()
                )));
            }
        },
        other => return Err(DmaError::Protocol(format!("unexpected reply {other:?}"))),
    };
    reply.receipt()
}

/// Parse `DMA.HELLO` into every fabric address the server may initiate from.
pub fn parse_hello(reply: Value) -> Result<Vec<Vec<u8>>, DmaError> {
    let encoded: Vec<String> = redis::from_redis_value(&reply)
        .map_err(|error| DmaError::Protocol(format!("dma.hello: {error}")))?;
    if encoded.is_empty() {
        return Err(DmaError::Protocol("dma.hello returned no addresses".into()));
    }
    encoded
        .iter()
        .map(|address| Ok(decode_hex(address.as_bytes())?))
        .collect()
}

/// Translate a DMA failure into the error type the rest of GLIDE carries.
impl From<DmaError> for RedisError {
    fn from(error: DmaError) -> Self {
        use redis::ErrorKind;
        let kind = match &error {
            DmaError::Protocol(_) => ErrorKind::ProtocolDesync,
            DmaError::ByteCountMismatch { .. } | DmaError::ChecksumMismatch { .. } => {
                ErrorKind::ClientError
            }
            DmaError::PayloadTooLarge { .. } => ErrorKind::UserOperationError,
            DmaError::Fabric { .. } => ErrorKind::IoError,
            DmaError::Configuration(_) => ErrorKind::InvalidClientConfig,
        };
        RedisError::from((kind, "DMA error", error.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DmaGetOptions, DmaSetOptions, get_command, hello_command, parse_hello, parse_receipt,
        set_command,
    };
    use crate::advertisement::Advertisement;
    use crate::error::DmaError;
    use redis::{ErrorKind, RedisError, Value};

    fn advertisement() -> Advertisement {
        Advertisement {
            address: vec![0xde, 0xad],
            remote_key: 7,
            remote_address: 0x1000,
        }
    }

    fn args(command: &redis::Cmd) -> Vec<String> {
        command
            .args_iter()
            .map(|arg| match arg {
                redis::Arg::Simple(bytes) => String::from_utf8_lossy(bytes).into_owned(),
                _ => "<non-simple>".to_string(),
            })
            .collect()
    }

    /// The layout itself is asserted in [`crate::command`]; this is about the mapping
    /// onto `Cmd` carrying the name and every argument through unchanged.
    #[test]
    fn set_maps_onto_a_cmd_in_wire_order() {
        let command = set_command(&advertisement(), b"key", 64, &DmaSetOptions::default());
        assert_eq!(
            args(&command),
            ["DMA.SET", "dead", "7", "4096", "64", "key"]
        );
    }

    #[test]
    fn get_maps_onto_a_cmd_in_wire_order() {
        let options = DmaGetOptions::default().with_checksum();
        let command = get_command(&advertisement(), b"key", 64, &options);
        assert_eq!(
            args(&command),
            ["DMA.GET", "dead", "7", "4096", "64", "key", "0"]
        );
    }

    #[test]
    fn an_argumentless_command_is_just_its_name() {
        assert_eq!(args(&hello_command()), ["DMA.HELLO"]);
    }

    #[test]
    fn parses_a_missing_key_as_none() {
        assert_eq!(parse_receipt(Value::Nil).unwrap(), None);
    }

    #[test]
    fn parses_a_bare_byte_count() {
        let receipt = parse_receipt(Value::Int(1024)).unwrap().unwrap();
        assert_eq!(receipt.bytes_written, 1024);
        assert_eq!(receipt.checksum, None);
    }

    #[test]
    fn parses_a_byte_count_and_checksum() {
        let reply = Value::Array(vec![Value::Int(1024), Value::Int(0xE306_9283)]);
        let receipt = parse_receipt(reply).unwrap().unwrap();
        assert_eq!(receipt.bytes_written, 1024);
        assert_eq!(receipt.checksum, Some(0xE306_9283));
    }

    #[test]
    fn rejects_a_negative_byte_count() {
        assert!(parse_receipt(Value::Int(-1)).is_err());
    }

    #[test]
    fn rejects_a_malformed_pair() {
        let reply = Value::Array(vec![Value::Int(1), Value::Int(2), Value::Int(3)]);
        assert!(parse_receipt(reply).is_err());
    }

    #[test]
    fn parses_every_hello_address() {
        let reply = Value::Array(vec![
            Value::BulkString(b"dead".to_vec().into()),
            Value::BulkString(b"beef".to_vec().into()),
        ]);
        assert_eq!(
            parse_hello(reply).unwrap(),
            [vec![0xde, 0xad], vec![0xbe, 0xef]]
        );
    }

    #[test]
    fn rejects_an_empty_hello() {
        assert!(parse_hello(Value::Array(vec![])).is_err());
    }

    #[test]
    fn dma_errors_carry_their_kind_and_message() {
        let too_large: RedisError = DmaError::PayloadTooLarge {
            value_bytes: 1_048_576,
            capacity: 65_536,
        }
        .into();
        assert_eq!(too_large.kind(), ErrorKind::UserOperationError);
        assert!(too_large.to_string().contains("1048576"));

        let desync: RedisError = DmaError::Protocol("bad reply".into()).into();
        assert_eq!(desync.kind(), ErrorKind::ProtocolDesync);

        let fabric: RedisError = DmaError::Fabric {
            operation: "fi_mr_reg",
            message: "Cannot allocate memory".into(),
            errno: Some(-12),
        }
        .into();
        assert_eq!(fabric.kind(), ErrorKind::IoError);
        assert!(fabric.to_string().contains("errno -12"));
    }
}
