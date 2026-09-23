// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! The RESP side of the RDMA commands.
//!
//! `glide-rdma` describes a command as a name and arguments and knows nothing
//! about RESP, so that it can be depended on by the connection layer, which is
//! what builds RESP. This is the adapter between the two: it turns those
//! descriptions into redis-rs commands and reads the replies back.

use redis::{Cmd, RedisError, Value, cmd};

use glide_rdma::{Handshake, RdmaCommand, RdmaError, ReadReceipt, RegionRef, decode_hex};

/// Build a redis-rs [`Cmd`] from a [`RdmaCommand`].
pub fn redis_command(command: &RdmaCommand) -> Cmd {
    let mut built = cmd(command.name());
    for argument in command.arguments() {
        built.arg(argument.as_slice());
    }
    built
}

/// `LO.HELLO <client-address>`
pub fn hello_command(client_address: &[u8]) -> Cmd {
    redis_command(&glide_rdma::hello(client_address))
}

/// `LO.SET <key> <length> <rkey> <remote-address>`
pub fn set_command(key: &[u8], length: usize, region_ref: &RegionRef) -> Cmd {
    redis_command(&glide_rdma::set(key, length, region_ref))
}

/// `LO.GET <key> <rkey> <remote-address>`
pub fn get_command(key: &[u8], region_ref: &RegionRef) -> Cmd {
    redis_command(&glide_rdma::get(key, region_ref))
}

/// Parse a read reply: nil for a missing key, a byte count, or a
/// `[bytes, checksum]` pair.
pub fn parse_receipt(reply: Value) -> Result<Option<ReadReceipt>, RedisError> {
    let (bytes_written, checksum) = match reply {
        Value::Nil => return Ok(None),
        Value::Int(bytes) => (bytes, None),
        Value::Array(items) => match items.as_slice() {
            [Value::Int(bytes), Value::Int(checksum)] => (*bytes, Some(*checksum)),
            other => {
                return Err(protocol_error(format!(
                    "expected [bytes, checksum], got {} elements",
                    other.len()
                )));
            }
        },
        other => return Err(protocol_error(format!("unexpected reply {other:?}"))),
    };
    ReadReceipt::from_wire(bytes_written, checksum)
        .map(Some)
        .map_err(as_redis_error)
}

/// Check a write reply. `LO.SET` answers `OK` on success.
pub fn parse_write_ack(reply: Value) -> Result<(), RedisError> {
    match reply {
        Value::Okay => Ok(()),
        Value::SimpleString(status) if status.eq_ignore_ascii_case("ok") => Ok(()),
        other => Err(protocol_error(format!(
            "expected OK from lo.set, got {other:?}"
        ))),
    }
}

/// Parse `LO.HELLO` into every fabric address the server may initiate from.
pub fn parse_hello(reply: Value) -> Result<Handshake, RedisError> {
    let encoded: Vec<String> = redis::from_redis_value(&reply)
        .map_err(|error| protocol_error(format!("lo.hello: {error}")))?;
    if encoded.is_empty() {
        return Err(protocol_error("lo.hello returned no addresses"));
    }
    let peers = encoded
        .iter()
        .map(|address| decode_hex(address.as_bytes()))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| protocol_error(error.to_string()))?;
    Ok(Handshake { peers })
}

/// Translate a RDMA failure into the error type the rest of GLIDE carries.
///
/// A free function rather than a `From` impl: both types are foreign to this
/// crate now that the fabric and the protocol live apart, so the impl would not
/// be allowed here and belongs to neither of them.
pub fn as_redis_error(error: RdmaError) -> RedisError {
    use redis::ErrorKind;
    let kind = match &error {
        RdmaError::Protocol(_) => ErrorKind::ProtocolDesync,
        RdmaError::ByteCountMismatch { .. } | RdmaError::ChecksumMismatch { .. } => {
            ErrorKind::ClientError
        }
        RdmaError::PayloadTooLarge { .. } => ErrorKind::UserOperationError,
        RdmaError::Fabric { .. } => ErrorKind::IoError,
        RdmaError::Configuration(_) | RdmaError::LibfabricUnavailable { .. } => {
            ErrorKind::InvalidClientConfig
        }
    };
    RedisError::from((kind, "RDMA error", error.to_string()))
}

/// A reply this client could not read, as the error the rest of GLIDE carries.
fn protocol_error(message: impl Into<String>) -> RedisError {
    as_redis_error(RdmaError::Protocol(message.into()))
}

/// A configuration failure, as the error the rest of GLIDE carries.
pub fn configuration_error(message: impl Into<String>) -> RedisError {
    as_redis_error(RdmaError::Configuration(message.into()))
}

/// How every cancellation's message begins. The Python client recognises a
/// cancellation by it, so it must not change without changing that too.
pub const CANCELLED: &str = "RDMA transfer cancelled";

/// A transfer given up because its region was revoked, usually by closing the
/// client. Never retried: the region it needs is gone.
pub fn cancelled(reason: impl Into<String>) -> RedisError {
    RedisError::from((redis::ErrorKind::ClientError, CANCELLED, reason.into()))
}

#[cfg(test)]
mod tests {
    use super::{
        CANCELLED, as_redis_error, cancelled, get_command, hello_command, parse_hello,
        parse_receipt, parse_write_ack, set_command,
    };
    use glide_rdma::{RdmaError, RegionRef};
    use redis::{ErrorKind, Value};

    fn region_ref() -> RegionRef {
        RegionRef {
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
        let command = set_command(b"key", 64, &region_ref());
        assert_eq!(args(&command), ["LO.SET", "key", "64", "7", "4096"]);
    }

    #[test]
    fn get_maps_onto_a_cmd_in_wire_order() {
        let command = get_command(b"key", &region_ref());
        assert_eq!(args(&command), ["LO.GET", "key", "7", "4096"]);
    }

    #[test]
    fn hello_maps_onto_a_cmd_carrying_the_client_address() {
        assert_eq!(args(&hello_command(&[0xde, 0xad])), ["LO.HELLO", "dead"]);
    }

    #[test]
    fn a_write_is_acknowledged_by_ok() {
        assert!(parse_write_ack(Value::Okay).is_ok());
        assert!(parse_write_ack(Value::SimpleString("ok".into())).is_ok());
    }

    #[test]
    fn a_write_reply_that_is_not_ok_is_a_protocol_error() {
        assert!(parse_write_ack(Value::Int(64)).is_err());
        assert!(parse_write_ack(Value::Nil).is_err());
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
        let handshake = parse_hello(reply).unwrap();
        assert_eq!(handshake.peers, [vec![0xde, 0xad], vec![0xbe, 0xef]]);
    }

    #[test]
    fn rejects_an_empty_hello() {
        assert!(parse_hello(Value::Array(vec![])).is_err());
    }

    #[test]
    fn rdma_errors_carry_their_kind_and_message() {
        let too_large = as_redis_error(RdmaError::PayloadTooLarge {
            value_bytes: 1_048_576,
            capacity: 65_536,
        });
        assert_eq!(too_large.kind(), ErrorKind::UserOperationError);
        assert!(too_large.to_string().contains("1048576"));

        let desync = as_redis_error(RdmaError::Protocol("bad reply".into()));
        assert_eq!(desync.kind(), ErrorKind::ProtocolDesync);

        let fabric = as_redis_error(RdmaError::Fabric {
            operation: "fi_mr_reg",
            message: "Cannot allocate memory".into(),
            errno: Some(-12),
        });
        assert_eq!(fabric.kind(), ErrorKind::IoError);
        assert!(fabric.to_string().contains("errno -12"));
    }

    /// Clients in other languages tell a cancellation from other failures by how
    /// its message begins.
    #[test]
    fn a_cancellation_message_begins_with_the_marker() {
        let error = cancelled("the client was closed");

        assert!(error.to_string().starts_with(CANCELLED), "{error}");
        assert_eq!(error.kind(), ErrorKind::ClientError);
    }
}
