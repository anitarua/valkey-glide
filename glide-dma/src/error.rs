// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! Errors from a DMA command or the fabric carrying its payload.

use crate::advertisement::AdvertisementError;

/// An error from a DMA transfer.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DmaError {
    /// The server's reply did not follow the DMA protocol.
    #[error("dma protocol error: {0}")]
    Protocol(String),

    /// The server reported moving a different number of bytes than the caller expected.
    #[error("dma transferred {actual} bytes, expected {expected}")]
    ByteCountMismatch {
        /// Bytes the caller expected to move.
        expected: usize,
        /// Bytes the server reported moving.
        actual: usize,
    },

    /// A checksum did not match. On `DMA.GET` the server's checksum disagreed with
    /// the bytes that landed. On `DMA.SET` the server rejected the value before
    /// storing it.
    #[error("dma checksum mismatch: expected {expected:#010x}, got {actual:#010x}")]
    ChecksumMismatch {
        /// The checksum computed over the bytes that were meant to move.
        expected: u32,
        /// The checksum actually observed.
        actual: u32,
    },

    /// The value does not fit in the window the caller offered.
    /// Retry with a larger window, or fall back to a plain `GET`.
    #[error("value of {value_bytes} bytes exceeds the {capacity} byte registered window")]
    PayloadTooLarge {
        /// Size of the value on the server.
        value_bytes: usize,
        /// Capacity the caller advertised.
        capacity: usize,
    },

    /// A libfabric call failed.
    #[error("fabric error in {operation}: {message}{}", errno.map(|e| format!(" (errno {e})")).unwrap_or_default())]
    Fabric {
        /// The libfabric operation that failed.
        operation: &'static str,
        /// `fi_strerror` rendering of the failure.
        message: String,
        /// The raw libfabric error code, where one was returned.
        errno: Option<i32>,
    },

    /// The fabric could not be configured as requested.
    #[error("fabric configuration error: {0}")]
    Configuration(String),
}

impl From<AdvertisementError> for DmaError {
    fn from(error: AdvertisementError) -> Self {
        DmaError::Protocol(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::DmaError;
    use crate::advertisement::AdvertisementError;

    #[test]
    fn advertisement_errors_become_protocol_errors() {
        let error: DmaError = AdvertisementError::Arity.into();
        assert!(matches!(error, DmaError::Protocol(_)));
    }

    #[test]
    fn fabric_errors_render_errno_only_when_present() {
        let with_errno = DmaError::Fabric {
            operation: "fi_mr_reg",
            message: "Cannot allocate memory".to_string(),
            errno: Some(-12),
        };
        assert!(with_errno.to_string().contains("errno -12"));

        let without = DmaError::Fabric {
            operation: "fi_getinfo",
            message: "No data available".to_string(),
            errno: None,
        };
        assert!(!without.to_string().contains("errno"));
    }
}
