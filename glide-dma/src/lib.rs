// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! Opt-in DMA support for Valkey GLIDE.
//! `DMA.GET` and `DMA.SET` commands use the RESP connection to coordinate with the
//! server but the server performs the actual data transfer to the advertised memory
//! location. The RESP response contains only a byte count and optional checksum.

mod advertisement;
#[cfg(feature = "libfabric")]
mod buffer;
mod checksum;
mod command;
mod config;
#[cfg(feature = "libfabric")]
mod endpoint;
mod error;
#[cfg(feature = "libfabric")]
mod fabric;
#[cfg(feature = "libfabric")]
mod progress;
mod protocol;

pub use advertisement::{Advertisement, AdvertisementError, decode_hex, encode_hex};
#[cfg(feature = "libfabric")]
pub use buffer::{DmaBuffer, RegionWindow};
pub use checksum::checksum;
pub use command::{
    DmaCommand, DmaGetOptions, DmaSetOptions, TransferReceipt, TransferReply, get, hello, info, set,
};
pub use config::{FabricConfig, Provider};
pub use error::DmaError;
#[cfg(feature = "libfabric")]
pub use fabric::{DmaFabric, discover_domains};
#[cfg(feature = "libfabric")]
pub use progress::ProgressGuard;

pub use protocol::{
    get_command, hello_command, info_command, parse_hello, parse_receipt, redis_command,
    set_command,
};
