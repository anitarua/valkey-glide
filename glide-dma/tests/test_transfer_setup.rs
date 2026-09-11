// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! Tests the client flow for setting up a transfer: register, stage, slice, build a command.
//! Does not require a server. Does require the `libfabric` feature and libfabric installed.
//!
//! Run with:
//!   PKG_CONFIG_PATH="$(brew --prefix libfabric)/lib/pkgconfig" \
//!     cargo test -p glide-dma --features libfabric --test test_transfer_setup

#![cfg(feature = "libfabric")]

use glide_dma::{
    checksum, encode_hex, set_command, DmaFabric, DmaSetOptions, FabricConfig, Provider,
};

/// The whole client-side sequence short of the transfer itself.
#[test]
fn a_registered_buffer_produces_a_sendable_set_command() {
    let fabric = DmaFabric::open(&FabricConfig::new(Provider::Tcp))
        .expect("the tcp provider should open on any host with libfabric");

    let mut buffer = fabric
        .register(vec![0u8; 64 * 1024])
        .expect("registration failed");

    let payload = b"the quick brown fox";
    assert_eq!(buffer.copy_from(payload), Some(payload.len()));

    let window = buffer.slice(0, payload.len()).expect("window should fit");
    let command = set_command(
        window.advertisement(),
        b"chunk:abc",
        window.length(),
        &DmaSetOptions::default().with_checksum(checksum(payload)),
    );

    let args: Vec<Vec<u8>> = command
        .args_iter()
        .filter_map(|arg| match arg {
            redis::Arg::Simple(bytes) => Some(bytes.to_vec()),
            redis::Arg::Cursor => None,
        })
        .collect();

    assert_eq!(args[0], b"DMA.SET");
    assert_eq!(args[1], encode_hex(fabric.local_address()).into_bytes());
    assert_eq!(args[4], payload.len().to_string().into_bytes());
    assert_eq!(args[5], b"chunk:abc");
}
