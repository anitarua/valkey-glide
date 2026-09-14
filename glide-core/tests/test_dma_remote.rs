// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! End-to-end DMA against a server running the valkey-dma module.
//!
//! Ignored by default, because this repository cannot start such a server.
//! EFA hardware is required only for `efa-direct`.
//!
//! | To run | You need |
//! |---|---|
//! | over `tcp`, one machine | libfabric, and a `valkey-dma.so` built for that machine |
//! | over `efa-direct` | the above, plus two EFA instances in one AZ, same VPC |
//!
//! Loopback, the more accessible setup:
//!
//! ```sh
//! valkey-server --port 6379 --loadmodule ./valkey-dma.so config ./tcp.toml &
//! export GLIDE_DMA_SERVER=127.0.0.1:6379
//! export GLIDE_DMA_PROVIDER=tcp
//! cargo test --features "proto,socket-layer,dma" --test test_dma_remote -- --ignored --nocapture
//! ```
//!
//! Against EFA hardware, where the transfer is a real NIC operation:
//!
//! ```sh
//! export PKG_CONFIG_PATH=/opt/amazon/efa/lib64/pkgconfig
//! export LD_LIBRARY_PATH=/opt/amazon/efa/lib64
//! export GLIDE_DMA_SERVER=<server private IP>:6379
//! export GLIDE_DMA_PROVIDER=efa-direct
//! cargo test --features "proto,socket-layer,dma" --test test_dma_remote -- --ignored --nocapture
//! ```

#![cfg(feature = "dma")]

#[cfg(test)]
mod remote_dma_tests {
    use glide_core::client::{Client, ConnectionRequest, NodeAddress};
    use glide_core::dma::{DmaSetting, FabricConfig, Provider};

    const VALUE_BYTES: usize = 1 << 20;

    fn server() -> (String, u16) {
        let address = std::env::var("GLIDE_DMA_SERVER")
            .expect("set GLIDE_DMA_SERVER=<host>:<port> to run the remote DMA tests");
        let (host, port) = address
            .rsplit_once(':')
            .expect("GLIDE_DMA_SERVER must be host:port");
        (
            host.to_string(),
            port.parse().expect("port must be a number"),
        )
    }

    fn provider() -> Provider {
        match std::env::var("GLIDE_DMA_PROVIDER")
            .expect("set GLIDE_DMA_PROVIDER=tcp or efa-direct")
            .as_str()
        {
            "tcp" => Provider::Tcp,
            "efa-direct" => Provider::EfaDirect,
            other => panic!("GLIDE_DMA_PROVIDER must be tcp or efa-direct, got {other:?}"),
        }
    }

    async fn dma_client() -> Client {
        let (host, port) = server();
        let mut fabric = FabricConfig::new(provider());
        if let Ok(interface) = std::env::var("GLIDE_DMA_INTERFACE") {
            fabric = fabric.with_interface(interface);
        }
        let request = ConnectionRequest {
            addresses: vec![NodeAddress { host, port }],
            dma: DmaSetting::Configured(fabric),
            ..Default::default()
        };
        Client::new(request, None)
            .await
            .expect("DMA client construction: handshake and compatibility check must pass")
    }

    /// Construction alone exercises DMA.INFO parsing, the provider and
    /// addressing comparison, DMA.HELLO, and peer insertion. If the real
    /// DMA.INFO format differs from what `parse_info` assumes, this is where it
    /// shows up as MalformedHandshake.
    #[tokio::test]
    #[serial_test::serial]
    #[ignore = "needs a server running the valkey-dma module"]
    async fn the_handshake_succeeds_against_a_real_module() {
        let _client = dma_client().await;
    }

    /// The round trip: stage a value, let the server read it out of registered
    /// memory, then let it write the value back into a second buffer, and
    /// compare. Both directions carry a checksum, so a misaddressed transfer
    /// fails here rather than returning plausible-looking bytes.
    #[tokio::test]
    #[serial_test::serial]
    #[ignore = "needs a server running the valkey-dma module"]
    async fn a_value_round_trips_through_the_fabric() {
        let mut client = dma_client().await;
        let key = format!("glide-dma-roundtrip-{}", std::process::id());

        let value: Vec<u8> = (0..VALUE_BYTES).map(|index| index as u8).collect();

        let mut source = client
            .register_dma_buffer(vec![0u8; VALUE_BYTES])
            .expect("registering the source region");
        source
            .copy_from(&value)
            .expect("staging the value into registered memory");

        // Checksum the value we meant to store, not the buffer we staged it
        // into: the server then verifies that what reached registered memory is
        // what was intended, so a staging bug fails here rather than storing
        // self-consistent garbage.
        let set_options =
            glide_dma::DmaSetOptions::default().with_checksum(glide_dma::checksum(&value));
        let receipt = client
            .dma_set(key.as_bytes(), &source, VALUE_BYTES, &set_options)
            .await
            .expect("DMA.SET");
        assert_eq!(receipt.bytes_written, VALUE_BYTES);

        // A separate region, scrubbed, so a "successful" read of untouched
        // memory cannot be mistaken for a transfer.
        let mut destination = client
            .register_dma_buffer(vec![0xAAu8; VALUE_BYTES])
            .expect("registering the destination region");

        let receipt = client
            .dma_get(
                key.as_bytes(),
                &mut destination,
                &glide_dma::DmaGetOptions::default().with_checksum(),
            )
            .await
            .expect("DMA.GET")
            .expect("the key was just written, so it must exist");
        assert_eq!(receipt.bytes_written, VALUE_BYTES);

        let landed = destination.as_host().expect("host memory");
        assert_eq!(
            &landed[..VALUE_BYTES],
            &value[..],
            "the bytes the server transferred must match what was stored"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    #[ignore = "needs a server running the valkey-dma module"]
    async fn a_missing_key_transfers_nothing() {
        let mut client = dma_client().await;
        let mut buffer = client
            .register_dma_buffer(vec![0u8; 4096])
            .expect("registering");

        let absent = format!("glide-dma-absent-{}", std::process::id());
        let receipt = client
            .dma_get(
                absent.as_bytes(),
                &mut buffer,
                &glide_dma::DmaGetOptions::default(),
            )
            .await
            .expect("DMA.GET against a missing key is not an error");
        assert!(receipt.is_none(), "a missing key must transfer nothing");
    }
}
