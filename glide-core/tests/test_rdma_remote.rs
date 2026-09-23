// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! End-to-end RDMA against a server that serves the `LO.*` commands.
//!
//! Ignored by default because this repository cannot start such a server.
//! EFA hardware is required only for `efa-direct`.
//!
//! | To run | You need |
//! |---|---|
//! | over `tcp`, one machine | libfabric and a module built for that machine |
//! | over `efa-direct` | the above, plus two EFA instances in one AZ, same VPC |
//!
//! Loopback, the more accessible setup:
//!
//! ```sh
//! valkey-server --port 6379 --loadmodule ./<rdma-module>.so <module arguments> &
//! export GLIDE_RDMA_SERVER=127.0.0.1:6379
//! export GLIDE_RDMA_PROVIDER=tcp
//! cargo test --features "proto,socket-layer,rdma" --test test_rdma_remote -- --ignored --nocapture
//! ```
//!
//! Against EFA hardware, where the transfer is a real NIC operation:
//!
//! ```sh
//! export PKG_CONFIG_PATH=/opt/amazon/efa/lib64/pkgconfig
//! export LD_LIBRARY_PATH=/opt/amazon/efa/lib64
//! export GLIDE_RDMA_SERVER=<server private IP>:6379
//! export GLIDE_RDMA_PROVIDER=efa-direct
//! cargo test --features "proto,socket-layer,rdma" --test test_rdma_remote -- --ignored --nocapture
//! ```

#![cfg(feature = "rdma")]

#[cfg(test)]
mod remote_dma_tests {
    use glide_core::client::{Client, ConnectionRequest, NodeAddress};
    use glide_core::rdma::{FabricConfig, Provider, RdmaSetting};

    const VALUE_BYTES: usize = 1 << 20;

    fn server() -> (String, u16) {
        let address = std::env::var("GLIDE_RDMA_SERVER")
            .expect("set GLIDE_RDMA_SERVER=<host>:<port> to run the remote RDMA tests");
        let (host, port) = address
            .rsplit_once(':')
            .expect("GLIDE_RDMA_SERVER must be host:port");
        (
            host.to_string(),
            port.parse().expect("port must be a number"),
        )
    }

    fn provider() -> Provider {
        match std::env::var("GLIDE_RDMA_PROVIDER")
            .expect("set GLIDE_RDMA_PROVIDER=tcp or efa-direct")
            .as_str()
        {
            "tcp" => Provider::Tcp,
            "efa-direct" => Provider::EfaDirect,
            other => panic!("GLIDE_RDMA_PROVIDER must be tcp or efa-direct, got {other:?}"),
        }
    }

    async fn dma_client() -> Client {
        let (host, port) = server();
        let mut fabric = FabricConfig::new(provider());
        if let Ok(interface) = std::env::var("GLIDE_RDMA_INTERFACE") {
            fabric = fabric.with_interface(interface);
        }
        let request = ConnectionRequest {
            addresses: vec![NodeAddress { host, port }],
            rdma: RdmaSetting::Configured(fabric),
            ..Default::default()
        };
        Client::new(request, None)
            .await
            .expect("RDMA client construction: handshake and compatibility check must pass")
    }

    #[tokio::test]
    #[serial_test::serial]
    #[ignore = "needs a server serving the LO.* commands"]
    async fn the_handshake_succeeds_against_a_real_module() {
        let mut client = dma_client().await;

        assert_eq!(
            client.rdma_peer_count(),
            0,
            "connecting sends no handshake: a client that never transfers pays \
             nothing for having asked for RDMA"
        );
        assert!(
            !client.rdma_handshake_targets().await.is_empty(),
            "a connected client has a target"
        );

        // The first transfer is what handshakes, so this is where a real module
        // hands over the fabric addresses it can be reached from.
        let mut buffer = client
            .register_rdma_region(vec![0u8; 4096])
            .expect("registers against a real fabric");
        client
            .rdma_get(b"lo-handshake-probe", &mut buffer, 0, 4096)
            .await
            .expect("a missing key is an outcome, not a failure");

        assert!(
            0 < client.rdma_peer_count(),
            "the transfer handshaked, so the client knows an address to be \
             reached from"
        );
    }

    /// The round trip: stage a value, let the server read it out of registered
    /// memory, then let it write the value back into a second buffer, and
    /// compare. Both directions carry a checksum, so a misaddressed transfer
    /// fails here rather than returning plausible-looking bytes.
    #[tokio::test]
    #[serial_test::serial]
    #[ignore = "needs a server serving the LO.* commands"]
    async fn a_value_round_trips_through_the_fabric() {
        let mut client = dma_client().await;
        let key = format!("glide-rdma-roundtrip-{}", std::process::id());

        let value: Vec<u8> = (0..VALUE_BYTES).map(|index| index as u8).collect();

        let mut source = client
            .register_rdma_region(vec![0u8; VALUE_BYTES])
            .expect("registering the source region");
        source
            .copy_from(&value)
            .expect("staging the value into registered memory");

        client
            .rdma_set(key.as_bytes(), &source, 0, VALUE_BYTES)
            .await
            .expect("LO.SET");

        // A separate region, scrubbed, so a "successful" read of untouched
        // memory cannot be mistaken for a transfer.
        let mut destination = client
            .register_rdma_region(vec![0xAAu8; VALUE_BYTES])
            .expect("registering the destination region");

        let receipt = client
            .rdma_get(key.as_bytes(), &mut destination, 0, VALUE_BYTES)
            .await
            .expect("LO.GET")
            .expect("the key was just written, so it must exist");
        assert_eq!(receipt.bytes_written, VALUE_BYTES);
        if let Some(checksum) = receipt.checksum {
            assert_eq!(checksum, glide_rdma::checksum(&value));
        }

        let landed = destination.as_host().expect("host memory");
        assert_eq!(
            &landed[..VALUE_BYTES],
            &value[..],
            "the bytes the server transferred must match what was stored"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    #[ignore = "needs a server serving the LO.* commands"]
    async fn windows_of_one_region_round_trip_independently() {
        const SLOT_BYTES: usize = 4096;
        const SLOTS: usize = 4;

        let mut client = dma_client().await;
        let prefix = format!("glide-rdma-window-{}", std::process::id());

        // One registration, filled so each slot holds only its own byte value.
        let mut source = client
            .register_rdma_region(vec![0u8; SLOT_BYTES * SLOTS])
            .expect("registering the source region");
        for slot in 0..SLOTS {
            let offset = slot * SLOT_BYTES;
            let bytes = source.as_host_mut().expect("host memory");
            bytes[offset..offset + SLOT_BYTES].fill(slot as u8 + 1);
        }

        for slot in 0..SLOTS {
            let offset = slot * SLOT_BYTES;
            client
                .rdma_set(
                    format!("{prefix}-{slot}").as_bytes(),
                    &source,
                    offset,
                    SLOT_BYTES,
                )
                .await
                .unwrap_or_else(|err| panic!("LO.SET of slot {slot}: {err}"));
        }

        // Read back into a second region, each value into a different slot than
        // it was written from, so a transfer that ignored either offset lands in
        // the wrong place and fails the comparison.
        let mut destination = client
            .register_rdma_region(vec![0xAAu8; SLOT_BYTES * SLOTS])
            .expect("registering the destination region");
        for slot in 0..SLOTS {
            let destination_slot = SLOTS - 1 - slot;
            let receipt = client
                .rdma_get(
                    format!("{prefix}-{slot}").as_bytes(),
                    &mut destination,
                    destination_slot * SLOT_BYTES,
                    SLOT_BYTES,
                )
                .await
                .unwrap_or_else(|err| panic!("LO.GET of slot {slot}: {err}"))
                .expect("the key was just written, so it must exist");
            assert_eq!(receipt.bytes_written, SLOT_BYTES);
        }

        let landed = destination.as_host().expect("host memory");
        for slot in 0..SLOTS {
            let destination_slot = SLOTS - 1 - slot;
            let offset = destination_slot * SLOT_BYTES;
            assert_eq!(
                &landed[offset..offset + SLOT_BYTES],
                &vec![slot as u8 + 1; SLOT_BYTES][..],
                "slot {slot} must land at offset {offset} and nowhere else"
            );
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    #[ignore = "needs a server serving the LO.* commands"]
    async fn a_missing_key_transfers_nothing() {
        let mut client = dma_client().await;
        let mut buffer = client
            .register_rdma_region(vec![0u8; 4096])
            .expect("registering");

        let absent = format!("glide-rdma-absent-{}", std::process::id());
        let receipt = client
            .rdma_get(absent.as_bytes(), &mut buffer, 0, 4096)
            .await
            .expect("LO.GET against a missing key is not an error");
        assert!(receipt.is_none(), "a missing key must transfer nothing");
    }

    /// A client without RDMA, for arranging the server around a transfer.
    async fn plain_client() -> Client {
        let (host, port) = server();
        Client::new(
            ConnectionRequest {
                addresses: vec![NodeAddress { host, port }],
                ..Default::default()
            },
            None,
        )
        .await
        .expect("plain client")
    }

    async fn run(client: &mut Client, args: &[&str]) {
        let mut command = redis::cmd(args[0]);
        for arg in &args[1..] {
            command.arg(*arg);
        }
        client
            .send_command(&mut command, None)
            .await
            .unwrap_or_else(|err| panic!("{args:?}: {err}"));
    }

    fn is_cancellation(error: &redis::RedisError) -> bool {
        error.to_string().contains("RDMA transfer cancelled")
    }

    /// The hung-server case. `CLIENT PAUSE` holds every command without answering
    /// it, so the transfer can only end with a cancellation because the client was
    /// closed: had the pause ended it, the server's reply would have come back
    /// instead. The pause is left to lapse rather than lifted, because the server
    /// holds `CLIENT UNPAUSE` too.
    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    #[ignore = "needs a server serving the LO.* commands"]
    async fn closing_the_client_cancels_a_transfer_the_server_never_answers() {
        let key = format!("glide-rdma-paused-{}", std::process::id());
        let mut pauser = plain_client().await;
        let client = dma_client().await;
        let mut buffer = client
            .register_rdma_region(vec![0u8; 4096])
            .expect("registering");
        // Open the session first, so the transfer below is held at LO.GET itself.
        let mut warming = client.clone();
        warming
            .rdma_get(key.as_bytes(), &mut buffer, 0, 4096)
            .await
            .expect("LO.GET");

        let pause = std::time::Duration::from_secs(5);
        let paused_at = std::time::Instant::now();
        run(&mut pauser, &["CLIENT", "PAUSE", "5000", "ALL"]).await;
        let mut transferring = client.clone();
        let transfer = tokio::spawn(async move {
            let outcome = transferring
                .rdma_get(key.as_bytes(), &mut buffer, 0, 4096)
                .await;
            (outcome.map(|receipt| receipt.is_some()), buffer)
        });
        // Give the transfer time to reach the server, so it is the held command
        // that is cancelled. A close that came first would still cancel it.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        client.close_rdma().expect("every region closes");
        let finished = tokio::time::timeout(std::time::Duration::from_secs(30), transfer).await;
        // Let the pause lapse, so the tests after this one can connect.
        tokio::time::sleep(
            pause.saturating_sub(paused_at.elapsed()) + std::time::Duration::from_millis(200),
        )
        .await;

        let (outcome, buffer) = finished.expect("closing the client ends the wait").unwrap();
        let error = outcome.expect_err("a cancelled transfer is an error");
        assert!(is_cancellation(&error), "{error}");
        assert!(buffer.is_revoked());
    }
}
