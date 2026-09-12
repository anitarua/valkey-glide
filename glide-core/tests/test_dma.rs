// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! DMA integration tests that need a live server but no fabric hardware.
//! Requires the `dma` feature, which needs libfabric on the build host.

#![cfg(feature = "dma")]

mod constants;
mod utilities;

#[cfg(test)]
mod dma_tests {
    use crate::utilities::*;
    use glide_core::client::{Client, ConnectionError, NodeAddress};
    use glide_core::dma::{DmaConfig, DmaSetting, DmaUnavailable, FabricConfig, Provider};

    fn extract_port(addr: &redis::ConnectionAddr) -> u16 {
        match addr {
            redis::ConnectionAddr::Tcp(_, port) => *port,
            redis::ConnectionAddr::TcpTls { port, .. } => *port,
            other => panic!("unexpected address type: {other:?}"),
        }
    }

    #[tokio::test]
    async fn server_without_the_module_is_reported_at_construction() {
        let server = RedisServer::new(ServerType::Tcp { tls: false });
        let port = extract_port(&server.get_client_addr());
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let request = glide_core::client::ConnectionRequest {
            addresses: vec![NodeAddress {
                host: "127.0.0.1".to_string(),
                port,
            }],
            dma: DmaSetting::Configured(DmaConfig {
                // tcp is the development provider: it opens a real endpoint
                // without EFA hardware, which is all this test needs.
                fabric: FabricConfig::new(Provider::Tcp),
                slots: 1,
                buffer_size: 1 << 20,
            }),
            ..Default::default()
        };

        let error = match Client::new(request, None).await {
            Ok(_) => panic!("a server without the vdma module must not yield a DMA client"),
            Err(error) => error,
        };

        assert!(
            matches!(
                error,
                ConnectionError::Dma(DmaUnavailable::ServerModuleMissing { .. })
            ),
            "expected ServerModuleMissing, got {error:?}"
        );
        assert!(
            error.to_string().contains(&port.to_string()),
            "the message must name the node: {error}"
        );
    }

    #[tokio::test]
    async fn a_plain_client_against_the_same_server_still_connects() {
        let server = RedisServer::new(ServerType::Tcp { tls: false });
        let port = extract_port(&server.get_client_addr());
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        let request = glide_core::client::ConnectionRequest {
            addresses: vec![NodeAddress {
                host: "127.0.0.1".to_string(),
                port,
            }],
            ..Default::default()
        };

        assert!(
            Client::new(request, None).await.is_ok(),
            "a client without DMA must be unaffected"
        );
    }
}
