// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! RDMA integration tests that need a live server but no fabric hardware.
//!
//! Requires the `rdma` feature. Needs libfabric since these tests open a fabric,
//! but over the `tcp` provider rather than a real card.

#![cfg(feature = "rdma")]

mod constants;
mod utilities;

#[cfg(test)]
mod dma_tests {
    use crate::utilities::mocks::{Mock, ServerMock};
    use crate::utilities::*;
    use glide_core::client::{Client, ConnectionError, NodeAddress};
    use glide_core::rdma::{FabricConfig, Provider, RdmaSetting};
    use redis::FromRedisValue;
    use redis::cluster_routing::{RoutingInfo, SingleNodeRoutingInfo};
    use redis::{ConnectionAddr, Value};
    use std::collections::HashMap;

    fn extract_port(addr: &redis::ConnectionAddr) -> u16 {
        match addr {
            redis::ConnectionAddr::Tcp(_, port) => *port,
            redis::ConnectionAddr::TcpTls { port, .. } => *port,
            other => panic!("unexpected address type: {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_server_without_the_module_still_serves_ordinary_commands() {
        let server = RedisServer::new(ServerType::Tcp { tls: false });
        let port = extract_port(&server.get_client_addr());
        wait_for_server_to_become_ready(&server.get_client_addr()).await;

        let request = glide_core::client::ConnectionRequest {
            addresses: vec![NodeAddress {
                host: "127.0.0.1".to_string(),
                port,
            }],
            // tcp is the development provider: it opens a real endpoint
            // without EFA hardware, which is all this test needs.
            rdma: RdmaSetting::Configured(FabricConfig::new(Provider::Tcp)),
            ..Default::default()
        };

        // Asking for RDMA is asking for a faster path to some keys, not for a
        // different client. A server that cannot serve it is still a server.
        let mut client = Client::new(request, None)
            .await
            .expect("a server without the module still yields a working client");

        let mut set = redis::cmd("SET");
        set.arg("plain-key").arg("plain-value");
        client
            .send_command(&mut set, None)
            .await
            .expect("SET works on a client whose handshake was refused");

        let mut get = redis::cmd("GET");
        get.arg("plain-key");
        let reply = client
            .send_command(&mut get, None)
            .await
            .expect("GET works");
        assert_eq!(
            String::from_owned_redis_value(reply).expect("GET returns text"),
            "plain-value"
        );

        // What the refusal does cost is the transfers, and that is reported
        // where it happens.
        let mut buffer = client
            .register_rdma_region(vec![0u8; 4096])
            .expect("the client has a fabric, so it registers");
        let error = client
            .rdma_get(b"plain-key", &mut buffer, 0, 4096)
            .await
            .expect_err("a node that refused the handshake cannot serve a transfer");
        let message = error.to_string();
        assert!(
            message.contains("no RDMA module loaded"),
            "the transfer error must name what the server is missing: {error}"
        );
        assert!(
            message.contains(&port.to_string()),
            "and which node is missing it: {error}"
        );
    }

    /// Which nodes the handshake fans out to, for a cluster that has replicas.
    #[tokio::test]
    async fn handshake_targets_are_every_primary_and_no_replica() {
        const PRIMARIES: u16 = 3;
        const REPLICAS_EACH: u16 = 1;

        let mut basics = cluster::setup_cluster_with_replicas(
            TestConfiguration::default(),
            REPLICAS_EACH,
            PRIMARIES,
        )
        .await;

        let targets = basics.client.rdma_handshake_targets().await;

        assert_eq!(
            targets.len(),
            PRIMARIES as usize,
            "one target per shard, replicas excluded: {targets:?}"
        );

        let unique: std::collections::HashSet<&String> = targets.iter().collect();
        assert_eq!(
            unique.len(),
            targets.len(),
            "duplicate targets: {targets:?}"
        );

        // The claim that matters: every address named is a primary. A replica has
        // its own fabric addresses and never receives a transfer, so handshaking
        // one would be wasted work against a node that cannot serve LO.GET.
        for node in &targets {
            let (host, port) = node.rsplit_once(':').expect("host:port");
            let mut info = redis::cmd("INFO");
            info.arg("replication");
            let reply = basics
                .client
                .send_command(
                    &mut info,
                    Some(RoutingInfo::SingleNode(SingleNodeRoutingInfo::ByAddress {
                        host: host.to_string(),
                        port: port.parse().expect("numeric port"),
                    })),
                )
                .await
                .expect("INFO replication succeeds");
            let text = String::from_owned_redis_value(reply).expect("INFO returns text");
            assert!(
                text.contains("role:master"),
                "{node} is not a primary:\n{text}"
            );
        }
    }

    /// A standalone client has exactly one target, and it is the node RDMA
    /// commands actually reach rather than whichever address was configured.
    #[tokio::test]
    async fn handshake_targets_name_the_standalone_primary() {
        let server = RedisServer::new(ServerType::Tcp { tls: false });
        let port = extract_port(&server.get_client_addr());
        wait_for_server_to_become_ready(&server.get_client_addr()).await;

        let request = glide_core::client::ConnectionRequest {
            addresses: vec![NodeAddress {
                host: "127.0.0.1".to_string(),
                port,
            }],
            ..Default::default()
        };
        let client = Client::new(request, None).await.expect("client connects");

        let targets = client.rdma_handshake_targets().await;
        assert_eq!(targets.len(), 1, "a standalone client has one node");
        assert!(
            targets[0].ends_with(&format!(":{port}")),
            "the target must name the connected node: {targets:?}"
        );
    }

    #[tokio::test]
    async fn the_transfer_target_is_the_node_holding_the_key() {
        const PRIMARIES: u16 = 3;

        let basics =
            cluster::setup_cluster_with_replicas(TestConfiguration::default(), 0, PRIMARIES).await;

        let primaries = basics.client.rdma_handshake_targets().await;
        assert_eq!(primaries.len(), PRIMARIES as usize, "{primaries:?}");

        // Enough keys to land on more than one shard. Which key lands where is the
        // server's business, so the test asserts the spread rather than the mapping.
        let keys: Vec<String> = (0..24).map(|i| format!("target-probe-{i}")).collect();
        let mut targets = Vec::new();

        for key in &keys {
            let target = basics
                .client
                .rdma_target_node(key.as_bytes())
                .await
                .expect("a live cluster names a node for every key");
            assert!(
                primaries.contains(&target),
                "{target} is not one of the primaries {primaries:?}"
            );
            targets.push(target);
        }

        let distinct: std::collections::HashSet<&String> = targets.iter().collect();
        assert!(
            distinct.len() > 1,
            "every key resolved to {targets:?}, so the mapping is not keyed on the slot"
        );

        for (key, target) in keys.iter().zip(&targets) {
            set_on_node(target, key)
                .await
                .unwrap_or_else(|error| panic!("{target} should hold {key}: {error}"));

            let elsewhere = primaries
                .iter()
                .find(|primary| *primary != target)
                .expect("a multi-shard cluster has another primary");
            let error = match set_on_node(elsewhere, key).await {
                Ok(_) => panic!("{elsewhere} answered for {key}, which {target} holds"),
                Err(error) => error,
            };
            assert_eq!(
                error.kind(),
                redis::ErrorKind::Moved,
                "expected MOVED from the wrong node, got {error:?}"
            );
        }
    }

    /// Run `SET key key` straight at one node, so a MOVED reply is what the caller
    /// gets back instead of being followed.
    async fn set_on_node(node: &str, key: &str) -> Result<redis::Value, redis::RedisError> {
        let (host, port) = node.rsplit_once(':').expect("host:port");
        let client = redis::Client::open(redis::ConnectionInfo {
            addr: redis::ConnectionAddr::Tcp(host.to_string(), port.parse().expect("numeric port")),
            redis: redis::RedisConnectionInfo::default(),
        })?;
        let mut connection = client
            .get_multiplexed_async_connection(redis::GlideConnectionOptions::default())
            .await?;
        let mut set = redis::cmd("SET");
        set.arg(key).arg(key);
        set.query_async(&mut connection).await
    }

    #[tokio::test]
    async fn a_standalone_client_sends_every_key_to_its_primary() {
        let server = RedisServer::new(ServerType::Tcp { tls: false });
        let port = extract_port(&server.get_client_addr());
        wait_for_server_to_become_ready(&server.get_client_addr()).await;

        let request = glide_core::client::ConnectionRequest {
            addresses: vec![NodeAddress {
                host: "127.0.0.1".to_string(),
                port,
            }],
            ..Default::default()
        };
        let client = Client::new(request, None).await.expect("client connects");

        let primary = client
            .rdma_handshake_targets()
            .await
            .pop()
            .expect("a standalone client has one node");

        for key in [b"alpha".as_slice(), b"omega".as_slice(), b"".as_slice()] {
            let target = client
                .rdma_target_node(key)
                .await
                .expect("a connected standalone client always names its primary");
            assert_eq!(
                target, primary,
                "every key belongs to the one primary, including {key:?}"
            );
        }
    }

    /// Static discovery never asks a node its role, so it cannot promise the node
    /// handshaken is a primary. Rejected before a fabric is opened.
    #[tokio::test]
    async fn static_node_discovery_is_rejected_at_construction() {
        let request = glide_core::client::ConnectionRequest {
            addresses: vec![NodeAddress {
                host: "127.0.0.1".to_string(),
                port: 1,
            }],
            node_discovery_mode: glide_core::client::NodeDiscoveryMode::Static,
            rdma: RdmaSetting::Configured(FabricConfig::new(Provider::Tcp)),
            ..Default::default()
        };

        let error = match Client::new(request, None).await {
            Ok(_) => panic!("RDMA with static discovery must not yield a client"),
            Err(error) => error,
        };

        let message = error.to_string();
        assert!(
            matches!(error, ConnectionError::Configuration(_)),
            "expected a configuration error, got {error:?}"
        );
        assert!(
            message.contains("static node discovery") && message.contains("primary"),
            "the message must name the conflict and why it matters: {message}"
        );
    }

    #[tokio::test]
    async fn a_cluster_without_the_module_still_serves_ordinary_commands() {
        const PRIMARIES: u16 = 3;

        let cluster = cluster::RedisCluster::new(false, &None, Some(PRIMARIES), Some(0));
        let ports: Vec<u16> = cluster
            .get_server_addresses()
            .iter()
            .map(extract_port)
            .collect();

        let request = glide_core::client::ConnectionRequest {
            addresses: ports
                .iter()
                .map(|port| NodeAddress {
                    host: "127.0.0.1".to_string(),
                    port: *port,
                })
                .collect(),
            cluster_mode_enabled: true,
            request_timeout: Some(10_000),
            rdma: RdmaSetting::Configured(FabricConfig::new(Provider::Tcp)),
            ..Default::default()
        };

        let mut client = Client::new(request, None)
            .await
            .expect("a cluster without the module still yields a working client");

        // Every node refused, so every connection was built without a session --
        // and the cluster is as usable as it would be with RDMA switched off.
        let mut set = redis::cmd("SET");
        set.arg("plain-key").arg("plain-value");
        client
            .send_command(&mut set, None)
            .await
            .expect("SET works across a cluster whose handshakes were all refused");

        let mut get = redis::cmd("GET");
        get.arg("plain-key");
        let reply = client
            .send_command(&mut get, None)
            .await
            .expect("GET works");
        assert_eq!(
            String::from_owned_redis_value(reply).expect("GET returns text"),
            "plain-value"
        );
    }

    #[tokio::test]
    async fn transferring_without_a_fabric_names_the_cause() {
        let server = RedisServer::new(ServerType::Tcp { tls: false });
        let port = extract_port(&server.get_client_addr());
        wait_for_server_to_become_ready(&server.get_client_addr()).await;

        let request = glide_core::client::ConnectionRequest {
            addresses: vec![NodeAddress {
                host: "127.0.0.1".to_string(),
                port,
            }],
            ..Default::default()
        };
        let mut client = Client::new(request, None)
            .await
            .expect("plain client connects");

        // A buffer cannot be made without a fabric, so borrow one from a
        // standalone tcp endpoint purely to have something to pass.
        let fabric =
            glide_rdma::RdmaFabric::open(&glide_rdma::FabricConfig::new(glide_rdma::Provider::Tcp))
                .expect("tcp fabric opens");
        let mut buffer = fabric.register(vec![0u8; 4096]).expect("registers");

        let error = client
            .rdma_get(b"key", &mut buffer, 0, 4096)
            .await
            .expect_err("a client without a fabric cannot transfer");
        assert!(error.to_string().contains("RdmaConfiguration"), "{error}");
    }

    #[tokio::test]
    async fn registering_without_a_fabric_names_the_cause() {
        let server = RedisServer::new(ServerType::Tcp { tls: false });
        let port = extract_port(&server.get_client_addr());
        wait_for_server_to_become_ready(&server.get_client_addr()).await;

        let request = glide_core::client::ConnectionRequest {
            addresses: vec![NodeAddress {
                host: "127.0.0.1".to_string(),
                port,
            }],
            ..Default::default()
        };
        let client = Client::new(request, None)
            .await
            .expect("plain client connects");

        let error = client
            .register_rdma_region(vec![0u8; 4096])
            .expect_err("a client without a fabric cannot register");
        let message = error.to_string();
        assert!(message.contains("RdmaConfiguration"), "{message}");
    }

    #[tokio::test]
    async fn a_plain_client_against_the_same_server_still_connects() {
        let server = RedisServer::new(ServerType::Tcp { tls: false });
        let port = extract_port(&server.get_client_addr());
        wait_for_server_to_become_ready(&server.get_client_addr()).await;

        let request = glide_core::client::ConnectionRequest {
            addresses: vec![NodeAddress {
                host: "127.0.0.1".to_string(),
                port,
            }],
            ..Default::default()
        };

        assert!(
            Client::new(request, None).await.is_ok(),
            "a client without RDMA must be unaffected"
        );
    }

    // ---- a node that reports no session -----------------------------------
    //
    // The server keeps a session against the connection `LO.HELLO` arrived on, so
    // a replaced connection, or a command redirected to a node this client never
    // handshook, reaches a node holding nothing for it. These drive that reply
    // from a mock, which is the only way to produce it without a live module.

    /// What a standalone client asks a node before it will use it.
    fn primary_responses() -> HashMap<String, Value> {
        HashMap::from([
            (
                "*1\r\n$4\r\nPING\r\n".to_string(),
                Value::BulkString(b"PONG".to_vec().into()),
            ),
            (
                "*2\r\n$4\r\nINFO\r\n$11\r\nREPLICATION\r\n".to_string(),
                Value::BulkString(b"role:master\r\n".to_vec().into()),
            ),
        ])
    }

    /// A `LO.HELLO` reply naming one peer and the endpoint it belongs to.
    ///
    /// The address comes from a real tcp endpoint rather than a handwritten
    /// sockaddr, because what `fi_av_insert` accepts is platform-specific -- a
    /// BSD `sockaddr_in` leads with `sin_len`, a Linux one does not. Nothing is
    /// ever sent to it; the insert is a local operation. The endpoint is returned
    /// so the caller can hold it open and keep the address its own.
    fn hello_reply() -> (glide_rdma::RdmaFabric, String) {
        let peer =
            glide_rdma::RdmaFabric::open(&glide_rdma::FabricConfig::new(glide_rdma::Provider::Tcp))
                .expect("tcp fabric opens");
        let hex = glide_rdma::encode_hex(peer.local_address());
        let reply = format!("*1\r\n${}\r\n{hex}\r\n", hex.len());
        (peer, reply)
    }

    fn mock_address(mock: &ServerMock) -> NodeAddress {
        match mock
            .get_addresses()
            .first()
            .expect("the mock has an address")
        {
            ConnectionAddr::Tcp(host, port) => NodeAddress {
                host: host.clone(),
                port: *port,
            },
            other => panic!("unexpected mock address: {other:?}"),
        }
    }

    async fn rdma_client_against(mock: &ServerMock) -> Client {
        let request = glide_core::client::ConnectionRequest {
            addresses: vec![mock_address(mock)],
            rdma: RdmaSetting::Configured(FabricConfig::new(Provider::Tcp)),
            ..Default::default()
        };
        Client::new(request, None)
            .await
            .expect("the mock answers LO.HELLO, so the client connects")
    }

    /// A byte count, which is what a read reply carries.
    const READ_REPLY: &str = ":8\r\n";

    /// What every replica answers, because the module registers `LO.HELLO` as a
    /// write command.
    const READONLY: &str = "-READONLY You can't write against a read only replica.\r\n";

    #[tokio::test]
    async fn the_session_is_opened_by_the_first_transfer_and_only_once() {
        let (_peer, hello) = hello_reply();
        let scripts = HashMap::from([
            // A second handshake would take this reply and fail the transfer
            // that sent it, so the test cannot pass if one is ever sent.
            (
                "LO.HELLO".to_string(),
                vec![hello, "-ERR a second LO.HELLO\r\n".to_string()],
            ),
            ("LO.GET".to_string(), vec![READ_REPLY.to_string()]),
        ]);
        let mock = ServerMock::new_with_command_scripts(primary_responses(), scripts);

        let mut client = rdma_client_against(&mock).await;

        assert_eq!(
            client.rdma_peer_count(),
            0,
            "building a connection must not handshake: a client that never \
             transfers should cost exactly what it costs without RDMA"
        );
        assert_eq!(mock.get_number_of_received_commands(), 0);

        let mut buffer = client
            .register_rdma_region(vec![0u8; 4096])
            .expect("registers");
        for attempt in 1..=2 {
            client
                .rdma_get(b"key", &mut buffer, 0, 4096)
                .await
                .unwrap_or_else(|error| panic!("transfer {attempt} failed: {error}"));
        }

        assert_eq!(
            client.rdma_peer_count(),
            1,
            "the first transfer opened the session"
        );
        assert_eq!(
            mock.get_number_of_received_commands(),
            3,
            "one LO.HELLO for the two LO.GETs, not one each"
        );
    }

    /// A reply the mock never sends: an empty script entry writes nothing.
    const NO_REPLY: &str = "";

    /// A mock whose handshake succeeds and whose `command` is never answered.
    fn server_that_never_answers(command: &str) -> (glide_rdma::RdmaFabric, ServerMock) {
        let (peer, hello) = hello_reply();
        let scripts = HashMap::from([
            ("LO.HELLO".to_string(), vec![hello]),
            (command.to_string(), vec![NO_REPLY.to_string()]),
        ]);
        (
            peer,
            ServerMock::new_with_command_scripts(primary_responses(), scripts),
        )
    }

    /// How long a transfer is given to show it is really stuck before it is
    /// cancelled, and how long the cancellation is given to take effect.
    const STUCK: std::time::Duration = std::time::Duration::from_millis(200);
    const PROMPTLY: std::time::Duration = std::time::Duration::from_secs(5);

    /// The hung-server case: no reply is coming, so only closing the client can
    /// end the wait. It must also leave the region revoked, since that is what
    /// makes returning early safe.
    #[tokio::test]
    async fn closing_the_client_cancels_a_read_the_server_never_answers() {
        let (_peer, mock) = server_that_never_answers("LO.GET");
        let client = rdma_client_against(&mock).await;
        let mut buffer = client
            .register_rdma_region(vec![0u8; 4096])
            .expect("registers");

        let mut transferring = client.clone();
        let transfer = tokio::spawn(async move {
            let outcome = transferring.rdma_get(b"key", &mut buffer, 0, 4096).await;
            (outcome.map(|receipt| receipt.is_some()), buffer)
        });
        tokio::time::sleep(STUCK).await;
        assert!(!transfer.is_finished(), "the server never replies");

        client.close_rdma().expect("the region closes");

        let (outcome, buffer) = tokio::time::timeout(PROMPTLY, transfer)
            .await
            .expect("closing the client ends the wait")
            .unwrap();
        let cancelled = outcome.expect_err("a cancelled read is an error");
        assert_eq!(cancelled.kind(), redis::ErrorKind::ClientError);
        assert!(
            cancelled.to_string().contains("RDMA transfer cancelled"),
            "{cancelled}"
        );
        assert!(buffer.is_revoked(), "the server can no longer reach it");
    }

    #[tokio::test]
    async fn closing_the_client_cancels_a_write_the_server_never_answers() {
        let (_peer, mock) = server_that_never_answers("LO.SET");
        let mut client = rdma_client_against(&mock).await;
        let buffer = client
            .register_rdma_region(vec![1u8; 4096])
            .expect("registers");

        // The write runs here and the close elsewhere, because a write only
        // borrows its buffer and so cannot be moved to another task.
        let closer = client.clone();
        let closing = tokio::spawn(async move {
            tokio::time::sleep(STUCK).await;
            closer.close_rdma()
        });

        let cancelled =
            tokio::time::timeout(STUCK + PROMPTLY, client.rdma_set(b"key", &buffer, 0, 4096))
                .await
                .expect("closing the client ends the wait")
                .expect_err("a cancelled write is an error");
        closing.await.unwrap().expect("the region closes");
        assert!(
            cancelled.to_string().contains("RDMA transfer cancelled"),
            "{cancelled}"
        );
        assert!(buffer.is_revoked());
    }

    /// Revoking one region is the same cancellation without closing the client,
    /// and the error should say which of the two happened.
    #[tokio::test]
    async fn revoking_a_region_cancels_the_transfer_using_it() {
        let (_peer, mock) = server_that_never_answers("LO.GET");
        let client = rdma_client_against(&mock).await;
        let mut buffer = client
            .register_rdma_region(vec![0u8; 4096])
            .expect("registers");
        let revoker = buffer.revoker();

        let mut transferring = client.clone();
        let transfer = tokio::spawn(async move {
            transferring
                .rdma_get(b"key", &mut buffer, 0, 4096)
                .await
                .map(|receipt| receipt.is_some())
        });
        tokio::time::sleep(STUCK).await;

        revoker.revoke().expect("the region closes");

        let cancelled = tokio::time::timeout(PROMPTLY, transfer)
            .await
            .expect("revoking the region ends the wait")
            .unwrap()
            .expect_err("a cancelled read is an error");
        assert!(
            cancelled.to_string().contains("region was revoked"),
            "{cancelled}"
        );
    }

    /// Nothing RDMA can start after a close, but the client is otherwise intact.
    #[tokio::test]
    async fn a_closed_client_refuses_rdma_and_still_serves_other_commands() {
        let (_peer, hello) = hello_reply();
        let scripts = HashMap::from([("LO.HELLO".to_string(), vec![hello])]);
        let mock = ServerMock::new_with_command_scripts(primary_responses(), scripts);
        let mut client = rdma_client_against(&mock).await;
        let mut registered_before = client
            .register_rdma_region(vec![0u8; 4096])
            .expect("registers");

        client.close_rdma().expect("closes");
        client.close_rdma().expect("closing again is harmless");

        let refused = client
            .register_rdma_region(vec![0u8; 4096])
            .expect_err("no registration after close");
        assert!(refused.to_string().contains("closed"), "{refused}");
        let refused = client
            .rdma_get(b"key", &mut registered_before, 0, 4096)
            .await
            .expect_err("no transfer after close");
        assert!(refused.to_string().contains("closed"), "{refused}");
        assert_eq!(
            mock.get_number_of_received_commands(),
            0,
            "a refused transfer sends nothing, not even the handshake"
        );

        let pong = client
            .send_command(&mut redis::cmd("PING"), None)
            .await
            .expect("other commands are unaffected");
        assert_eq!(pong, Value::BulkString(b"PONG".to_vec().into()));
    }

    /// The promotion case, which is why a refusal cannot be remembered.
    ///
    /// A replica refuses every handshake. The connection built against it stays
    /// perfectly usable, and when that node is promoted the next transfer has to
    /// ask again on the same connection rather than having written the node off.
    #[tokio::test]
    async fn a_refused_handshake_is_tried_again_by_the_next_transfer() {
        let (_peer, hello) = hello_reply();
        let scripts = HashMap::from([
            ("LO.HELLO".to_string(), vec![READONLY.to_string(), hello]),
            ("LO.GET".to_string(), vec![READ_REPLY.to_string()]),
        ]);
        let mock = ServerMock::new_with_command_scripts(primary_responses(), scripts);

        let mut client = rdma_client_against(&mock).await;
        let mut buffer = client
            .register_rdma_region(vec![0u8; 4096])
            .expect("registers");

        let refused = client
            .rdma_get(b"key", &mut buffer, 0, 4096)
            .await
            .expect_err("a node that refuses the handshake cannot transfer");
        assert!(
            refused.to_string().contains("refused the RDMA handshake"),
            "the error must say the handshake was refused: {refused}"
        );
        assert_eq!(
            refused.kind(),
            redis::ErrorKind::ReadOnly,
            "a replica's refusal stays READONLY so a cluster client retries it"
        );
        assert_eq!(client.rdma_peer_count(), 0, "a refusal opens no session");

        // Same client, same connection -- only the node's answer changed.
        client
            .rdma_get(b"key", &mut buffer, 0, 4096)
            .await
            .expect("the next transfer asks again, and this time it is answered");

        assert_eq!(
            client.rdma_peer_count(),
            1,
            "the retry opened the session the refusal did not"
        );
    }
}
