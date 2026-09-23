// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! The multiplexed RESP connection and the RDMA session that belongs to it.
//!
//! A server records where to reach a client's memory against the connection the
//! handshake arrived on, so this pairs the connection with its RDMA session on
//! the client side as well: a session lives and dies with one RESP connection,
//! and one connection never opens two.
//!
//! The session is opened by the first transfer that needs it rather than when
//! the connection is built. A connection that never carries a transfer -- every
//! replica connection, every connection of a client that only ever sends
//! ordinary commands -- never sends `LO.HELLO` at all. That is what keeps RDMA
//! an opt-in fast path instead of something a client pays for everywhere, and
//! it is also what lets a node that was a replica when this connection was
//! built serve transfers once it is promoted.

#[cfg(not(feature = "rdma"))]
pub type GlideConnectionWithRdma = redis::aio::MultiplexedConnection;

#[cfg(feature = "rdma")]
pub use with_rdma::GlideConnectionWithRdma;

#[cfg(feature = "rdma")]
mod with_rdma {
    use std::net::{IpAddr, SocketAddr};
    use std::ops::{Deref, DerefMut};
    use std::sync::Arc;
    use std::time::Duration;

    use redis::aio::MultiplexedConnection;
    use redis::cluster_async::Connect;
    use redis::{
        Cmd, GlideConnectionOptions, IntoConnectionInfo, Pipeline, PipelineRetryStrategy,
        RedisError, RedisFuture, RedisResult, Value, aio::ConnectionLike,
    };

    use tokio::sync::OnceCell;

    /// A connection and the RDMA session opened on it.
    ///
    /// Clones share the session, so it is released when the last clone of a
    /// connection goes -- which is when the server drops its side too. They
    /// share the cell it lives in too, so whichever clone carries the first
    /// transfer opens the session for all of them.
    #[derive(Clone)]
    pub struct GlideConnectionWithRdma {
        inner: MultiplexedConnection,
        /// `None` when RDMA is not configured.
        rdma: Option<Arc<Rdma>>,
    }

    /// What a connection needs to open its session, and the session once open.
    struct Rdma {
        fabric: glide_rdma::RdmaFabric,
        /// Only for error messages: a failed handshake has to say which node.
        node: String,
        session: OnceCell<glide_rdma::RdmaSession>,
    }

    impl Rdma {
        /// Open the session, unless `command` is one that does not need one or
        /// the session is already open.
        ///
        /// The cell is left empty when a handshake fails, so a node that refuses
        /// today -- a replica, which refuses every handshake -- is asked again by
        /// the next transfer rather than written off for the connection's life.
        /// That is what recovers a replica once it is promoted to primary.
        async fn open_session_if_needed(
            &self,
            connection: &mut MultiplexedConnection,
            command: &Cmd,
        ) -> RedisResult<()> {
            if self.session.initialized() || !needs_session(command) {
                return Ok(());
            }
            self.session
                .get_or_try_init(|| handshake(connection, &self.fabric, &self.node))
                .await?;
            Ok(())
        }
    }

    /// Whether this command can only run on a connection that has a session.
    ///
    /// The name is the first argument, as redis-rs builds every command, and
    /// which names those are belongs to `glide-rdma` along with the rest of the
    /// wire format.
    fn needs_session(command: &Cmd) -> bool {
        match command.args_iter().next() {
            Some(redis::Arg::Simple(name)) => glide_rdma::needs_session(name),
            _ => false,
        }
    }

    impl GlideConnectionWithRdma {
        /// Pair a connection with the fabric its session will be opened on.
        ///
        /// Nothing is sent here. The handshake waits for a transfer, so building
        /// a connection costs exactly what it costs without RDMA, and a node
        /// that would refuse the handshake -- every replica does -- still gets a
        /// connection that carries ordinary commands like any other.
        pub fn open(
            inner: MultiplexedConnection,
            rdma_fabric: Option<glide_rdma::RdmaFabric>,
            node: &str,
        ) -> Self {
            Self {
                inner,
                rdma: rdma_fabric.map(|fabric| {
                    Arc::new(Rdma {
                        fabric,
                        node: node.to_string(),
                        session: OnceCell::new(),
                    })
                }),
            }
        }

        /// Send one command, opening this connection's RDMA session first if the
        /// command is a transfer and no session is open yet.
        ///
        /// Every single command passes through here. It shadows the inherent
        /// method of the same name on the connection underneath, which `Deref`
        /// would otherwise reach straight past this wrapper, and
        /// `ConnectionLike::req_packed_command` is routed through it as well --
        /// the standalone client uses the first, the cluster client the second.
        ///
        /// Pipelines have no such gate and need none: a transfer carries a
        /// reference to registered memory that only a client's own `rdma_get`
        /// and `rdma_set` can build, and those send a single command.
        pub async fn send_packed_command(&mut self, cmd: &Cmd) -> RedisResult<Value> {
            // Split the borrow: the session is read through `rdma` while the
            // handshake it may run needs `inner` mutably, and they are separate
            // fields.
            let Self { inner, rdma } = self;
            if let Some(rdma) = rdma.as_ref() {
                rdma.open_session_if_needed(inner, cmd).await?;
            }
            inner.send_packed_command(cmd).await
        }
    }

    /// Run `LO.HELLO` on this connection and open the session it reports.
    ///
    /// Only ever reached through the cell that holds the result, which admits
    /// one caller at a time, so a connection never sends a second `LO.HELLO`
    /// while it has a session and never sends two at once.
    async fn handshake(
        connection: &mut MultiplexedConnection,
        rdma_fabric: &glide_rdma::RdmaFabric,
        node: &str,
    ) -> RedisResult<glide_rdma::RdmaSession> {
        use crate::rdma::protocol;

        let reply = protocol::hello_command(rdma_fabric.local_address())
            .query_async(connection)
            .await
            .map_err(|error| crate::rdma::handshake_refused(node, error))?;
        let handshake = protocol::parse_hello(reply)
            .map_err(|error| crate::rdma::malformed_handshake(node, error))?;
        rdma_fabric
            .open_session(&handshake)
            .map_err(protocol::as_redis_error)
    }

    impl Deref for GlideConnectionWithRdma {
        type Target = MultiplexedConnection;

        fn deref(&self) -> &Self::Target {
            &self.inner
        }
    }

    impl DerefMut for GlideConnectionWithRdma {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.inner
        }
    }

    impl std::fmt::Debug for GlideConnectionWithRdma {
        fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter
                .debug_struct("GlideConnectionWithRdma")
                .field("inner", &self.inner)
                .field("rdma", &self.rdma.is_some())
                .field(
                    "session",
                    &self
                        .rdma
                        .as_ref()
                        .is_some_and(|rdma| rdma.session.initialized()),
                )
                .finish()
        }
    }

    impl ConnectionLike for GlideConnectionWithRdma {
        fn req_packed_command<'a>(&'a mut self, cmd: &'a Cmd) -> RedisFuture<'a, Value> {
            Box::pin(self.send_packed_command(cmd))
        }

        fn req_packed_commands<'a>(
            &'a mut self,
            cmd: &'a Pipeline,
            offset: usize,
            count: usize,
            pipeline_retry_strategy: Option<PipelineRetryStrategy>,
        ) -> RedisFuture<'a, Vec<Value>> {
            self.inner
                .req_packed_commands(cmd, offset, count, pipeline_retry_strategy)
        }

        fn get_db(&self) -> i64 {
            self.inner.get_db()
        }

        fn is_closed(&self) -> bool {
            self.inner.is_closed()
        }

        // The three below have defaults on the trait, and a wrapper that does not
        // delegate them silently answers for the connection it wraps rather than
        // from it: the default `get_az` reports no zone, which reads as "this node
        // is in no availability zone" and quietly disables affinity routing.
        fn get_az(&self) -> Option<String> {
            self.inner.get_az()
        }

        fn set_az(&mut self, az: Option<String>) {
            self.inner.set_az(az)
        }

        fn update_push_manager_node_address(&mut self, address: String) {
            self.inner.update_push_manager_node_address(address)
        }
    }

    impl Connect for GlideConnectionWithRdma {
        fn connect<'a, T>(
            info: T,
            response_timeout: Duration,
            connection_timeout: Duration,
            socket_addr: Option<SocketAddr>,
            glide_connection_options: GlideConnectionOptions,
        ) -> RedisFuture<'a, (Self, Option<IpAddr>)>
        where
            T: IntoConnectionInfo + Send + 'a,
        {
            Box::pin(async move {
                let connection_info = info.into_connection_info()?;
                let rdma_fabric = glide_connection_options.rdma_fabric.clone();
                // Taken before connecting, because the info is moved in.
                let address = connection_info.addr.clone();
                let (inner, ip) = MultiplexedConnection::connect(
                    connection_info,
                    response_timeout,
                    connection_timeout,
                    socket_addr,
                    glide_connection_options,
                )
                .await?;
                let node = address.to_string();
                Ok::<(Self, Option<IpAddr>), RedisError>((
                    Self::open(inner, rdma_fabric, &node),
                    ip,
                ))
            })
        }
    }
}
