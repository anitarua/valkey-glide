// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! Direct memory access configuration and error types.
//!
//! RDMA is opt-in at build time. Without the `rdma` feature `glide-rdma` is not
//! linked at all.
//!
//! The RESP side of the commands lives in [`protocol`], not in `glide-rdma`:
//! that crate has to be depended on by the connection layer, so it cannot
//! depend on the connection layer in turn.

#[cfg(feature = "rdma")]
pub mod protocol;

/// Read what a refused `LO.HELLO` says about the server that refused it.
///
/// A server with no module answers every RDMA command the same way, so that is
/// read here rather than left to reach a caller as "unknown command". A
/// handshake that nothing answered is passed straight back: it says nothing
/// about RDMA, and rewriting it as a configuration error would stop the retry
/// it deserves.
///
/// A replica's `READONLY` keeps its kind too, with the node added to the
/// message. In a cluster it means the slot map is out of date, for example
/// after a failover, and the cluster client only refreshes the map and retries
/// when it sees `READONLY`.
#[cfg(feature = "rdma")]
pub fn handshake_refused(node: &str, error: redis::RedisError) -> redis::RedisError {
    if !answered_by_server(&error) {
        return error;
    }
    if error.kind() == redis::ErrorKind::ReadOnly {
        let reason = RdmaUnavailable::HandshakeRefused {
            node: node.to_string(),
            reason: error.to_string(),
        };
        return redis::RedisError::from((
            redis::ErrorKind::ReadOnly,
            "An error was signalled by the server:",
            reason.to_string(),
        ));
    }
    let message = error.to_string().to_ascii_lowercase();
    let reason =
        if message.contains("unknown command") || message.contains("unknown or disabled command") {
            RdmaUnavailable::ServerModuleMissing {
                node: node.to_string(),
            }
        } else {
            RdmaUnavailable::HandshakeRefused {
                node: node.to_string(),
                reason: error.to_string(),
            }
        };
    protocol::configuration_error(reason.to_string())
}

/// A `LO.HELLO` reply this client could not read, as the error a transfer carries.
///
/// Kept apart from a refusal because it says something different: the node has
/// the module and answered, and the two disagree about the reply's shape. It
/// stays a protocol error rather than becoming a configuration one, since
/// nothing about the client's configuration would fix it.
#[cfg(feature = "rdma")]
pub fn malformed_handshake(node: &str, error: redis::RedisError) -> redis::RedisError {
    protocol::as_redis_error(glide_rdma::RdmaError::Protocol(
        RdmaUnavailable::MalformedHandshake {
            node: node.to_string(),
            reason: error.to_string(),
        }
        .to_string(),
    ))
}

/// Whether the server answered the handshake at all.
#[cfg(feature = "rdma")]
fn answered_by_server(error: &redis::RedisError) -> bool {
    !error.is_io_error()
        && !error.is_connection_dropped()
        && !error.is_timeout()
        && !matches!(
            error.kind(),
            redis::ErrorKind::IoError
                | redis::ErrorKind::BusyLoadingError
                | redis::ErrorKind::TryAgain
        )
}

#[cfg(feature = "rdma")]
pub use glide_rdma::{FabricConfig, Provider};

/// The fabric a client's connections handshake with.
///
/// A newtype rather than the fabric itself, so that the plumbing carrying it
/// from `Client::new` down to connection setup is ordinary parameters instead of
/// conditional ones, and so that `glide-rdma` stays out of signatures that have
/// no other reason to name it. Absence is `Option::None`, matching the other
/// optional things a client is built with.
#[cfg(feature = "rdma")]
#[derive(Clone, Debug)]
pub struct Fabric(glide_rdma::RdmaFabric);

#[cfg(feature = "rdma")]
impl Fabric {
    /// Wrap the fabric a client opened.
    pub fn new(opened: glide_rdma::RdmaFabric) -> Self {
        Self(opened)
    }

    /// The fabric itself, for the connection layer that advertises it.
    pub fn opened(self) -> glide_rdma::RdmaFabric {
        self.0
    }
}

/// Every region a client has registered, so that closing the client can revoke
/// them all.
///
/// Revoking a region is what cancels a transfer: the server can no longer reach
/// the memory, and a transfer waiting on the region returns at once. Closing also
/// refuses any registration or transfer that comes after it, so nothing can
/// start against a client that has been closed.
///
/// Holds only [`glide_rdma::RdmaRevoker`]s, which do not keep a region alive, so
/// a region the caller freed drops out of the list the next time one is added.
#[cfg(feature = "rdma")]
#[derive(Debug)]
pub struct Regions {
    /// A revoker for every region registered and not yet revoked.
    revokers: std::sync::Mutex<Vec<glide_rdma::RdmaRevoker>>,
    /// Set at the start of a close. Kept apart from the list so that checking it
    /// never waits on a registration, which holds the list while it pins memory.
    closed: std::sync::atomic::AtomicBool,
    /// Held for the whole of a close, so a second close waits for the first to
    /// finish revoking instead of returning while it is still under way.
    closing: std::sync::Mutex<()>,
    /// Becomes `true` once a close has tried every region, whether or not each
    /// revoke succeeded, for transfers that must not return before then.
    close_finished: tokio::sync::watch::Sender<bool>,
}

#[cfg(feature = "rdma")]
impl Default for Regions {
    fn default() -> Self {
        Self {
            revokers: Default::default(),
            closed: Default::default(),
            closing: Default::default(),
            close_finished: tokio::sync::watch::channel(false).0,
        }
    }
}

#[cfg(feature = "rdma")]
impl Regions {
    /// Run `register` and record the region it returns.
    ///
    /// The list stays locked while it runs, so a close that arrives meanwhile waits
    /// for the region and then revokes it, rather than missing it.
    pub fn register(
        &self,
        register: impl FnOnce() -> Result<glide_rdma::RdmaBuffer, glide_rdma::RdmaError>,
    ) -> Result<glide_rdma::RdmaBuffer, redis::RedisError> {
        let mut revokers = self.lock();
        // Read under the list lock, which a close also holds while it sets the
        // flag, so a registration either sees the close or is revoked by it.
        if self.is_closed() {
            return Err(protocol::cancelled(
                "the client is closed, so it cannot register memory",
            ));
        }
        let buffer = register().map_err(protocol::as_redis_error)?;
        revokers.retain(|revoker| !revoker.is_released());
        revokers.push(buffer.revoker());
        Ok(buffer)
    }

    /// Revoke every region and refuse whatever comes after.
    ///
    /// Every region is attempted even if one fails. A region that failed to close
    /// stays registered, and a transfer using it keeps waiting for its reply; it
    /// also stays on the list, so closing again tries it again. Otherwise closing
    /// again does nothing.
    ///
    /// # Errors
    ///
    /// The first failure, if libfabric could not close some region.
    pub fn close(&self) -> Result<(), redis::RedisError> {
        self.close_with(glide_rdma::RdmaRevoker::revoke)
    }

    /// [`Self::close`], revoking each region with `revoke`, so tests can make a
    /// revoke fail.
    fn close_with(
        &self,
        revoke: impl Fn(&glide_rdma::RdmaRevoker) -> Result<(), glide_rdma::RdmaError>,
    ) -> Result<(), redis::RedisError> {
        let _closing = self
            .closing
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let revokers = {
            let mut revokers = self.lock();
            self.closed
                .store(true, std::sync::atomic::Ordering::Release);
            std::mem::take(&mut *revokers)
        };
        let mut first_failure = None;
        let mut still_open = Vec::new();
        for revoker in revokers {
            if let Err(error) = revoke(&revoker) {
                first_failure.get_or_insert(protocol::as_redis_error(error));
                still_open.push(revoker);
            }
        }
        self.lock().extend(still_open);
        self.close_finished.send_replace(true);
        first_failure.map_or(Ok(()), Err)
    }

    /// Whether [`Self::close`] has been called. True from the start of a close,
    /// before its regions are revoked. Never waits on a registration.
    pub fn is_closed(&self) -> bool {
        self.closed.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Resolves once a close has tried to revoke every region.
    pub async fn close_finished(&self) {
        let mut finished = self.close_finished.subscribe();
        // The sender lives as long as `self`, so this only returns once it is true.
        let _ = finished.wait_for(|finished| *finished).await;
    }

    #[cfg(test)]
    fn tracked(&self) -> usize {
        self.lock().len()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Vec<glide_rdma::RdmaRevoker>> {
        self.revokers
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// The error a cancelled transfer returns, naming what cancelled it.
#[cfg(feature = "rdma")]
pub fn cancellation(client_closed: bool) -> redis::RedisError {
    protocol::cancelled(if client_closed {
        "the client was closed"
    } else {
        "its region was revoked"
    })
}

/// A transfer's reply, with a failure caused by cancelling it reported as the
/// cancellation.
///
/// Revoking makes the server's RMA fail, and its error reply can arrive before
/// the transfer learns of the revoke, even before the revoke has finished. It
/// means the transfer failed because it was cancelled, and the caller should
/// hear that rather than the server's account of the failed RMA. So a failure
/// counts as a cancellation once the region is revoked, or once the client has
/// started closing, which revokes it. A reply that succeeded stays a success:
/// that transfer finished before the revoke reached it.
#[cfg(feature = "rdma")]
pub fn cancelled_if_revoked(
    reply: redis::RedisResult<redis::Value>,
    region_revoked: bool,
    client_closed: bool,
) -> redis::RedisResult<redis::Value> {
    match reply {
        Err(_) if region_revoked || client_closed => Err(cancellation(client_closed)),
        reply => reply,
    }
}

/// Uninhabited without the `rdma` feature, so `Option<Fabric>` can only ever be
/// `None` there. The type still exists, which is what lets every signature that
/// carries one stay the same in both builds.
#[cfg(not(feature = "rdma"))]
#[derive(Clone, Debug)]
pub enum Fabric {}

/// What a connection request asked of RDMA and whether it can be honored.
#[derive(Debug, Clone, Default)]
pub enum RdmaSetting {
    /// No RDMA was requested. The zero-cost default.
    #[default]
    Absent,
    /// RDMA was requested but cannot be honored, for this reason.
    Rejected(RdmaUnavailable),
    /// RDMA was requested and the configuration is usable.
    #[cfg(feature = "rdma")]
    Configured(FabricConfig),
}

/// The reason a requested RDMA configuration cannot be honored.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum RdmaUnavailable {
    /// No RDMA support compiled in but the configuration is valid.
    #[error(
        "this build of GLIDE has no RDMA support compiled in; \
         it was built from source without the `rdma` feature. \
         The published packages include it."
    )]
    NotCompiledIn,

    /// No provider was named.
    #[error("RdmaConfiguration requires a provider; none was set")]
    NoProviderConfigured,

    /// A node did not recognise the RDMA commands, so whatever module serves them
    /// is not loaded there.
    #[error("node {node} has no RDMA module loaded")]
    ServerModuleMissing {
        /// The node that answered without the module.
        node: String,
    },

    /// A node answered the handshake with an error of its own. A replica does
    /// this, because the module registers `LO.HELLO` as a write command; that
    /// refusal keeps its `READONLY` kind so a cluster client retries it.
    #[error("node {node} refused the RDMA handshake: {reason}")]
    HandshakeRefused {
        /// The node that refused.
        node: String,
        /// What it said.
        reason: String,
    },

    /// The handshake reply could not be read.
    #[error("node {node} returned a malformed LO.HELLO reply: {reason}")]
    MalformedHandshake {
        /// The node that answered.
        node: String,
        /// What was wrong with the reply.
        reason: String,
    },

    /// The fabric refused the configuration.
    #[cfg(feature = "rdma")]
    #[error("{0}")]
    Fabric(#[from] glide_rdma::RdmaError),
}

impl RdmaSetting {
    /// Whether the request asked for RDMA at all
    pub fn is_requested(&self) -> bool {
        !matches!(self, RdmaSetting::Absent)
    }

    /// A fragment for the connection log, empty when RDMA was not requested.
    pub fn log_summary(&self) -> String {
        match self {
            RdmaSetting::Absent => String::new(),
            RdmaSetting::Rejected(reason) => format!("\nRDMA: unavailable ({reason})"),
            #[cfg(feature = "rdma")]
            RdmaSetting::Configured(config) => {
                let interface = config
                    .interface()
                    .map(|interface| format!(", interface: {interface}"))
                    .unwrap_or_default();
                let bind = config
                    .bind()
                    .map(|bind| format!(", bind: {bind}"))
                    .unwrap_or_default();
                format!("\nRDMA: {:?}{interface}{bind}", config.provider())
            }
        }
    }
}

/// Check that a requested RDMA configuration can be honored before connecting.
#[cfg(not(feature = "rdma"))]
pub fn validate(setting: &RdmaSetting) -> Result<(), RdmaUnavailable> {
    match setting {
        RdmaSetting::Absent => Ok(()),
        RdmaSetting::Rejected(reason) => Err(reason.clone()),
    }
}

/// Check that a requested RDMA configuration can be honored before connecting.
#[cfg(feature = "rdma")]
pub fn validate(setting: &RdmaSetting) -> Result<(), RdmaUnavailable> {
    open(setting).map(|_| ())
}

/// Open the local fabric endpoint, if one was requested.
///
/// `Ok(None)` means RDMA was not requested. The returned fabric must be retained
/// for the client's lifetime: dropping it tears down the endpoint the server
/// RMAs into.
#[cfg(feature = "rdma")]
pub fn open(setting: &RdmaSetting) -> Result<Option<glide_rdma::RdmaFabric>, RdmaUnavailable> {
    match setting {
        RdmaSetting::Absent => Ok(None),
        RdmaSetting::Rejected(reason) => Err(reason.clone()),
        RdmaSetting::Configured(config) => Ok(Some(glide_rdma::RdmaFabric::open(config)?)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "rdma")]
    fn server_error(detail: &str) -> redis::RedisError {
        redis::RedisError::from((
            redis::ErrorKind::ResponseError,
            "An error was signalled by the server",
            detail.to_string(),
        ))
    }

    #[cfg(feature = "rdma")]
    #[test]
    fn a_server_that_does_not_know_the_command_has_no_module() {
        for detail in [
            "unknown command 'LO.HELLO'",
            "unknown or disabled command 'LO.HELLO'",
        ] {
            let failure = handshake_refused("10.0.0.1:6379", server_error(detail));
            let message = failure.to_string();

            assert!(message.contains("no RDMA module loaded"), "{message}");
            assert!(message.contains("10.0.0.1:6379"), "{message}");
        }
    }

    /// Every replica answers this, because the module registers `LO.HELLO` as a
    /// write command. It reaches a caller only when a transfer is routed to one.
    #[cfg(feature = "rdma")]
    #[test]
    fn a_replica_refusal_names_the_node_and_what_it_said() {
        let failure = handshake_refused("10.0.0.1:6379", replica_refusal());
        let message = failure.to_string();

        assert!(message.contains("10.0.0.1:6379"), "{message}");
        assert!(message.contains("refused the RDMA handshake"), "{message}");
        assert!(message.contains("read only replica"), "{message}");
    }

    /// After a failover the cluster client can still route a transfer to the old
    /// primary. The refusal must stay `READONLY`, which makes the client refresh
    /// its slot map and retry, rather than become a configuration error, which
    /// it never retries.
    #[cfg(feature = "rdma")]
    #[test]
    fn a_replica_refusal_is_retried_after_a_slot_refresh() {
        let failure = handshake_refused("10.0.0.1:6379", replica_refusal());

        assert_eq!(failure.kind(), redis::ErrorKind::ReadOnly);
    }

    /// A `READONLY` reply exactly as the parser builds it from the wire.
    #[cfg(feature = "rdma")]
    fn replica_refusal() -> redis::RedisError {
        redis::parse_redis_value(b"-READONLY You can't write against a read only replica.\r\n")
            .and_then(|reply| reply.extract_error())
            .expect_err("a READONLY reply is an error")
    }

    /// A handshake nothing answered says nothing about RDMA. Rewriting one as a
    /// configuration error would mark it permanent, and nothing retries those.
    #[cfg(feature = "rdma")]
    #[test]
    fn a_handshake_that_was_never_answered_is_returned_unchanged() {
        for unanswered in [
            redis::RedisError::from(std::io::Error::from(std::io::ErrorKind::BrokenPipe)),
            redis::RedisError::from((redis::ErrorKind::IoError, "connection closed")),
            redis::RedisError::from((redis::ErrorKind::BusyLoadingError, "LOADING")),
            redis::RedisError::from((redis::ErrorKind::TryAgain, "TRYAGAIN")),
        ] {
            let kind = unanswered.kind();

            let failure = handshake_refused("10.0.0.1:6379", unanswered);

            assert_eq!(
                failure.kind(),
                kind,
                "{kind:?} must reach the caller unchanged"
            );
        }
    }

    /// A cluster has many nodes, so an unreadable reply has to say which one
    /// sent it, and must not read as something the caller could reconfigure.
    #[cfg(feature = "rdma")]
    #[test]
    fn an_unreadable_reply_names_the_node_and_stays_a_protocol_error() {
        let unreadable = protocol::as_redis_error(glide_rdma::RdmaError::Protocol(
            "lo.hello returned no addresses".to_string(),
        ));

        let failure = malformed_handshake("10.0.0.1:6379", unreadable);

        assert_eq!(failure.kind(), redis::ErrorKind::ProtocolDesync);
        let message = failure.to_string();
        assert!(message.contains("10.0.0.1:6379"), "{message}");
        assert!(message.contains("malformed LO.HELLO reply"), "{message}");
    }

    #[test]
    fn an_absent_setting_is_accepted() {
        assert!(validate(&RdmaSetting::Absent).is_ok());
    }

    #[test]
    fn a_rejected_setting_fails_construction() {
        let setting = RdmaSetting::Rejected(RdmaUnavailable::NotCompiledIn);
        assert!(validate(&setting).is_err());
    }

    #[test]
    fn an_absent_setting_logs_nothing() {
        assert_eq!(RdmaSetting::Absent.log_summary(), "");
    }

    #[test]
    fn not_compiled_in_names_the_remedy() {
        let setting = RdmaSetting::Rejected(RdmaUnavailable::NotCompiledIn);
        let error = validate(&setting).unwrap_err();
        let message = error.to_string();
        assert!(message.contains("built from source"), "{message}");
        assert!(message.contains("published packages"), "{message}");
    }

    #[cfg(feature = "rdma")]
    fn tcp_fabric() -> glide_rdma::RdmaFabric {
        glide_rdma::RdmaFabric::open(&FabricConfig::new(Provider::Tcp)).expect("tcp opens")
    }

    #[cfg(feature = "rdma")]
    #[test]
    fn closing_revokes_every_registered_region() {
        let fabric = tcp_fabric();
        let regions = Regions::default();
        let first = regions.register(|| fabric.register(vec![0u8; 64])).unwrap();
        let second = regions.register(|| fabric.register(vec![0u8; 64])).unwrap();

        regions.close().expect("both regions close");

        assert!(first.is_revoked());
        assert!(second.is_revoked());
        assert!(regions.is_closed());
        regions.close().expect("closing again is a no-op");
    }

    #[cfg(feature = "rdma")]
    #[test]
    fn a_closed_client_registers_nothing() {
        let fabric = tcp_fabric();
        let regions = Regions::default();
        regions.close().unwrap();

        let mut called = false;
        let refused = regions
            .register(|| {
                called = true;
                fabric.register(vec![0u8; 64])
            })
            .expect_err("registration after close is refused");

        assert!(!called, "no memory is pinned for a refused registration");
        assert!(refused.to_string().contains("closed"), "{refused}");
    }

    /// A long-lived client that registers and frees regions over and over must
    /// not keep a record of every one it ever had.
    #[cfg(feature = "rdma")]
    #[test]
    fn freed_regions_drop_out_of_the_list() {
        let fabric = tcp_fabric();
        let regions = Regions::default();
        for _ in 0..10 {
            drop(regions.register(|| fabric.register(vec![0u8; 64])).unwrap());
        }
        let _kept = regions.register(|| fabric.register(vec![0u8; 64])).unwrap();

        assert_eq!(regions.tracked(), 1);
    }

    #[cfg(feature = "rdma")]
    #[test]
    fn a_failed_registration_is_not_recorded() {
        let fabric = tcp_fabric();
        let regions = Regions::default();

        regions
            .register(|| fabric.register(Vec::<u8>::new()))
            .expect_err("0 bytes cannot be registered");

        assert_eq!(regions.tracked(), 0);
    }

    /// What the server sends when a revoke beat its RMA.
    #[cfg(feature = "rdma")]
    fn failed_rma() -> redis::RedisResult<redis::Value> {
        Err(server_error("EFA write"))
    }

    #[cfg(feature = "rdma")]
    #[test]
    fn a_failure_on_a_revoked_region_is_the_cancellation() {
        let closed = cancelled_if_revoked(failed_rma(), true, true).unwrap_err();
        assert!(
            closed.to_string().contains("the client was closed"),
            "{closed}"
        );
        assert_eq!(closed.kind(), redis::ErrorKind::ClientError);

        let revoked = cancelled_if_revoked(failed_rma(), true, false).unwrap_err();
        assert!(
            revoked.to_string().contains("region was revoked"),
            "{revoked}"
        );
    }

    /// The server's error can arrive while the close is still revoking.
    #[cfg(feature = "rdma")]
    #[test]
    fn a_failure_while_the_client_closes_is_the_cancellation() {
        let closing = cancelled_if_revoked(failed_rma(), false, true).unwrap_err();
        assert!(
            closing.to_string().contains("the client was closed"),
            "{closing}"
        );
    }

    #[cfg(feature = "rdma")]
    #[test]
    fn a_failure_on_a_live_region_is_the_servers_own() {
        let failure = cancelled_if_revoked(failed_rma(), false, false).unwrap_err();
        assert!(failure.to_string().contains("EFA write"), "{failure}");
    }

    /// The transfer finished before the revoke reached it, so it succeeded.
    #[cfg(feature = "rdma")]
    #[test]
    fn a_success_on_a_revoked_region_stays_a_success() {
        let reply = cancelled_if_revoked(Ok(redis::Value::Int(8)), true, true);
        assert_eq!(reply.unwrap(), redis::Value::Int(8));
    }

    #[cfg(feature = "rdma")]
    #[tokio::test]
    async fn a_close_is_finished_once_every_region_was_tried() {
        let fabric = tcp_fabric();
        let regions = Regions::default();
        let _region = regions.register(|| fabric.register(vec![0u8; 64])).unwrap();

        let waiting = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            regions.close_finished(),
        )
        .await;
        assert!(waiting.is_err(), "nothing has closed yet");

        regions.close().unwrap();

        tokio::time::timeout(std::time::Duration::from_secs(5), regions.close_finished())
            .await
            .expect("the close has finished");
    }

    /// A region that failed to close must not be forgotten: closing again is the
    /// only way left to revoke it.
    #[cfg(feature = "rdma")]
    #[test]
    fn a_region_that_failed_to_close_is_tried_again() {
        let fabric = tcp_fabric();
        let regions = Regions::default();
        let region = regions.register(|| fabric.register(vec![0u8; 64])).unwrap();

        let failed = regions.close_with(|_| {
            Err(glide_rdma::RdmaError::Fabric {
                operation: "fi_close",
                message: "Device or resource busy".to_string(),
                errno: Some(-16),
            })
        });

        assert!(failed.is_err());
        assert!(!region.is_revoked());
        assert_eq!(regions.tracked(), 1, "the failed region stays on the list");

        regions.close().expect("the second close revokes it");

        assert!(region.is_revoked());
        assert_eq!(regions.tracked(), 0);
    }

    /// A registration holds the list while it pins memory, which can take a long
    /// time. Async code asks whether the client is closed and must not wait on it.
    #[cfg(feature = "rdma")]
    #[test]
    fn asking_whether_closed_does_not_wait_on_a_registration() {
        let regions = std::sync::Arc::new(Regions::default());
        let registering = regions.lock();

        let (answered, answer) = std::sync::mpsc::channel();
        let asking = regions.clone();
        std::thread::spawn(move || answered.send(asking.is_closed()));

        let closed = answer.recv_timeout(std::time::Duration::from_secs(5));
        drop(registering);
        assert_eq!(closed, Ok(false), "answered while the list was held");
    }
}
