// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! Direct memory access configuration and error types.
//!
//! DMA is opt-in at build time. Without the `dma` feature `glide-dma` is not
//! linked at all.

#[cfg(feature = "dma")]
pub use glide_dma::{FabricConfig, Provider};

/// What a connection request asked of DMA and whether it can be honored.
#[derive(Debug, Clone, Default)]
pub enum DmaSetting {
    /// No DMA was requested. The zero-cost default.
    #[default]
    Absent,
    /// DMA was requested but cannot be honored, for this reason.
    Rejected(DmaUnavailable),
    /// DMA was requested and the configuration is usable.
    #[cfg(feature = "dma")]
    Configured(FabricConfig),
}

/// The reason a requested DMA configuration cannot be honored.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DmaUnavailable {
    /// This build artifact has no DMA support compiled in but the configuration is valid.
    #[error(
        "this build of GLIDE has no DMA support compiled in; \
         install a DMA-capable build to use DmaConfiguration"
    )]
    NotCompiledIn,

    /// No provider was named.
    #[error("DmaConfiguration requires a provider; none was set")]
    NoProviderConfigured,

    /// A node did not recognise the DMA commands, so the server module is not
    /// loaded there.
    #[error("node {node} does not have the valkey-dma module loaded")]
    ServerModuleMissing {
        /// The node that answered without the module.
        node: String,
    },

    /// Client and server are on different fabric providers, so no transfer
    /// between them can succeed.
    #[error("provider mismatch with node {node}: client {client}, server {server}")]
    ProviderMismatch {
        /// The provider this client opened.
        client: String,
        /// The provider the node reported through `DMA.INFO`.
        server: String,
        /// The node that disagreed.
        node: String,
    },

    /// Client and server disagree on what `remote_address` means.
    #[error(
        "addressing mismatch with node {node}: \
         client uses_virtual_addressing={client_virt_addr}, \
         server uses_virtual_addressing={server_virt_addr}"
    )]
    AddressingMismatch {
        /// Whether this client addresses by virtual address.
        client_virt_addr: bool,
        /// Whether the node addresses by virtual address.
        server_virt_addr: bool,
        /// The node that disagreed.
        node: String,
    },

    /// The handshake reply could not be read.
    #[error("node {node} returned a malformed DMA.HELLO reply: {reason}")]
    MalformedHandshake {
        /// The node that answered.
        node: String,
        /// What was wrong with the reply.
        reason: String,
    },

    /// A node's fabric addresses could not be inserted into the local address
    /// vector, so the server could never reach this client's memory.
    #[cfg(feature = "dma")]
    #[error("failed to insert fabric addresses for node {node}: {source}")]
    PeerInsertFailed {
        /// The node whose addresses were rejected.
        node: String,
        /// The fabric error underneath.
        source: glide_dma::DmaError,
    },

    /// The fabric refused the configuration.
    #[cfg(feature = "dma")]
    #[error("{0}")]
    Fabric(#[from] glide_dma::DmaError),
}

impl DmaSetting {
    /// Whether the request asked for DMA at all
    pub fn is_requested(&self) -> bool {
        !matches!(self, DmaSetting::Absent)
    }

    /// A fragment for the connection log, empty when DMA was not requested.
    pub fn log_summary(&self) -> String {
        match self {
            DmaSetting::Absent => String::new(),
            DmaSetting::Rejected(reason) => format!("\nDMA: unavailable ({reason})"),
            #[cfg(feature = "dma")]
            DmaSetting::Configured(config) => {
                let interface = config
                    .interface()
                    .map(|interface| format!(", interface: {interface}"))
                    .unwrap_or_default();
                let bind = config
                    .bind()
                    .map(|bind| format!(", bind: {bind}"))
                    .unwrap_or_default();
                format!("\nDMA: {:?}{interface}{bind}", config.provider())
            }
        }
    }
}

/// Check that a requested DMA configuration can be honored before connecting.
#[cfg(not(feature = "dma"))]
pub fn validate(setting: &DmaSetting) -> Result<(), DmaUnavailable> {
    match setting {
        DmaSetting::Absent => Ok(()),
        DmaSetting::Rejected(reason) => Err(reason.clone()),
    }
}

/// Check that a requested DMA configuration can be honored before connecting.
#[cfg(feature = "dma")]
pub fn validate(setting: &DmaSetting) -> Result<(), DmaUnavailable> {
    open(setting).map(|_| ())
}

/// Open the local fabric endpoint, if one was requested.
///
/// `Ok(None)` means DMA was not requested. The returned fabric must be retained
/// for the client's lifetime: dropping it tears down the endpoint the server
/// RMAs into.
#[cfg(feature = "dma")]
pub fn open(setting: &DmaSetting) -> Result<Option<glide_dma::DmaFabric>, DmaUnavailable> {
    match setting {
        DmaSetting::Absent => Ok(None),
        DmaSetting::Rejected(reason) => Err(reason.clone()),
        DmaSetting::Configured(config) => Ok(Some(glide_dma::DmaFabric::open(config)?)),
    }
}

/// Check that a node's fabric is compatible with this client's.
/// Checks for provider and addressing mismatches.
#[cfg(feature = "dma")]
pub fn check_compatibility(
    fabric: &glide_dma::DmaFabric,
    provider: glide_dma::Provider,
    node: &str,
    info: &glide_dma::DmaInfo,
) -> Result<(), DmaUnavailable> {
    let reported = info
        .provider()
        .ok_or_else(|| DmaUnavailable::MalformedHandshake {
            node: node.to_string(),
            reason: "DMA.INFO reported no provider".to_string(),
        })?;
    if !provider.matches_reported(reported) {
        return Err(DmaUnavailable::ProviderMismatch {
            client: format!("{provider:?}"),
            server: reported.to_string(),
            node: node.to_string(),
        });
    }

    let server_virt_addr =
        info.uses_virtual_addressing()
            .ok_or_else(|| DmaUnavailable::MalformedHandshake {
                node: node.to_string(),
                reason: "DMA.INFO did not report uses_virtual_addressing".to_string(),
            })?;
    let client_virt_addr = fabric.uses_virtual_addressing();
    if server_virt_addr != client_virt_addr {
        return Err(DmaUnavailable::AddressingMismatch {
            client_virt_addr,
            server_virt_addr,
            node: node.to_string(),
        });
    }

    Ok(())
}

/// Insert every address a node advertised so it can reach this client's memory.
#[cfg(feature = "dma")]
pub fn insert_peers(
    fabric: &glide_dma::DmaFabric,
    node: &str,
    addresses: &[Vec<u8>],
) -> Result<(), DmaUnavailable> {
    for address in addresses {
        fabric
            .insert_peer(address)
            .map_err(|source| DmaUnavailable::PeerInsertFailed {
                node: node.to_string(),
                source,
            })?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(feature = "dma")]
    fn info_reply(lines: &[&str]) -> glide_dma::DmaInfo {
        let reply = redis::Value::Array(
            lines
                .iter()
                .map(|line| redis::Value::BulkString(line.as_bytes().to_vec().into()))
                .collect(),
        );
        glide_dma::parse_info(reply).expect("test replies parse")
    }

    #[test]
    fn an_absent_setting_is_accepted() {
        assert!(validate(&DmaSetting::Absent).is_ok());
    }

    #[test]
    fn a_rejected_setting_fails_construction() {
        let setting = DmaSetting::Rejected(DmaUnavailable::NoProviderConfigured);
        assert_eq!(
            validate(&setting).unwrap_err(),
            DmaUnavailable::NoProviderConfigured
        );
    }

    #[test]
    fn an_absent_setting_logs_nothing() {
        assert!(DmaSetting::Absent.log_summary().is_empty());
    }

    #[cfg(feature = "dma")]
    #[test]
    fn a_configured_setting_logs_its_provider() {
        let setting =
            DmaSetting::Configured(FabricConfig::new(Provider::Tcp).with_interface("lo0"));
        let summary = setting.log_summary();
        assert!(summary.contains("Tcp"), "{summary}");
        assert!(summary.contains("lo0"), "{summary}");
    }

    #[cfg(feature = "dma")]
    #[test]
    fn unset_fabric_options_are_omitted_from_the_log() {
        let setting = DmaSetting::Configured(FabricConfig::new(Provider::EfaDirect));
        let summary = setting.log_summary();
        assert!(!summary.contains("interface"), "{summary}");
        assert!(!summary.contains("bind"), "{summary}");
    }

    #[test]
    fn not_compiled_in_names_the_remedy() {
        let setting = DmaSetting::Rejected(DmaUnavailable::NotCompiledIn);
        let error = validate(&setting).unwrap_err();
        assert!(error.to_string().contains("DMA-capable build"), "{error}");
    }

    #[cfg(feature = "dma")]
    #[test]
    fn a_tcp_config_opens_a_fabric() {
        let setting = DmaSetting::Configured(FabricConfig::new(Provider::Tcp));
        assert!(validate(&setting).is_ok());
    }

    #[cfg(feature = "dma")]
    #[test]
    fn a_matching_server_passes_the_compatibility_check() {
        let fabric =
            glide_dma::DmaFabric::open(&FabricConfig::new(Provider::Tcp)).expect("tcp opens");
        let info = info_reply(&[
            "provider: tcp",
            &format!(
                "uses_virtual_addressing: {}",
                fabric.uses_virtual_addressing()
            ),
        ]);
        assert!(check_compatibility(&fabric, Provider::Tcp, "node:1", &info).is_ok());
    }

    #[cfg(feature = "dma")]
    #[test]
    fn mismatched_providers_are_rejected() {
        let fabric =
            glide_dma::DmaFabric::open(&FabricConfig::new(Provider::Tcp)).expect("tcp opens");
        let info = info_reply(&["provider: efa-direct", "uses_virtual_addressing: true"]);

        let error = check_compatibility(&fabric, Provider::Tcp, "node:1", &info).unwrap_err();
        assert!(
            matches!(error, DmaUnavailable::ProviderMismatch { .. }),
            "{error:?}"
        );
        assert!(error.to_string().contains("efa-direct"), "{error}");
        assert!(error.to_string().contains("node:1"), "{error}");
    }

    #[cfg(feature = "dma")]
    #[test]
    fn an_addressing_disagreement_is_rejected() {
        let fabric =
            glide_dma::DmaFabric::open(&FabricConfig::new(Provider::Tcp)).expect("tcp opens");
        let flipped = !fabric.uses_virtual_addressing();
        let info = info_reply(&[
            "provider: tcp",
            &format!("uses_virtual_addressing: {flipped}"),
        ]);

        let error = check_compatibility(&fabric, Provider::Tcp, "node:1", &info).unwrap_err();
        assert!(
            matches!(error, DmaUnavailable::AddressingMismatch { .. }),
            "{error:?}"
        );
    }

    /// "Could not verify" must mean "do not proceed".
    #[cfg(feature = "dma")]
    #[test]
    fn an_unreported_attribute_fails_closed() {
        let fabric =
            glide_dma::DmaFabric::open(&FabricConfig::new(Provider::Tcp)).expect("tcp opens");

        for reply in [
            info_reply(&["provider: tcp"]),
            info_reply(&["uses_virtual_addressing: false"]),
        ] {
            let error = check_compatibility(&fabric, Provider::Tcp, "node:1", &reply).unwrap_err();
            assert!(
                matches!(error, DmaUnavailable::MalformedHandshake { .. }),
                "{error:?}"
            );
        }
    }

    #[cfg(feature = "dma")]
    #[test]
    fn a_fabric_failure_passes_through_with_its_detail() {
        let setting = DmaSetting::Configured(
            FabricConfig::new(Provider::Tcp).with_interface("definitely-not-a-card"),
        );
        let error = validate(&setting).unwrap_err();
        assert!(matches!(error, DmaUnavailable::Fabric(_)), "{error:?}");
        assert!(
            error.to_string().contains("definitely-not-a-card"),
            "the requested interface must survive: {error}"
        );
    }
}
