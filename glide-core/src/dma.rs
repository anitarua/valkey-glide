// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! Direct memory access configuration and error types.
//!
//! DMA is opt-in at build time. Without the `dma` feature `glide-dma` is not
//! linked at all.

#[cfg(feature = "dma")]
pub use glide_dma::{FabricConfig, Provider};

/// How to open the local fabric endpoint and how much memory to pin for it.
#[cfg(feature = "dma")]
#[derive(Debug, Clone)]
pub struct DmaConfig {
    /// The fabric to open.
    pub fabric: FabricConfig,
    /// DMA-capable connections. Each slot pins `buffer_size` bytes against
    /// `RLIMIT_MEMLOCK`, so this is a resource decision, not a tuning knob.
    pub slots: u32,
    /// Bytes pinned per slot.
    pub buffer_size: u64,
}

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
    Configured(DmaConfig),
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

    /// The fabric refused the configuration.
    #[cfg(feature = "dma")]
    #[error("{0}")]
    Fabric(#[from] glide_dma::DmaError),
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
    match setting {
        DmaSetting::Absent => Ok(()),
        DmaSetting::Rejected(reason) => Err(reason.clone()),
        DmaSetting::Configured(config) => {
            glide_dma::DmaFabric::open(&config.fabric)?;
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn not_compiled_in_names_the_remedy() {
        let setting = DmaSetting::Rejected(DmaUnavailable::NotCompiledIn);
        let error = validate(&setting).unwrap_err();
        assert!(error.to_string().contains("DMA-capable build"), "{error}");
    }

    #[cfg(feature = "dma")]
    #[test]
    fn a_tcp_config_opens_a_fabric() {
        let setting = DmaSetting::Configured(DmaConfig {
            fabric: FabricConfig::new(Provider::Tcp),
            slots: 1,
            buffer_size: 1 << 20,
        });
        assert!(validate(&setting).is_ok());
    }

    #[cfg(feature = "dma")]
    #[test]
    fn a_fabric_failure_passes_through_with_its_detail() {
        let setting = DmaSetting::Configured(DmaConfig {
            fabric: FabricConfig::new(Provider::Tcp).with_interface("definitely-not-a-card"),
            slots: 1,
            buffer_size: 1 << 20,
        });
        let error = validate(&setting).unwrap_err();
        assert!(matches!(error, DmaUnavailable::Fabric(_)), "{error:?}");
        assert!(
            error.to_string().contains("definitely-not-a-card"),
            "the requested interface must survive: {error}"
        );
    }
}
