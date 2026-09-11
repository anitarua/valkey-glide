// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

//! Registered memory and the windows a transfer advertises.

use std::fmt;
use std::sync::Arc;

use crate::advertisement::Advertisement;
use crate::endpoint::Registration;
use crate::fabric::DmaFabric;

/// A window of a registration: where it is and how big it is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RegionWindow {
    advertisement: Advertisement,
    length: usize,
}

impl RegionWindow {
    /// Where the window starts.
    pub fn advertisement(&self) -> &Advertisement {
        &self.advertisement
    }

    /// How many bytes of the region it covers.
    pub fn length(&self) -> usize {
        self.length
    }
}

/// What backs a buffer's bytes.
enum Backing {
    /// Host memory owned here and writable, so a transfer can land in it.
    Owned(Box<dyn AsMut<[u8]> + Send>),
    /// Host memory shared with the caller and read-only, so it can back a buffer on
    /// several endpoints at once.
    Shared(#[allow(dead_code)] Arc<dyn AsRef<[u8]> + Send + Sync>),
}

impl Backing {
    fn describe(&self) -> &'static str {
        match self {
            Self::Owned(_) => "owned",
            Self::Shared(_) => "shared",
        }
    }
}

/// Memory registered with a [`DmaFabric`] for the server to RMA against.
///
/// Owned host memory is reachable through [`Self::as_host_mut`]. Shared memory is
/// read-only and a source only: the server may read windows of it for `DMA.SET`, never
/// write to it.
pub struct DmaBuffer {
    /// Declared first so the region deregisters before the domain that owns it closes.
    registration: Registration,
    advertisement: Advertisement,
    length: usize,
    backing: Backing,
    /// Keeps the domain alive for as long as the registration.
    fabric: DmaFabric,
}

impl DmaBuffer {
    pub(crate) fn host(
        registration: Registration,
        advertisement: Advertisement,
        length: usize,
        memory: Box<dyn AsMut<[u8]> + Send>,
        fabric: DmaFabric,
    ) -> Self {
        Self {
            registration,
            advertisement,
            length,
            backing: Backing::Owned(memory),
            fabric,
        }
    }

    /// A read-only source registered on this fabric. The `Arc` keeps the storage mapped
    /// for as long as this registration lives, which is what lets one allocation back a
    /// buffer on every endpoint at once.
    pub(crate) fn shared(
        registration: Registration,
        advertisement: Advertisement,
        length: usize,
        memory: Arc<dyn AsRef<[u8]> + Send + Sync>,
        fabric: DmaFabric,
    ) -> Self {
        Self {
            registration,
            advertisement,
            length,
            backing: Backing::Shared(memory),
            fabric,
        }
    }

    /// Bytes registered, capping what one transfer can move through this buffer.
    pub fn capacity(&self) -> usize {
        self.length
    }

    /// Where this buffer lives, for a transfer command.
    pub fn advertisement(&self) -> &Advertisement {
        &self.advertisement
    }

    /// The advertisement for `[at, at + length)` of this registration, or `None` if
    /// that runs past the end.
    ///
    /// One registration serves many transfers by advertising a different window per
    /// request, so a pool registered once can source every `DMA.SET` with no staging
    /// copy. The remote key covers the whole region, so only the address moves - and it
    /// moves the same way under both addressing modes: a `FI_MR_VIRT_ADDR` provider
    /// carries the buffer's virtual address, making this `base + at`, while an
    /// offset-addressed provider carries 0 for the start of the region, making it `at`.
    ///
    /// Note this is a pure bounds check. It does not record that a window is
    /// outstanding, so two callers can be handed overlapping windows and both will
    /// validate. Tracking ownership of a window is the caller's problem, and in GLIDE
    /// that means `glide-core`'s, where every language binding inherits it.
    pub fn slice(&self, at: usize, length: usize) -> Option<RegionWindow> {
        if self.length < at.checked_add(length)? {
            return None;
        }
        Some(RegionWindow {
            advertisement: Advertisement {
                address: self.advertisement.address.clone(),
                remote_key: self.advertisement.remote_key,
                remote_address: self
                    .advertisement
                    .remote_address
                    .checked_add(u64::try_from(at).ok()?)?,
            },
            length,
        })
    }

    /// The fabric this buffer is registered with.
    pub fn fabric(&self) -> &DmaFabric {
        &self.fabric
    }

    /// The host mapping, or `None` for a shared source, which is not writable through
    /// this handle.
    pub fn as_host_mut(&mut self) -> Option<&mut [u8]> {
        match &mut self.backing {
            Backing::Owned(memory) => Some((**memory).as_mut()),
            Backing::Shared(_) => None,
        }
    }

    /// The host mapping for reading what a transfer landed, or `None` when there is
    /// none to read.
    pub fn as_host(&mut self) -> Option<&[u8]> {
        self.as_host_mut().map(|memory| &*memory)
    }

    /// Copy `value` into the front of the buffer, returning the bytes staged, or `None`
    /// without staging when there is no writable host mapping or it will not fit.
    pub fn copy_from(&mut self, value: &[u8]) -> Option<usize> {
        let destination = self.as_host_mut()?.get_mut(..value.len())?;
        destination.copy_from_slice(value);
        Some(value.len())
    }
}

impl fmt::Debug for DmaBuffer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DmaBuffer")
            .field("length", &self.length)
            .field("backing", &self.backing.describe())
            .field("advertisement", &self.advertisement)
            .field("registration", &self.registration)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{FabricConfig, Provider};
    use crate::fabric::DmaFabric;
    use std::sync::Arc;

    fn fabric() -> DmaFabric {
        DmaFabric::open(&FabricConfig::new(Provider::Tcp)).expect("tcp should open")
    }

    #[test]
    fn a_window_advances_the_remote_address_by_its_offset() {
        let buffer = fabric().register(vec![0u8; 4096]).unwrap();
        let base = buffer.advertisement().remote_address;
        let window = buffer.slice(1024, 256).expect("window should fit");
        assert_eq!(window.length(), 256);
        assert_eq!(window.advertisement().remote_address, base + 1024);
        // The key covers the whole region, so only the address moves.
        assert_eq!(
            window.advertisement().remote_key,
            buffer.advertisement().remote_key
        );
    }

    #[test]
    fn a_window_past_the_end_is_refused() {
        let buffer = fabric().register(vec![0u8; 1024]).unwrap();
        assert!(buffer.slice(0, 1025).is_none());
        assert!(buffer.slice(1024, 1).is_none());
        assert!(buffer.slice(1024, 0).is_some(), "empty window at the end");
        assert!(buffer.slice(usize::MAX, 1).is_none(), "offset overflow");
    }

    #[test]
    fn overlapping_windows_both_validate() {
        let buffer = fabric().register(vec![0u8; 4096]).unwrap();
        assert!(buffer.slice(0, 2048).is_some());
        assert!(buffer.slice(1024, 2048).is_some());
    }

    #[test]
    fn owned_memory_is_readable_and_writable() {
        let mut buffer = fabric().register(vec![0u8; 64]).unwrap();
        assert_eq!(buffer.copy_from(b"hello"), Some(5));
        assert_eq!(&buffer.as_host().unwrap()[..5], b"hello");
    }

    #[test]
    fn staging_more_than_fits_writes_nothing() {
        let mut buffer = fabric().register(vec![0u8; 4]).unwrap();
        assert_eq!(buffer.copy_from(b"too long"), None);
        assert_eq!(buffer.as_host().unwrap(), &[0u8; 4]);
    }

    #[test]
    fn shared_memory_has_no_writable_mapping() {
        let mut buffer = fabric().register_shared(Arc::new(vec![1u8; 128])).unwrap();
        assert!(buffer.as_host_mut().is_none());
        assert!(buffer.copy_from(b"nope").is_none());
    }
}
