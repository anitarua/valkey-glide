// Copyright Valkey GLIDE Project Contributors - SPDX Identifier: Apache-2.0

use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use crate::advertisement::Advertisement;
use crate::buffer::DmaBuffer;
use crate::config::FabricConfig;
use crate::endpoint::{LibfabricEndpoint, Registration, domain_names, query_info};
use crate::error::DmaError;
use crate::progress::{ProgressDriver, ProgressGuard};

/// A local domain. Every [`DmaBuffer`] holds a clone, keeping
/// the domain alive for as long as any registration.
#[derive(Clone, Debug)]
pub struct DmaFabric {
    inner: Arc<FabricInner>,
}

#[derive(Debug)]
struct FabricInner {
    /// Declared first so its thread joins before the endpoint closes the queue it
    /// polls. `None` for efa-direct, which generates no completions.
    progress: Option<ProgressDriver>,
    endpoint: Mutex<LibfabricEndpoint>,
    address: Vec<u8>,
    uses_virtual_addressing: bool,
}

impl DmaFabric {
    /// Open an endpoint for `config`.
    pub fn open(config: &FabricConfig) -> Result<Self, DmaError> {
        let endpoint = LibfabricEndpoint::open(config)?;
        let address = endpoint.local_address()?;
        let uses_virtual_addressing = endpoint.uses_virtual_addressing();
        let progress = config
            .provider()
            .needs_manual_progress()
            .then(|| ProgressDriver::new(endpoint.completion_queue()));
        Ok(Self {
            inner: Arc::new(FabricInner {
                progress,
                endpoint: Mutex::new(endpoint),
                address,
                uses_virtual_addressing,
            }),
        })
    }

    /// Register host memory for the server to RMA against.
    ///
    /// Registration is expensive and pins pages against `RLIMIT_MEMLOCK`, so reuse a
    /// region and advertise windows of it with [`DmaBuffer::slice`] rather than
    /// registering per transfer.
    pub fn register(
        &self,
        memory: impl AsMut<[u8]> + Send + 'static,
    ) -> Result<DmaBuffer, DmaError> {
        let mut memory: Box<dyn AsMut<[u8]> + Send> = Box::new(memory);
        let buffer = (*memory).as_mut();
        let (pointer, length) = (buffer.as_ptr() as u64, buffer.len());
        if length == 0 {
            return Err(DmaError::Configuration("cannot register 0 bytes".into()));
        }
        // SAFETY: `memory` is boxed and moved into the returned DmaBuffer, which
        // declares its registration first so the region closes before the memory drops.
        let memory_region = unsafe { self.endpoint().register_remote(buffer)? };
        let registration = Registration::new(memory_region, self.clone());
        let advertisement = self.advertisement(pointer, &registration);
        Ok(DmaBuffer::host(
            registration,
            advertisement,
            length,
            memory,
            self.clone(),
        ))
    }

    /// Register shared, read-only memory as a source for the `DMA.SET` direction.
    ///
    /// Takes no ownership, so one allocation can be registered on every fabric: the
    /// pages are pinned once and mapped by each domain. Registered `FI_REMOTE_READ`
    /// only, so a server can read any window advertised from it but never write.
    ///
    /// Each registration counts separately against `RLIMIT_MEMLOCK` even though the
    /// physical pages are the same.
    pub fn register_shared<S>(&self, memory: Arc<S>) -> Result<DmaBuffer, DmaError>
    where
        S: AsRef<[u8]> + Send + Sync + 'static,
    {
        let bytes: &[u8] = (*memory).as_ref();
        let (pointer, length) = (bytes.as_ptr() as u64, bytes.len());
        if length == 0 {
            return Err(DmaError::Configuration("cannot register 0 bytes".into()));
        }
        let memory_region = unsafe { self.endpoint().register_source(bytes)? };
        let registration = Registration::new(memory_region, self.clone());
        let advertisement = self.advertisement(pointer, &registration);
        Ok(DmaBuffer::shared(
            registration,
            advertisement,
            length,
            memory,
            self.clone(),
        ))
    }

    /// Insert a server address into the address vector.
    /// efa-direct requires the target to hold the initiator's address before any RMA.
    pub fn insert_peer(&self, address: &[u8]) -> Result<(), DmaError> {
        self.endpoint().fi_av_insert(address)
    }

    /// Drive progress until the returned guard drops.
    /// Hold it across a transfer's RESP round trip so the server's RMA is serviced.
    /// `None` when the provider needs no polling, which is the efa-direct case.
    #[must_use]
    pub fn drive_progress(&self) -> Option<ProgressGuard> {
        self.inner.progress.as_ref().map(ProgressDriver::drive)
    }

    /// This endpoint's local fabric address.
    pub fn local_address(&self) -> &[u8] {
        &self.inner.address
    }

    /// Whether this provider addresses registrations by virtual address rather than by
    /// offset into the region.
    pub fn uses_virtual_addressing(&self) -> bool {
        self.inner.uses_virtual_addressing
    }

    /// Close a fid belonging to this domain.
    pub(crate) fn fi_close(&self, fid: *mut ofi_libfabric_sys::bindgen::fid) {
        // this is the domain synchronization lock
        let _domain = self.endpoint();
        // SAFETY: the caller owns `fid`, it is live, and is closed exactly once. The
        // guard above excludes every other call into this domain for the duration.
        unsafe { ofi_libfabric_sys::bindgen::fi_close(fid) };
    }

    fn endpoint(&self) -> MutexGuard<'_, LibfabricEndpoint> {
        self.inner
            .endpoint
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn advertisement(&self, remote_address: u64, registration: &Registration) -> Advertisement {
        Advertisement {
            address: self.inner.address.clone(),
            remote_key: registration.remote_key(),
            remote_address: if self.inner.uses_virtual_addressing {
                remote_address
            } else {
                // for offset-addressed providers like tcp
                0
            },
        }
    }
}

/// Every fabric domain the configured provider offers, deduplicated, in the order
/// libfabric returns them. Pass one back via [`FabricConfig::with_interface`].
pub fn discover_domains(config: &FabricConfig) -> Result<Vec<String>, DmaError> {
    let list = query_info(config)?;
    let names = domain_names(list);
    // SAFETY: the list came from query_info and is freed exactly once here.
    unsafe { ofi_libfabric_sys::bindgen::fi_freeinfo(list) };
    Ok(names)
}

#[cfg(test)]
mod tests {
    use super::{DmaFabric, discover_domains};
    use crate::config::{FabricConfig, Provider};
    use std::sync::Arc;

    fn fabric() -> DmaFabric {
        DmaFabric::open(&FabricConfig::new(Provider::Tcp)).expect("tcp should open")
    }

    #[test]
    fn the_host_reports_its_domains() {
        let domains =
            discover_domains(&FabricConfig::new(Provider::Tcp)).expect("discovery failed");
        assert!(!domains.is_empty(), "tcp reported no domains");
    }

    /// An interface that does not exist still queries: filtering happens when the
    /// endpoint is opened, not here.
    #[test]
    fn discovery_ignores_the_interface_hint() {
        let config = FabricConfig::new(Provider::Tcp).with_interface("definitely-not-a-card");
        assert!(discover_domains(&config).is_ok());
    }

    #[test]
    fn clones_share_the_endpoint() {
        let first = fabric();
        let second = first.clone();
        assert_eq!(first.local_address(), second.local_address());
    }

    #[test]
    fn registers_a_buffer_and_advertises_it() {
        let fabric = fabric();
        let buffer = fabric
            .register(vec![0u8; 4096])
            .expect("registration failed");
        assert_eq!(buffer.capacity(), 4096);
        assert_eq!(buffer.advertisement().address, fabric.local_address());
        assert!(
            !buffer.advertisement().address.is_empty(),
            "an enabled endpoint has an address to advertise"
        );
    }

    /// tcp addresses by offset, so the region starts at 0 rather than at its virtual
    /// address. Getting this backwards is the silent-corruption case.
    #[test]
    fn offset_addressing_advertises_zero_for_the_region_start() {
        let fabric = fabric();
        let buffer = fabric.register(vec![0u8; 4096]).unwrap();
        if fabric.uses_virtual_addressing() {
            assert_ne!(buffer.advertisement().remote_address, 0);
        } else {
            assert_eq!(buffer.advertisement().remote_address, 0);
        }
    }

    #[test]
    fn registering_nothing_is_a_configuration_error() {
        assert!(fabric().register(Vec::new()).is_err());
        assert!(fabric().register_shared(Arc::new(Vec::new())).is_err());
    }

    /// A shared source can be registered on several fabrics at once.
    #[test]
    fn one_allocation_registers_on_several_fabrics() {
        let shared: Arc<Vec<u8>> = Arc::new(vec![7u8; 2048]);
        let first = fabric().register_shared(shared.clone()).unwrap();
        let second = fabric().register_shared(shared.clone()).unwrap();
        assert_eq!(first.capacity(), 2048);
        assert_eq!(second.capacity(), 2048);
        // Distinct endpoints, so distinct advertised addresses.
        assert_ne!(
            first.advertisement().address,
            second.advertisement().address
        );
    }

    /// A registration keeps the domain alive, so dropping the fabric first must be
    /// safe: the buffer still holds a clone.
    /// tcp emulates RMA in software, so a transfer only progresses while the target
    /// polls. efa-direct needs no driver and would return None.
    #[test]
    fn tcp_drives_progress() {
        assert!(fabric().drive_progress().is_some());
    }

    #[test]
    fn a_buffer_outlives_the_fabric_handle_it_came_from() {
        let buffer = fabric().register(vec![0u8; 1024]).unwrap();
        assert_eq!(buffer.capacity(), 1024);
        drop(buffer);
    }
}
