# ofi-libfabric-sys

The official distribution of lightweight Rust bindings for Libfabric - a communication API for high-performance parallel and distributed applications, by the OFI Working Group.

### Motivation

Increasing number of HPC networking code is being written in Rust. Naturally, to
support Libfabric usage in Rust, there needs a proper Rust library that wraps
Libfabric APIs written in C. This practice is commonly referred as a Rust
binding / FFI (foreign function interface).

This library builds a lightweight Rust binding via bindgen. Lightweight, meaning
there's no additional abstraction on top of the automatically generated code via
bindgen, aside from the `wrapper.[ch]` which is strictly used to support
`static inline` functions to be properly bound, by introducing a new translation
unit upon compilation.

### Build

```
// Not strictly necessary for the build process, but necessary for running the built library.
LD_LIBRARY_PATH={your_directory_containing_libfabric.so}

// Clean the existing build.
cargo clean

// [Recommended] Build, using the already installed Libfabric.
PKG_CONFIG_PATH={your_directory_containing_libfabric.pc} cargo build

// [Only for library development] Build, using the Libfabric binary that is compiled on-the-fly.
// The vendored option is not supported for library import scenario (imported under Cargo.toml).
// Rather, it is meant for developers building the ofi-libfabric-sys library under the libfabric repo.
cargo build --features vendored

// Unit-tests.
cargo test

// Unit-tests with ASAN enabled.
cargo test --features asan
```

### How to use the library

Add the crate dependency under your Rust application's `Cargo.toml` file. Then;

```rust
use ofi_libfabric_sys::bindgen as ffi;
use std::ffi::CString;
use std::ptr;

fn test_get_info() {
    unsafe {
        // Configure hints.
        let hints = ffi::fi_allocinfo();
        assert_eq!(hints.is_null(), false);

        (*hints).caps = ffi::FI_MSG as u64;
        (*hints).mode = ffi::FI_CONTEXT;
        (*(*hints).ep_attr).type_ = ffi::fi_ep_type_FI_EP_RDM;
        (*(*hints).domain_attr).mr_mode = ffi::FI_MR_LOCAL as i32;
        let prov_name = CString::new("tcp").unwrap();
        (*(*hints).fabric_attr).prov_name = prov_name.into_raw() as *mut i8;

        // Get Fabric info based on the hints.
        let mut info_ptr = ptr::null_mut();
        let version = ffi::fi_version();
        let ret = ffi::fi_getinfo(
            version,
            ptr::null_mut(),
            ptr::null_mut(),
            0,
            hints,
            &mut info_ptr,
        );

        assert_eq!(ret, 0);

        // Free the info structure returned by fi_getinfo.
        if !info_ptr.is_null() {
            ffi::fi_freeinfo(info_ptr);
        }

        // Free the hints structure we allocated.
        ffi::fi_freeinfo(hints);
    }
}
```

### Files

- `build.rs`: The actual build script for the bindgen.
- `src/lib.rs`: The generated binding is copy-pasted programmatically and
  publicly exported under `bindgen` namespace.
- `wrapper.[ch]`: Wrapper source files that simply calls the static inline
  functions. This way, an isolated translation unit for each static inline
  function is made, for which the Rust bindgen is able to link against it.
- `tests/unit_test.rs`: Unit tests.

---

## Notes for valkey-glide

This is a vendored copy of `bindings/rust` from
[ofiwg/libfabric](https://github.com/ofiwg/libfabric) at revision `6953579a5035`.
It differs from upstream in five ways, the first four so that a build of GLIDE
needs nothing installed and a shipped binary does not demand libfabric on the
user's machine.

1. **Package metadata is inlined** rather than inherited from libfabric's Cargo
   workspace, which is not vendored with it.

2. **`cargo:rustc-link-lib=fabric` is behind the `link-fabric` feature, off by
   default.** Upstream emits it unconditionally. It records a dependency that the
   operating system resolves while opening the file, so a machine without
   libfabric cannot start a program built this way even if the program was never
   going to use a fabric. With the feature off, the few libfabric entry points
   that are real linker symbols are resolved at run time by `glide-rdma`, and the
   binary loads anywhere. Note that this took two changes, not one: the
   `pkg-config` probe prints the same directive of its own accord unless asked
   not to.

3. **libfabric's public headers are vendored under `include/`.** Upstream builds
   against whatever `pkg-config` finds. That left the API version the bindings
   describe up to the build machine — and libfabric is told at run time which
   version the caller was compiled for, so that has to be chosen deliberately.
   Pass `--features system-headers` for upstream's behaviour.

4. **The bindgen output is generated once and kept, as `src/bindings.rs`.**
   Running bindgen needs libclang on every machine that builds this crate. It is
   an optional build dependency now, so an ordinary build does not even compile
   it.

   One consequence to know about: a few of the constants bindgen copies out are
   the build machine's, not the target's. The `FI_E*` family is defined in terms
   of the platform's own `errno` values, which differ — `FI_ENODATA` is 61 on
   Linux and 96 on macOS — so the checked-in file carries whichever set belonged
   to the machine that last ran bindgen. Do not compare a libfabric return code
   against these; take the value from `libc` instead, which follows the target.

5. **The `cargo:warning` lines listing the include paths are removed.** Upstream
   prints them on every build, and cargo repeats them for everyone who depends on
   the crate. They read as debug output left behind.

Together, (3) and (4) mean a release builder needs neither `libfabric-dev` nor
`libclang`.

### Regenerating the bindings

After changing anything under `include/`:

```
cargo build --features regenerate-bindings   # needs libclang
```

This rewrites `src/bindings.rs` in place. Commit the result. The API version it
describes — `FI_MAJOR_VERSION` and `FI_MINOR_VERSION` near the top — is the
version `glide-rdma` will refuse to run against anything older than, so a header
bump is a deliberate change to what the client supports.
