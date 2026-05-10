# Development Environment Setup

This document describes the WSL2 and Rust setup used to build and develop Sol.

## Host

| Component | Version |
|---|---|
| OS | Windows + WSL2 |
| WSL kernel | 6.6.87.2-microsoft-standard-WSL2 |
| Distro | Ubuntu 24.04.3 LTS (Noble Numbat) |
| Architecture | x86_64 |
| RAM | 12 GB (WSL2 allocated) |
| CPU cores | 12 |
| Disk | ~1 TB ext4 (WSL2 virtual disk) |

### WSL configuration

`/etc/wsl.conf`:

```ini
[boot]
systemd=true

[user]
default=clem
```

## Rust toolchain

### Installation

Install Rust via [rustup](https://rustup.rs/):

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

Follow the on-screen prompts (the defaults are fine). Once installed, make sure `~/.cargo/bin` is on your `PATH` by sourcing the environment:

```bash
source "$HOME/.cargo/env"
```

Verify the installation:

```bash
rustc --version
cargo --version
```

> **Note:** The project's `rust-toolchain.toml` will cause `rustup` to automatically download and use the correct toolchain version the first time you run a `cargo` command in the repository.

### Pinned toolchain

The project pins its toolchain via `rust-toolchain.toml`:

```toml
[toolchain]
channel = "1.92"
profile = "default"
```

| Component | Version |
|---|---|
| rustc | 1.92.0 (ded5c06cf 2025-12-08) |
| cargo | 1.92.0 (344c4567c 2025-10-21) |
| Target | x86_64-unknown-linux-gnu |
| clippy | installed |
| rustfmt | installed |

## System dependencies

Install them on Ubuntu 24.04 with:

```bash
sudo apt-get update && sudo apt-get install -y \
    autoconf \
    automake \
    build-essential \
    cmake \
    git \
    libclang-dev \
    libsasl2-dev \
    libssl-dev \
    libtool \
    pkg-config \
    protobuf-compiler
```

> **Why autoconf / automake / libtool?** The `rdkafka` crate enables
> `gssapi-vendored` on Linux GNU targets, which pulls in `sasl2-sys` with the
> `vendored` feature. That feature builds `libsasl2` from source using GNU
> Autotools. Without these packages the build fails with
> `configure failed: No such file or directory`.

Installed versions on this machine:

| Package | Version |
|---|---|
| autoconf | 2.71 |
| automake | 1.16.5 |
| libtool | 2.4.7 |
| gcc / g++ | 13.3.0 |
| cmake | 3.28.3 |
| protoc (protobuf-compiler) | 3.20.2 |
| libssl-dev | 3.0.13 |
| libsasl2-dev | 2.1.28 |
| libclang-dev | 18.0 |
| pkg-config | 1.8.1 |
| make | 4.3 |
| perl | 5.38.2 |

## Repository

```
git@github.com-cboudereau/sol.git
```

Workspace root: `/home/clem/gh/sol`

## Additional Cargo tools

### cargo-nextest

[cargo-nextest](https://nexte.st/) is used by CI (`make test`) to run unit tests. Install it with:

```bash
cargo install cargo-nextest --locked
```

Verify:

```bash
cargo nextest --version
```

## Build commands

```bash
# Type-check (fastest feedback loop, ~30-60s incremental)
cargo check

# Run all lib unit tests (~45s after build)
cargo test --lib

# Run a specific test
cargo test --lib -- "sinks::elasticsearch::tests::encode_valid"

# Clippy lint
cargo clippy --workspace --all-targets -- -D warnings

# Format check
cargo fmt --all -- --check

# Test sol
cargo test -p sol --all-features

# Test
cargo test -p sol -p sol-core -p codecs

# To log output:
RUSTFLAGS="--cfg tokio_unstable" cargo test -p sol --all-features 2>&1 | tee tests.log
```

### Cargo aliases

Defined in `.cargo/config.toml`:

```bash
# Run the vdev CLI helper
cargo vdev <args>
```

### Notable build flags

From `.cargo/config.toml`:

- **jemalloc**: `JEMALLOC_SYS_WITH_LG_PAGE=16` (large page support for CentOS/RHEL compatibility)
- **Linux GNU target**: `-C link-args=-rdynamic` (export symbols for plugin support)
- **Clippy denies**: `print_stdout`, `print_stderr`, `dbg_macro`

## Git configuration

Ensure `core.fileMode` and `core.symlinks` are enabled:

```bash
git config core.fileMode true
git config core.symlinks true
```

`core.symlinks` is required because some test data directories (e.g. `lib/sol-core/tests/data/ca`) are symbolic links. With `core.symlinks=false` (common default on WSL2), git checks them out as plain text files, breaking TLS tests with `NotADirectory` errors.

Some test fixtures (e.g. `tests/data/journalctl`) are shell scripts that must be executable. If `core.fileMode` is `false`, git ignores permission bits and these files lose their execute flag, causing tests to fail with `PermissionDenied`.

If you already cloned with `core.fileMode=false`, restore the permissions after changing the setting:

```bash
git checkout -- tests/data/journalctl
```

Alternatively, fix the permission directly:

```bash
chmod +x tests/data/journalctl
```

## Performance tips for WSL2

- **Keep the repo on the Linux filesystem** (`/home/...`), not on `/mnt/c/...`. Cross-filesystem I/O through the 9P mount is 10-50x slower.
- **Increase WSL memory** if builds OOM. Create or edit `%USERPROFILE%\.wslconfig`:

  ```ini
  [wsl2]
  memory=16GB
  processors=12
  ```

  Then restart WSL: `wsl --shutdown` from PowerShell.

- **Use `cargo check`** for fast iteration. A full `cargo test --lib` build from scratch takes ~5 minutes; incremental `cargo check` takes ~30s.
- **Avoid running Windows antivirus on the WSL2 vhdx**. Add the vhdx path to Windows Defender exclusions.
