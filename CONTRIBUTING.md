# Contributing

Thank you for your interest in contributing to Sol!

## Getting started

1. Fork the repository and create a feature branch.
2. Make your changes.
3. Run tests: `cargo test -p sol --lib`
4. Submit a pull request.

## Pull requests

- Keep PRs focused and small when possible.
- Follow [conventional commits](https://www.conventionalcommits.org) for the PR title.
- Add tests for new functionality, especially integration tests for external services.
- Ensure `cargo clippy` and `cargo fmt --check` pass.

## Running tests

```bash
# Unit tests
cargo test -p sol --lib

# Integration tests
cargo test -p sol --test integration
```

## License

Sol is dual-licensed (see [LICENSE](LICENSE)). By contributing, you agree that
your contributions are licensed under the license that already governs the file
you change:

- Sol-original files tagged `SPDX-License-Identifier: AGPL-3.0-only` (notably
  `src/query/**`) → [AGPL-3.0-only](LICENSE-AGPL-3.0).
- Vector-derived files → [MPL-2.0](LICENSE-MPL-2.0).

New Sol-original files should carry the AGPL-3.0-only SPDX header.
