# Sol Internal Documentation

**_This folder contains internal documentation for Sol contributors._**

## Getting started

Whether you're a Vector team member, or an outside contributor, this is the best
place to start. This folder contains internal documentation to help with the
development of Vector and ensuring your change gets approved in a timely manner.

1. **[CONTRIBUTING.md](../CONTRIBUTING.md)** - Start here, contributor basics and workflow
2. **[DEVELOPING.md](DEVELOPING.md)** - Everything necessary to develop
3. **[DOCUMENTING.md](DOCUMENTING.md)** - Preparing your change for Vector users

## Vector team members

Vector team members have additional responsibilities beyond outside
contributors:

- **[REVIEWING.md](REVIEWING.md)** - Code review expectations and guidelines.
- **[USER_EXPERIENCE_DESIGN.md](USER_EXPERIENCE_DESIGN.md)** - User experience
  principles and guidelines.

## Architecture

Design and internals of Sol's query backend:

- **[architecture/metrics-path.md](architecture/metrics-path.md)** - End-to-end
  metrics write & read path (OTLP ingest → per-subtype Parquet → compaction/rollup
  lattice; query routing → cache layers → DataFusion → rate frame), the structural
  differences vs Prometheus/Mimir, and a code review of the querier. All claims
  cite `file:line`.

The decision history behind these subsystems lives in the dated workspace folders
`docs/YYYYMMDD_<name>/` (each with a `README.md`, `designs/`, ADRs, and a live
`VERIFY.md`) — e.g. `20260716_parquet-backend`, `20260716_rollup-read-routing`,
`20260717_promql-plan-cache`, `20260720_write-side-small-files`,
`20260722_rate-row-work` trace the querier-performance work.

## Project policies

Vector's policies are located in the root directory:

- **[CODE_OF_CONDUCT.md](../CODE_OF_CONDUCT.md)**
- **[PRIVACY.md](../PRIVACY.md)**
- **[RELEASES.md](../RELEASES.md)**
- **[SECURITY.md](../SECURITY.md)**
- **[VERSIONING.md](../VERSIONING.md)**
