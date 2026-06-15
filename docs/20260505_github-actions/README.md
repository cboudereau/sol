# github-actions

Sol is a fork of Vector (Datadog) and inherits **42 GitHub Actions workflows** designed for Vector's upstream open-source project. Most of these workflows are unnecessary for Sol's purposes: they serve Datadog-specific infrastructure (SMP regression, Datadog CI, dd-pkg), open-source governance (CLA, gardener bots, scorecards), upstream publishing (S3, DockerHub/timberio, Homebrew), preview sites (

## Design
- [20260505_github-actions](./designs/20260505_github-actions.md)

## ADRs
- [20260505_single-arch-target](./adrs/20260505_single-arch-target.md) — Single architecture packaging, multi-platform CI
- [20260505_workflow-consolidation](./adrs/20260505_workflow-consolidation.md) — Workflow consolidation strategy
