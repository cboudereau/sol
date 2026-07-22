# CLAUDE.md

## Standing directives
- **No parquet/rollup retro-compat, ever**: any storage layout/schema change ships as a clean cutover (demo store wipe); never write dual-format read paths or migration code.

## Active workspaces