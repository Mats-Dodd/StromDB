# StromDB

StromDB is a Rust database project organized as a Cargo workspace.

Run `just ci` before committing changes. All workspace crates must inherit the
workspace package metadata and lint configuration.

Binding design decisions live in `docs/codex/`. Read `docs/codex/index.json`
before you change code and follow every `must` statement of a committed or
enforced RFC whose `applies_to` globs match the files you touch. The process
is defined in `docs/codex/0001-strom-codex.md`. After editing any codex RFC,
run `just codex-extract` and commit the regenerated index; `just codex` must
pass. The `tools/strom-codex` crate is repository tooling outside the
workspace and is exempt from the workspace lint regime.
