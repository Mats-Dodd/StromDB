default:
    just --list

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all --check

check: fmt-check clippy lint-style
    cargo check --workspace --all-targets --all-features

clippy:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

lint-style:
    ast-grep test --skip-snapshot-tests
    ast-grep scan

dylint:
    DYLINT_RUSTFLAGS="--deny warnings" cargo dylint --all --workspace -- --all-targets --all-features

test:
    cargo nextest run --workspace --all-features --no-tests pass

test-ci:
    cargo nextest run --workspace --all-features --profile ci --no-tests pass

doctest:
    cargo test --workspace --doc

docs:
    RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps

mutants-list:
    cargo mutants --list

mutants *ARGS:
    NEXTEST_PROFILE=mutants cargo mutants -j2 {{ARGS}}

mutants-iterate *ARGS:
    NEXTEST_PROFILE=mutants cargo mutants -j2 --iterate {{ARGS}}

mutants-ci:
    NEXTEST_PROFILE=mutants cargo mutants --in-place

deny:
    cargo deny check

unused-deps:
    cargo machete crates

feature-matrix:
    cargo hack check --workspace --feature-powerset --all-targets

supply-chain: deny unused-deps feature-matrix

ci: check test-ci doctest docs supply-chain
