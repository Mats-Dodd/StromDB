# StromDB

StromDB is a database project written in Rust.

## Development

The repository is a Cargo workspace with a reproducible development environment
managed by [devenv](https://devenv.sh/getting-started/). Install Nix and devenv,
then enter the environment:

```sh
devenv shell
```

Inside the environment, run the complete CI suite with:

```sh
just ci
```

Alternatively, build the environment and run the complete suite directly:

```sh
devenv test
```

The Rust toolchain is pinned by `rust-toolchain.toml`; devenv provides the
workspace tools. Use `just --list` to see the individual checks and development
commands.
