# StromDB

StromDB is a durable streams db.  The goal for the project is to offer both an embeddable library as well as a http server binary that serve the protocol, using only s3 as the durable source of truth.  

All development happens through the rfc process.  existing RFC's can be found in [docs/rfcs.md](docs/rfcs.md).  An initial sketch of the architecture is available in our architecture doc at [docs/architecture.md](docs/architecture.md).  It is in no way canonical and is guarentee to change as this project develops.  

The project is pre alpha software.  It is under active development and all design decisions, api surface and implementation details are subject to significant change.  


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
