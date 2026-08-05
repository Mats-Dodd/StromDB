# StromDB

StromDB is a conforming durable streams server. See [docs/durable-streams-protocol.md](docs/durable-streams-protocol.md) for the full spec.

The project is nascent and pre alpha, there is no need for any kind of backward compatibility of any kind.

All code must be written to adhere to our style guide in [docs/stromstyle](docs/stromstyle.md).

The current architecture is still being refined, broad strokes exist in [docs/architecture.md](docs/architecture.md) and will be chisseled at and refined through the rfc process in [docs/rfcs](docs/rfcs/).

Run `just ci` before committing changes. All workspace crates must inherit the
workspace package metadata and lint configuration.

## Crate outline

```text
strom-common            clock and entropy seams
strom-domain            Durable Streams protocol vocabulary
strom-storage-domain    storage vocabulary and durable codecs
strom-object-store      object-store adapter (opaque bytes)
stromdb                 engine (joins the crates above)
```

Layering: protocol types stay in `strom-domain`; storage spelling stays in
`strom-storage-domain`; I/O stays in `strom-object-store`; the engine owns
typed stores, fold, and the correctness protocol.

