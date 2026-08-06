# StromDB

StromDB is a conforming durable streams server. See [docs/durable-streams-protocol.md](docs/durable-streams-protocol.md) for the full spec.

The project is nascent and pre alpha, there is no need for any kind of backward compatibility of any kind. Any and all changes are allowed and actively encouraged, no migration, no versioning.  The code should be as clean, straightforward and simple as possible. Be ambitious, if there is a clear path to improving the implementation that involves restructuring some of the codebase, go for it. Look for opportunities to reframe the change so that whole branches, helpers, modes, conditionals, or layers disappear entirely.
Prefer the solution that makes the code feel inevitable in hindsight.
Assume there is often a "code judo" move available: a re-organization that uses the existing architecture more effectively and makes the change dramatically simpler and more elegant.
If you see a path to delete complexity rather than rearrange it, push hard for that path.

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
strom-storage-engine    writer, bootstrap, admission, forest, stores
stromdb                 the embeddable library; the public API
```

Layering: protocol types stay in `strom-domain`; storage spelling stays in
`strom-storage-domain`; I/O stays in `strom-object-store`; the engine owns
typed stores, fold, and the correctness protocol; `stromdb` exposes the `Db`
handle and re-exports the vocabulary callers must name.

As you work on this codebase, if you encounter any friction in the development environment you can record it in [docs/nits.md](docs/nits.md).  Contributors will review these to help improve the lives of all devs and "make the world a better place".  