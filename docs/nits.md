# Nits

This is a document where we can record issues in the development environment, codebase standards, practices or general development.  

The bar to add something here is low, if we dont like it we just delete the nit, if we like it we improve the dx for everyone working on strom.  

eg. text x is flaky, lint x is too cumbersome, module structure led to unclear boundaries.  

- `just dylint` fails on the current baseline (`unnamed_constant` in
  `strom-common/src/randomness.rs` and `wall_clock.rs`,
  `non_topologically_sorted_functions` in `strom-domain/src/strategy.rs`), so
  dylint never reaches later crates such as `stromdb`. `just ci` does not run
  dylint, which hides this. Either fix the baseline findings or add dylint to
  `ci` so it cannot drift again.
- RFC 0003 points to `docs/rkyv.md` as the current codec boundary, but that
  file does not exist in the repository.