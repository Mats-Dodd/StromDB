# Nits

This is a document where we can record issues in the development environment, codebase standards, practices or general development.  

The bar to add something here is low, if we dont like it we just delete the nit, if we like it we improve the dx for everyone working on strom.  

eg. text x is flaky, lint x is too cumbersome, module structure led to unclear boundaries.  

- `crates/strom-db/src/lib.rs` carries three `panic!` arms (`create_outcome`,
  `close_outcome_for_reply`, `delete_outcome`) because every command shares the
  one `StreamReply` enum, so the compiler cannot see the command/reply pairing.
  Possible fix: give each command a typed reply (typed `oneshot` per command)
  so the impossible arms and their panics disappear.
