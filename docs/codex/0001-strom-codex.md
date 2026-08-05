---
rfc: 1
title: The Strom Codex
state: enforced
applies_to:
  - docs/codex/**
---

# RFC 0001: The Strom Codex

The codex is StromDB's decision record. It holds one RFC per design topic.
Each RFC carries a free narrative for humans and explicit binding statements
for machines. A tool (`tools/strom-codex`) extracts every statement into
`docs/codex/index.json`, and CI keeps prose, index, and code in agreement.

The model combines two lineages:

- the Oxide RFD process: numbered documents, lifecycle states, discussion in
  pull requests, permanent once merged;
- the Cloudflare Codex: MUST/SHOULD statements compacted into a structured
  index with stable identifiers, so agents load rules without loading prose.

One deliberate difference: Cloudflare extracts statements from prose with an
LLM. Here statements are marked explicitly in the source document, so
extraction is deterministic and CI needs no model.

## Position among the other documents

- `docs/stromstyle.md` is the constitution. It is global, unscoped, and above
  the codex. Its rules are enforced by clippy, ast-grep, and dylint directly.
- `docs/architecture.md` is the standing design authority, as its own preamble
  says, "unless superseded by explicit RFCs". Those RFCs live here.
- The codex holds scoped design decisions: one RFC per topic, each binding
  statement addressable by a stable slug and checkable by CI.

## Lifecycle

```text
draft -> discussion -> committed -> enforced
                            \-> abandoned / superseded
```

- `draft`: private iteration in a branch.
- `discussion`: open design, iterated in a pull request.
- `committed`: merged; the design is authority. Verifiers are encouraged but
  not required.
- `enforced`: every MUST statement names at least one verifier, and CI fails
  otherwise.
- `abandoned` / `superseded`: kept for the record; statements lose force. An
  RFC file is never deleted.

## Document format

An RFC file is named `NNNN-topic.md` with a four-digit number. It starts with
front matter carrying four keys: `rfc` (must equal the file name prefix),
`title`, `state`, and an optional `applies_to` list of file globs. The body is
free Markdown narrative.

Binding statements appear inline, next to the narrative that justifies them,
as fenced code blocks with the info string `statement`. A block carries a
`slug` (lowercase kebab-case, unique across the codex), a `level` (`must` or
`should`), a one-sentence `text`, and zero or more `verify` lines of the form
`verify: <kind> <target>`. See `TEMPLATE.md` for a literal example; this
document cannot show one inline because the extractor would parse it.

Verifier kinds:

| Kind | Target | Resolved against |
| --- | --- | --- |
| `lint` | ast-grep rule id | `lint/rules/<id>.yml` |
| `test` | test function name | `fn <name>` in `crates/` or `tools/` |
| `type` | type name | `struct`/`enum`/`trait` declaration in `crates/` |

## Rules of the codex itself

These statements govern the codex, and the extractor's own tests verify them.

```statement
slug: codex-front-matter-is-complete
level: must
text: Every codex RFC declares rfc, title, and state in front matter, and the rfc number equals the file name prefix.
verify: test front_matter_missing_key_is_rejected
verify: test front_matter_number_must_match_file_name
```

```statement
slug: codex-slugs-are-unique
level: must
text: Statement slugs are unique across the whole codex.
verify: test duplicate_slug_is_rejected
```

```statement
slug: codex-slugs-are-kebab-case
level: must
text: Statement slugs are lowercase kebab-case.
verify: test malformed_slug_is_rejected
```

```statement
slug: codex-enforced-musts-name-verifiers
level: must
text: In an enforced RFC, every must-statement names at least one verifier.
verify: test enforced_must_without_verifier_is_rejected
```

```statement
slug: codex-verifiers-resolve
level: must
text: Every named verifier resolves to an existing lint rule, test function, or type.
verify: test unresolved_verifier_is_rejected
```

```statement
slug: codex-index-is-current
level: must
text: docs/codex/index.json always equals the index rendered from the current RFC files.
verify: test stale_index_is_detected
```

```statement
slug: codex-rfcs-are-permanent
level: should
text: An RFC file is superseded or abandoned, never deleted, so decision history stays recoverable.
```

## Workflow

1. Copy `TEMPLATE.md` to `NNNN-topic.md` with the next free number.
2. Iterate in a branch (`draft`), then open a pull request (`discussion`).
3. Merge with state `committed`. Run `just codex-extract` and commit the
   regenerated index.
4. When the implementation lands, add `verify` lines to the MUST statements
   and promote the state to `enforced`.

Agents: read `docs/codex/index.json` before changing code. Follow every
statement of a `committed` or `enforced` RFC whose `applies_to` globs match
the files you touch. Load the full RFC narrative only when the statement text
is not enough.
