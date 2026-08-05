---
rfc: 9999
title: Replace with the topic title
state: draft
applies_to:
  - crates/replace/with/real/globs/**
---

# RFC 9999: Replace with the topic title

This file is the template. Copy it to `NNNN-topic.md` with the next free
number; the extractor ignores `TEMPLATE.md` itself. Process rules live in
`0001-strom-codex.md`.

## Context

What problem forces a decision. What constraints apply. What
`docs/architecture.md` already fixes.

## Alternatives

The options considered and why the losers lost.

## Decision

The narrative of the chosen design. Put each binding statement next to the
prose that justifies it, like this:

```statement
slug: replace-with-a-unique-slug
level: must
text: One binding sentence, written so a violation is recognizable.
verify: test replace_with_a_test_function_name
```

A statement block carries:

- `slug`: lowercase kebab-case, unique across the whole codex;
- `level`: `must` or `should`;
- `text`: exactly one sentence;
- `verify` (zero or more): `lint <rule-id>`, `test <fn-name>`, or
  `type <TypeName>`. Required on every `must` once the RFC is `enforced`.

## Consequences

What becomes easier, what becomes harder, what future work this creates.
