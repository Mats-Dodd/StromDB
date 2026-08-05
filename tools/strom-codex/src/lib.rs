//! Pure core of the strom-codex tool.
//!
//! This module parses codex RFC documents, validates the corpus, and renders
//! the statement index. It performs no I/O: the binary shell in `main.rs`
//! gathers file contents and writes results.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::str::FromStr;

/// One codex RFC file as read from disk: its file name and its raw contents.
#[derive(Debug, Clone)]
pub struct RfcSource {
    pub file_name: String,
    pub contents: String,
}

/// A parsed codex RFC document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RfcDocument {
    pub number: u32,
    pub title: String,
    pub state: RfcState,
    pub applies_to: Vec<String>,
    pub file_name: String,
    pub statements: Vec<Statement>,
}

/// One binding statement extracted from an RFC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statement {
    pub slug: String,
    pub level: StatementLevel,
    pub text: String,
    pub verifiers: Vec<Verifier>,
}

/// A pointer from a statement to the artifact that verifies it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Verifier {
    pub kind: VerifierKind,
    pub target: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RfcState {
    Draft,
    Discussion,
    Committed,
    Enforced,
    Abandoned,
    Superseded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementLevel {
    Must,
    Should,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VerifierKind {
    Lint,
    Test,
    Type,
}

/// A problem found while parsing or validating the codex.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Issue {
    pub location: String,
    pub message: String,
}

impl fmt::Display for Issue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.location, self.message)
    }
}

/// The artifacts that verifiers may resolve against: ast-grep rule ids and
/// the contents of every Rust source file in the repository.
#[derive(Debug, Clone, Default)]
pub struct VerifierCatalog {
    pub lint_rule_ids: BTreeSet<String>,
    pub rust_sources: Vec<String>,
}

impl VerifierCatalog {
    fn resolves(&self, verifier: &Verifier) -> bool {
        match verifier.kind {
            VerifierKind::Lint => self.lint_rule_ids.contains(&verifier.target),
            VerifierKind::Test => self
                .rust_sources
                .iter()
                .any(|source| declares(source, "fn", &verifier.target)),
            VerifierKind::Type => self.rust_sources.iter().any(|source| {
                declares(source, "struct", &verifier.target)
                    || declares(source, "enum", &verifier.target)
                    || declares(source, "trait", &verifier.target)
            }),
        }
    }
}

impl RfcState {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Draft => "draft",
            Self::Discussion => "discussion",
            Self::Committed => "committed",
            Self::Enforced => "enforced",
            Self::Abandoned => "abandoned",
            Self::Superseded => "superseded",
        }
    }
}

impl FromStr for RfcState {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "draft" => Ok(Self::Draft),
            "discussion" => Ok(Self::Discussion),
            "committed" => Ok(Self::Committed),
            "enforced" => Ok(Self::Enforced),
            "abandoned" => Ok(Self::Abandoned),
            "superseded" => Ok(Self::Superseded),
            other => Err(format!(
                "unknown state `{other}`; expected draft, discussion, committed, \
                 enforced, abandoned, or superseded"
            )),
        }
    }
}

impl StatementLevel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Must => "must",
            Self::Should => "should",
        }
    }
}

impl FromStr for StatementLevel {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "must" => Ok(Self::Must),
            "should" => Ok(Self::Should),
            other => Err(format!("unknown level `{other}`; expected must or should")),
        }
    }
}

impl VerifierKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Lint => "lint",
            Self::Test => "test",
            Self::Type => "type",
        }
    }
}

impl FromStr for VerifierKind {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        match raw {
            "lint" => Ok(Self::Lint),
            "test" => Ok(Self::Test),
            "type" => Ok(Self::Type),
            other => Err(format!(
                "unknown verifier kind `{other}`; expected lint, test, or type"
            )),
        }
    }
}

impl TryFrom<RfcSource> for RfcDocument {
    type Error = Vec<Issue>;

    fn try_from(source: RfcSource) -> Result<Self, Self::Error> {
        let mut issues = Vec::new();
        let lines: Vec<&str> = source.contents.lines().collect();

        let front = match parse_front_matter(&lines, &source.file_name) {
            Ok(front) => front,
            Err(front_issues) => return Err(front_issues),
        };

        match file_name_number(&source.file_name) {
            Some(prefix_number) if prefix_number == front.number => {}
            Some(prefix_number) => issues.push(Issue {
                location: source.file_name.clone(),
                message: format!(
                    "front matter says rfc {} but the file name prefix says {}",
                    front.number, prefix_number
                ),
            }),
            None => issues.push(Issue {
                location: source.file_name.clone(),
                message: "file name must start with a four-digit RFC number and a dash".into(),
            }),
        }

        let statements = parse_statement_blocks(&lines, &source.file_name, &mut issues);

        if issues.is_empty() {
            Ok(Self {
                number: front.number,
                title: front.title,
                state: front.state,
                applies_to: front.applies_to,
                file_name: source.file_name,
                statements,
            })
        } else {
            Err(issues)
        }
    }
}

struct FrontMatter {
    number: u32,
    title: String,
    state: RfcState,
    applies_to: Vec<String>,
}

fn parse_front_matter(lines: &[&str], location: &str) -> Result<FrontMatter, Vec<Issue>> {
    let mut issues = Vec::new();
    let issue = |message: String| Issue {
        location: location.to_owned(),
        message,
    };

    if lines.first().map(|line| line.trim()) != Some("---") {
        return Err(vec![issue(
            "document must start with `---` front matter".into(),
        )]);
    }
    let Some(close) = lines
        .iter()
        .enumerate()
        .skip(1)
        .find(|(_, line)| line.trim() == "---")
        .map(|(index, _)| index)
    else {
        return Err(vec![issue("front matter has no closing `---`".into())]);
    };

    let mut number = None;
    let mut title = None;
    let mut state = None;
    let mut applies_to = Vec::new();
    let mut in_applies_to = false;

    for line in lines.iter().take(close).skip(1) {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if in_applies_to {
            if let Some(entry) = trimmed.strip_prefix("- ") {
                applies_to.push(entry.trim().to_owned());
                continue;
            }
            in_applies_to = false;
        }
        let Some((key, value)) = trimmed.split_once(':') else {
            issues.push(issue(format!("front matter line `{trimmed}` has no key")));
            continue;
        };
        let value = value.trim();
        match key.trim() {
            "rfc" => match value.parse::<u32>() {
                Ok(parsed) => number = Some(parsed),
                Err(_) => issues.push(issue(format!("rfc number `{value}` is not a u32"))),
            },
            "title" => {
                if value.is_empty() {
                    issues.push(issue("title is empty".into()));
                } else {
                    title = Some(value.to_owned());
                }
            }
            "state" => match RfcState::from_str(value) {
                Ok(parsed) => state = Some(parsed),
                Err(message) => issues.push(issue(message)),
            },
            "applies_to" => {
                if value.is_empty() {
                    in_applies_to = true;
                } else {
                    issues.push(issue("applies_to must be a list, one glob per line".into()));
                }
            }
            other => issues.push(issue(format!("unknown front matter key `{other}`"))),
        }
    }

    if number.is_none() {
        issues.push(issue("front matter is missing `rfc`".into()));
    }
    if title.is_none() {
        issues.push(issue("front matter is missing `title`".into()));
    }
    if state.is_none() {
        issues.push(issue("front matter is missing `state`".into()));
    }

    match (number, title, state) {
        (Some(number), Some(title), Some(state)) if issues.is_empty() => Ok(FrontMatter {
            number,
            title,
            state,
            applies_to,
        }),
        _ => Err(issues),
    }
}

fn parse_statement_blocks(
    lines: &[&str],
    location: &str,
    issues: &mut Vec<Issue>,
) -> Vec<Statement> {
    let mut statements = Vec::new();
    let mut cursor = 0;
    while cursor < lines.len() {
        if lines[cursor].trim() != "```statement" {
            cursor += 1;
            continue;
        }
        let block_start = cursor + 1;
        let Some(block_end) =
            (block_start..lines.len()).find(|&index| lines[index].trim() == "```")
        else {
            issues.push(Issue {
                location: location.to_owned(),
                message: "statement block has no closing fence".into(),
            });
            break;
        };
        match parse_statement_block(&lines[block_start..block_end], location) {
            Ok(statement) => statements.push(statement),
            Err(mut block_issues) => issues.append(&mut block_issues),
        }
        cursor = block_end + 1;
    }
    statements
}

fn parse_statement_block(lines: &[&str], location: &str) -> Result<Statement, Vec<Issue>> {
    let mut issues = Vec::new();
    let issue = |message: String| Issue {
        location: location.to_owned(),
        message,
    };

    let mut slug = None;
    let mut level = None;
    let mut text = None;
    let mut verifiers = Vec::new();

    for line in lines {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let Some((key, value)) = trimmed.split_once(':') else {
            issues.push(issue(format!("statement line `{trimmed}` has no key")));
            continue;
        };
        let value = value.trim();
        match key.trim() {
            "slug" => {
                if is_kebab_slug(value) {
                    slug = Some(value.to_owned());
                } else {
                    issues.push(issue(format!("slug `{value}` is not lowercase kebab-case")));
                }
            }
            "level" => match StatementLevel::from_str(value) {
                Ok(parsed) => level = Some(parsed),
                Err(message) => issues.push(issue(message)),
            },
            "text" => {
                if value.is_empty() {
                    issues.push(issue("statement text is empty".into()));
                } else {
                    text = Some(value.to_owned());
                }
            }
            "verify" => {
                let tokens: Vec<&str> = value.split_whitespace().collect();
                match tokens.as_slice() {
                    [kind, target] => match VerifierKind::from_str(kind) {
                        Ok(kind) => verifiers.push(Verifier {
                            kind,
                            target: (*target).to_owned(),
                        }),
                        Err(message) => issues.push(issue(message)),
                    },
                    _ => issues.push(issue(format!(
                        "verify line `{value}` must be `<kind> <target>`"
                    ))),
                }
            }
            other => issues.push(issue(format!("unknown statement key `{other}`"))),
        }
    }

    if slug.is_none() {
        issues.push(issue("statement block is missing `slug`".into()));
    }
    if level.is_none() {
        issues.push(issue("statement block is missing `level`".into()));
    }
    if text.is_none() {
        issues.push(issue("statement block is missing `text`".into()));
    }

    match (slug, level, text) {
        (Some(slug), Some(level), Some(text)) if issues.is_empty() => Ok(Statement {
            slug,
            level,
            text,
            verifiers,
        }),
        _ => Err(issues),
    }
}

fn is_kebab_slug(candidate: &str) -> bool {
    !candidate.is_empty()
        && !candidate.starts_with('-')
        && !candidate.ends_with('-')
        && !candidate.contains("--")
        && candidate
            .chars()
            .all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '-')
}

fn file_name_number(file_name: &str) -> Option<u32> {
    let prefix = file_name.get(..4)?;
    if !prefix.chars().all(|ch| ch.is_ascii_digit()) {
        return None;
    }
    if file_name.get(4..5) != Some("-") {
        return None;
    }
    prefix.parse().ok()
}

/// Validates rules that span the whole corpus: unique RFC numbers, unique
/// slugs, enforcement obligations, and verifier resolution.
pub fn validate_corpus(docs: &[RfcDocument], catalog: &VerifierCatalog) -> Vec<Issue> {
    let mut issues = Vec::new();
    let mut numbers_seen: BTreeMap<u32, &str> = BTreeMap::new();
    let mut slugs_seen: BTreeMap<&str, &str> = BTreeMap::new();

    for doc in docs {
        if let Some(previous) = numbers_seen.insert(doc.number, &doc.file_name) {
            issues.push(Issue {
                location: doc.file_name.clone(),
                message: format!("rfc number {} is already used by {previous}", doc.number),
            });
        }
        for statement in &doc.statements {
            if let Some(previous) = slugs_seen.insert(&statement.slug, &doc.file_name) {
                issues.push(Issue {
                    location: doc.file_name.clone(),
                    message: format!("slug `{}` is already used by {previous}", statement.slug),
                });
            }
            if doc.state == RfcState::Enforced
                && statement.level == StatementLevel::Must
                && statement.verifiers.is_empty()
            {
                issues.push(Issue {
                    location: doc.file_name.clone(),
                    message: format!(
                        "enforced must-statement `{}` names no verifier",
                        statement.slug
                    ),
                });
            }
            for verifier in &statement.verifiers {
                if !catalog.resolves(verifier) {
                    issues.push(Issue {
                        location: doc.file_name.clone(),
                        message: format!(
                            "statement `{}` names {} verifier `{}` but it does not exist",
                            statement.slug,
                            verifier.kind.as_str(),
                            verifier.target
                        ),
                    });
                }
            }
        }
    }
    issues
}

/// Renders the deterministic statement index. Output is stable: RFCs are
/// ordered by number and all fields appear in a fixed order.
pub fn render_index(docs: &[RfcDocument]) -> String {
    let mut ordered: Vec<&RfcDocument> = docs.iter().collect();
    ordered.sort_by_key(|doc| doc.number);

    let mut out = String::new();
    out.push_str("{\n  \"rfcs\": [");
    for (doc_index, doc) in ordered.iter().enumerate() {
        if doc_index > 0 {
            out.push(',');
        }
        out.push_str("\n    {\n");
        out.push_str(&format!("      \"rfc\": {},\n", doc.number));
        out.push_str(&format!(
            "      \"title\": \"{}\",\n",
            json_escape(&doc.title)
        ));
        out.push_str(&format!("      \"state\": \"{}\",\n", doc.state.as_str()));
        out.push_str(&format!(
            "      \"path\": \"docs/codex/{}\",\n",
            json_escape(&doc.file_name)
        ));
        out.push_str(&format!(
            "      \"applies_to\": [{}],\n",
            doc.applies_to
                .iter()
                .map(|glob| format!("\"{}\"", json_escape(glob)))
                .collect::<Vec<_>>()
                .join(", ")
        ));
        out.push_str("      \"statements\": [");
        for (statement_index, statement) in doc.statements.iter().enumerate() {
            if statement_index > 0 {
                out.push(',');
            }
            out.push_str("\n        {\n");
            out.push_str(&format!(
                "          \"slug\": \"{}\",\n",
                json_escape(&statement.slug)
            ));
            out.push_str(&format!(
                "          \"level\": \"{}\",\n",
                statement.level.as_str()
            ));
            out.push_str(&format!(
                "          \"text\": \"{}\",\n",
                json_escape(&statement.text)
            ));
            out.push_str(&format!(
                "          \"verify\": [{}]\n",
                statement
                    .verifiers
                    .iter()
                    .map(|verifier| format!(
                        "{{\"kind\": \"{}\", \"target\": \"{}\"}}",
                        verifier.kind.as_str(),
                        json_escape(&verifier.target)
                    ))
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
            out.push_str("        }");
        }
        if !doc.statements.is_empty() {
            out.push_str("\n      ");
        }
        out.push_str("]\n    }");
    }
    if !ordered.is_empty() {
        out.push_str("\n  ");
    }
    out.push_str("]\n}\n");
    out
}

/// True when the stored index byte-equals the freshly rendered index.
pub fn is_index_current(stored: &str, rendered: &str) -> bool {
    stored == rendered
}

fn json_escape(raw: &str) -> String {
    let mut escaped = String::with_capacity(raw.len());
    for ch in raw.chars() {
        match ch {
            '"' => escaped.push_str("\\\""),
            '\\' => escaped.push_str("\\\\"),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            control if (control as u32) < 0x20 => {
                escaped.push_str(&format!("\\u{:04x}", control as u32));
            }
            other => escaped.push(other),
        }
    }
    escaped
}

fn declares(source: &str, keyword: &str, name: &str) -> bool {
    let needle = format!("{keyword} {name}");
    let mut search_from = 0;
    while let Some(found) = source[search_from..].find(&needle) {
        let start = search_from + found;
        let end = start + needle.len();
        let boundary_before = source[..start]
            .chars()
            .next_back()
            .is_none_or(|ch| !ch.is_alphanumeric() && ch != '_');
        let boundary_after = source[end..]
            .chars()
            .next()
            .is_none_or(|ch| !ch.is_alphanumeric() && ch != '_');
        if boundary_before && boundary_after {
            return true;
        }
        search_from = end;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(file_name: &str, contents: &str) -> Result<RfcDocument, Vec<Issue>> {
        RfcDocument::try_from(RfcSource {
            file_name: file_name.to_owned(),
            contents: contents.to_owned(),
        })
    }

    fn parsed(file_name: &str, contents: &str) -> RfcDocument {
        parse(file_name, contents).expect("document under test parses cleanly")
    }

    const STATEMENT_RFC: &str = "---\n\
        rfc: 7\n\
        title: Example\n\
        state: enforced\n\
        applies_to:\n\
        \x20 - crates/strom-domain/**\n\
        ---\n\
        \n\
        Narrative.\n\
        \n\
        ```statement\n\
        slug: example-rule\n\
        level: must\n\
        text: Example rule text.\n\
        verify: test example_rule_holds\n\
        verify: lint example-rule\n\
        ```\n";

    #[test]
    fn statement_rfc_parses_completely() {
        let doc = parsed("0007-example.md", STATEMENT_RFC);
        assert_eq!(doc.number, 7, "front matter number is preserved");
        assert_eq!(doc.state, RfcState::Enforced, "state is preserved");
        assert_eq!(
            doc.applies_to,
            vec!["crates/strom-domain/**".to_owned()],
            "applies_to globs are preserved"
        );
        let statement = doc.statements.first().expect("one statement is extracted");
        assert_eq!(statement.slug, "example-rule", "slug is preserved");
        assert_eq!(statement.level, StatementLevel::Must, "level is preserved");
        assert_eq!(
            statement.verifiers,
            vec![
                Verifier {
                    kind: VerifierKind::Test,
                    target: "example_rule_holds".to_owned(),
                },
                Verifier {
                    kind: VerifierKind::Lint,
                    target: "example-rule".to_owned(),
                },
            ],
            "both verifiers are preserved in order"
        );
    }

    #[test]
    fn front_matter_missing_key_is_rejected() {
        let contents = "---\nrfc: 7\ntitle: Example\n---\nBody.\n";
        let issues = parse("0007-example.md", contents).expect_err("missing state must fail");
        assert!(
            issues.iter().any(|issue| issue.message.contains("state")),
            "the missing key is named in the issue: {issues:?}"
        );
    }

    #[test]
    fn front_matter_number_must_match_file_name() {
        let issues = parse("0008-example.md", STATEMENT_RFC)
            .expect_err("number and file name prefix disagree");
        assert!(
            issues
                .iter()
                .any(|issue| issue.message.contains("file name prefix")),
            "the mismatch is reported: {issues:?}"
        );
    }

    #[test]
    fn malformed_slug_is_rejected() {
        let contents = STATEMENT_RFC.replace("slug: example-rule", "slug: Example_Rule");
        let issues = parse("0007-example.md", &contents).expect_err("bad slug must fail");
        assert!(
            issues
                .iter()
                .any(|issue| issue.message.contains("kebab-case")),
            "the slug issue is reported: {issues:?}"
        );
    }

    #[test]
    fn duplicate_slug_is_rejected() {
        let first = parsed("0007-example.md", STATEMENT_RFC);
        let second = parsed(
            "0008-other.md",
            &STATEMENT_RFC
                .replace("rfc: 7", "rfc: 8")
                .replace("title: Example", "title: Other"),
        );
        let issues = validate_corpus(&[first, second], &full_catalog());
        assert!(
            issues
                .iter()
                .any(|issue| issue.message.contains("already used")),
            "the duplicate slug is reported: {issues:?}"
        );
    }

    #[test]
    fn enforced_must_without_verifier_is_rejected() {
        let contents = STATEMENT_RFC
            .replace("verify: test example_rule_holds\n", "")
            .replace("verify: lint example-rule\n", "");
        let doc = parsed("0007-example.md", &contents);
        let issues = validate_corpus(&[doc], &full_catalog());
        assert!(
            issues
                .iter()
                .any(|issue| issue.message.contains("names no verifier")),
            "the unverified must-statement is reported: {issues:?}"
        );
    }

    #[test]
    fn unresolved_verifier_is_rejected() {
        let doc = parsed("0007-example.md", STATEMENT_RFC);
        let issues = validate_corpus(&[doc], &VerifierCatalog::default());
        assert_eq!(
            issues.len(),
            2,
            "both verifiers fail to resolve against an empty catalog: {issues:?}"
        );
    }

    #[test]
    fn resolved_verifiers_pass() {
        let doc = parsed("0007-example.md", STATEMENT_RFC);
        let issues = validate_corpus(&[doc], &full_catalog());
        assert_eq!(issues, Vec::new(), "a complete catalog resolves everything");
    }

    #[test]
    fn declaration_search_requires_word_boundaries() {
        let source = "pub fn example_rule_holds_more() {}";
        assert!(
            !VerifierCatalog {
                lint_rule_ids: BTreeSet::new(),
                rust_sources: vec![source.to_owned()],
            }
            .resolves(&Verifier {
                kind: VerifierKind::Test,
                target: "example_rule_holds".to_owned(),
            }),
            "a longer identifier must not satisfy a shorter target"
        );
    }

    #[test]
    fn rendered_index_matches_spec_anchor() {
        let doc = parsed("0007-example.md", STATEMENT_RFC);
        let expected = "{\n  \"rfcs\": [\n    {\n      \"rfc\": 7,\n      \"title\": \"Example\",\n      \"state\": \"enforced\",\n      \"path\": \"docs/codex/0007-example.md\",\n      \"applies_to\": [\"crates/strom-domain/**\"],\n      \"statements\": [\n        {\n          \"slug\": \"example-rule\",\n          \"level\": \"must\",\n          \"text\": \"Example rule text.\",\n          \"verify\": [{\"kind\": \"test\", \"target\": \"example_rule_holds\"}, {\"kind\": \"lint\", \"target\": \"example-rule\"}]\n        }\n      ]\n    }\n  ]\n}\n";
        assert_eq!(
            render_index(&[doc]),
            expected,
            "the rendered index is the exact durable format"
        );
    }

    #[test]
    fn stale_index_is_detected() {
        let doc = parsed("0007-example.md", STATEMENT_RFC);
        let rendered = render_index(std::slice::from_ref(&doc));
        assert!(
            is_index_current(&rendered, &rendered),
            "an identical index is current"
        );
        assert!(
            !is_index_current("{}\n", &rendered),
            "a differing index is stale"
        );
    }

    fn full_catalog() -> VerifierCatalog {
        VerifierCatalog {
            lint_rule_ids: BTreeSet::from(["example-rule".to_owned()]),
            rust_sources: vec!["pub fn example_rule_holds() {}".to_owned()],
        }
    }
}
