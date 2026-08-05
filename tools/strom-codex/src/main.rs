//! I/O shell of the strom-codex tool.
//!
//! `strom-codex extract` regenerates `docs/codex/index.json`.
//! `strom-codex check` validates the corpus and fails on any issue or on a
//! stale index. Run from the repository root (the `just` recipes do).

use std::collections::BTreeSet;
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use strom_codex::{
    Issue, RfcDocument, RfcSource, VerifierCatalog, is_index_current, render_index, validate_corpus,
};

const CODEX_DIR: &str = "docs/codex";
const INDEX_PATH: &str = "docs/codex/index.json";
const LINT_RULES_DIR: &str = "lint/rules";
const RUST_SOURCE_ROOTS: [&str; 2] = ["crates", "tools/strom-codex/src"];

enum Command {
    Extract,
    Check,
}

fn main() -> ExitCode {
    let mut args = env::args().skip(1);
    let command = match args.next().as_deref() {
        Some("extract") => Command::Extract,
        Some("check") => Command::Check,
        _ => {
            eprintln!("usage: strom-codex <extract|check>  (run from the repository root)");
            return ExitCode::from(2);
        }
    };

    if !Path::new(CODEX_DIR).is_dir() {
        eprintln!("{CODEX_DIR} not found; run from the repository root");
        return ExitCode::from(2);
    }

    let sources = match load_rfc_sources() {
        Ok(sources) => sources,
        Err(message) => {
            eprintln!("{message}");
            return ExitCode::FAILURE;
        }
    };

    let mut issues: Vec<Issue> = Vec::new();
    let mut docs: Vec<RfcDocument> = Vec::new();
    for source in sources {
        match RfcDocument::try_from(source) {
            Ok(doc) => docs.push(doc),
            Err(mut source_issues) => issues.append(&mut source_issues),
        }
    }

    let catalog = load_catalog();
    issues.extend(validate_corpus(&docs, &catalog));
    let rendered = render_index(&docs);
    let statement_count: usize = docs.iter().map(|doc| doc.statements.len()).sum();

    match command {
        Command::Extract => {
            if issues.is_empty() {
                if let Err(error) = fs::write(INDEX_PATH, &rendered) {
                    eprintln!("cannot write {INDEX_PATH}: {error}");
                    return ExitCode::FAILURE;
                }
                println!(
                    "wrote {INDEX_PATH}: {} rfcs, {statement_count} statements",
                    docs.len()
                );
            }
        }
        Command::Check => match fs::read_to_string(INDEX_PATH) {
            Ok(stored) => {
                if !is_index_current(&stored, &rendered) {
                    issues.push(Issue {
                        location: INDEX_PATH.to_owned(),
                        message: "index is stale; run `just codex-extract` and commit".to_owned(),
                    });
                }
            }
            Err(_) => issues.push(Issue {
                location: INDEX_PATH.to_owned(),
                message: "index is missing; run `just codex-extract` and commit".to_owned(),
            }),
        },
    }

    if issues.is_empty() {
        if matches!(command, Command::Check) {
            println!(
                "codex ok: {} rfcs, {statement_count} statements, index current",
                docs.len()
            );
        }
        ExitCode::SUCCESS
    } else {
        for issue in &issues {
            eprintln!("codex: {issue}");
        }
        eprintln!("codex: {} issue(s)", issues.len());
        ExitCode::FAILURE
    }
}

fn load_rfc_sources() -> Result<Vec<RfcSource>, String> {
    let mut paths: Vec<PathBuf> = fs::read_dir(CODEX_DIR)
        .map_err(|error| format!("cannot read {CODEX_DIR}: {error}"))?
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect();
    paths.sort();

    let mut sources = Vec::new();
    for path in paths {
        let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if !is_rfc_file_name(file_name) {
            continue;
        }
        let contents = fs::read_to_string(&path)
            .map_err(|error| format!("cannot read {}: {error}", path.display()))?;
        sources.push(RfcSource {
            file_name: file_name.to_owned(),
            contents,
        });
    }
    Ok(sources)
}

fn is_rfc_file_name(file_name: &str) -> bool {
    file_name.len() > 5
        && file_name.ends_with(".md")
        && file_name.as_bytes()[..4].iter().all(u8::is_ascii_digit)
        && file_name.as_bytes()[4] == b'-'
}

fn load_catalog() -> VerifierCatalog {
    let mut lint_rule_ids = BTreeSet::new();
    if let Ok(entries) = fs::read_dir(LINT_RULES_DIR) {
        for entry in entries.filter_map(Result::ok) {
            let path = entry.path();
            if path.extension().is_some_and(|extension| extension == "yml")
                && let Some(stem) = path.file_stem().and_then(|stem| stem.to_str())
            {
                lint_rule_ids.insert(stem.to_owned());
            }
        }
    }

    let mut rust_sources = Vec::new();
    for root in RUST_SOURCE_ROOTS {
        collect_rust_sources(Path::new(root), &mut rust_sources);
    }

    VerifierCatalog {
        lint_rule_ids,
        rust_sources,
    }
}

fn collect_rust_sources(dir: &Path, rust_sources: &mut Vec<String>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    let mut paths: Vec<PathBuf> = entries
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .collect();
    paths.sort();
    for path in paths {
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == "target") {
                continue;
            }
            collect_rust_sources(&path, rust_sources);
        } else if path.extension().is_some_and(|extension| extension == "rs")
            && let Ok(contents) = fs::read_to_string(&path)
        {
            rust_sources.push(contents);
        }
    }
}
