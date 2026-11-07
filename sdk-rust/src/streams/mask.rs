//! Read masks, applied in the SDK.
//!
//! The stream sends every client the same checkpoint: the full canonical set
//! ([`archive::CANONICAL`], BCS encodings included). A read mask narrows what this SDK
//! yields: it is validated here, and both live and archive checkpoints are
//! pruned to it, so they look the same.

use sui_rpc::field::{FieldMask, FieldMaskTree, FieldMaskUtil};

use crate::streams::Checkpoint;
use crate::streams::archive::{self, covers};

/// Fields no Sui node renders consistently; never served.
pub const EXCLUDED_PATHS: &[&str] = &[
    "transactions.events.events.json",
    "objects.objects.json",
    "objects.objects.display",
];

/// Validate `paths` and resolve them to the fields kept: `None` keeps the
/// whole checkpoint (no mask, `*`, or paths that cover every served field).
pub fn resolve(paths: &[String]) -> Result<Option<Vec<String>>, String> {
    let mut named: Vec<String> = Vec::new();
    for p in paths.iter().map(|p| p.trim()).filter(|p| !p.is_empty()) {
        if !named.iter().any(|n| n == p) {
            named.push(p.to_owned());
        }
    }
    let fields: Vec<&str> = named
        .iter()
        .map(String::as_str)
        .filter(|p| *p != "*")
        .collect();
    FieldMask::from_paths(&fields)
        .validate::<Checkpoint>()
        .map_err(|p| format!("invalid read_mask path {p:?}"))?;
    for path in &fields {
        if let Some(excluded) = EXCLUDED_PATHS.iter().find(|ex| covers(ex, path)) {
            return Err(format!(
                "read_mask path {path:?} is not served (excluded: {excluded})"
            ));
        }
    }
    if named.is_empty() {
        return Ok(None);
    }
    let effective = archive::effective(&named);
    let everything = archive::CANONICAL
        .iter()
        .all(|c| effective.iter().any(|e| covers(e, c)));
    Ok((!everything).then_some(effective))
}

/// The pruning tree for resolved paths.
pub fn tree(paths: &[String]) -> FieldMaskTree {
    FieldMaskTree::from_field_mask(&FieldMask::from_paths(paths))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(paths: &[&str]) -> Result<Option<Vec<String>>, String> {
        resolve(&paths.iter().map(|s| s.to_string()).collect::<Vec<_>>())
    }

    #[test]
    fn whole_checkpoint_needs_no_mask() {
        assert_eq!(r(&[]), Ok(None));
        assert_eq!(r(&["*"]), Ok(None));
        assert_eq!(r(&[" * ", ""]), Ok(None));
        assert_eq!(r(archive::CANONICAL), Ok(None));
        assert_eq!(r(&["*", "summary.bcs"]), Ok(None));
        // The decoded fields alone leave out the BCS encodings: a narrowing
        // mask.
        let decoded = r(archive::DEFAULT).unwrap().unwrap();
        assert!(!decoded.iter().any(|p| p.ends_with(".bcs")));
    }

    #[test]
    fn narrows_to_served_fields() {
        assert_eq!(
            r(&["sequence_number", "transactions.digest", "sequence_number"]),
            Ok(Some(vec![
                "sequence_number".to_owned(),
                "transactions.digest".to_owned()
            ]))
        );
        // A parent path stands for the whole field, BCS included.
        assert_eq!(r(&["summary"]), Ok(Some(vec!["summary".to_owned()])));
    }

    #[test]
    fn accepts_bcs() {
        assert_eq!(
            r(&["sequence_number", "transactions.effects.bcs"]),
            Ok(Some(vec![
                "sequence_number".to_owned(),
                "transactions.effects.bcs".to_owned()
            ]))
        );
        assert_eq!(
            r(&["summary.bcs.value"]),
            Ok(Some(vec!["summary.bcs.value".to_owned()]))
        );
        for b in archive::BCS_PATHS {
            assert_eq!(r(&[b]), Ok(Some(vec![(*b).to_owned()])));
        }
    }

    #[test]
    fn rejects_invalid_and_excluded() {
        assert!(r(&["transactions.efects"]).unwrap_err().contains("invalid"));
        assert!(
            r(&["objects.objects.json"])
                .unwrap_err()
                .contains("excluded")
        );
    }

    #[test]
    fn excluded_paths_match_the_server() {
        let Some(src) = crate::streams::server_common_lib() else {
            return;
        };
        for p in EXCLUDED_PATHS {
            assert!(src.contains(&format!("\"{p}\"")), "{p}");
        }
    }
}
