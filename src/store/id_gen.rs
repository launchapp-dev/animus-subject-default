//! Sequential task id generation that mirrors the in-tree
//! `next_sequential_id` helper in
//! `crates/orchestrator-core/src/services/task_shared.rs`.
//!
//! Given a prefix (`TASK-`) and an existing set of ids, the next id is the
//! highest numeric suffix + 1, zero-padded to a fixed width.

/// Compute the next sequential id given the existing id set, the desired
/// prefix, and zero-pad width.
///
/// IDs that don't start with `prefix` or whose suffix isn't a base-10
/// integer are ignored — matching the in-tree behavior.
pub fn next_sequential_id<'a, I>(existing: I, prefix: &str, pad: usize) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    let next_seq = existing
        .into_iter()
        .filter_map(|id| id.strip_prefix(prefix))
        .filter_map(|seq| seq.parse::<u32>().ok())
        .max()
        .map_or(1, |max_seq| max_seq.saturating_add(1));
    format!("{prefix}{next_seq:0width$}", width = pad)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_yields_first_id() {
        let next = next_sequential_id(std::iter::empty(), "TASK-", 3);
        assert_eq!(next, "TASK-001");
    }

    #[test]
    fn increments_max_suffix() {
        let existing = ["TASK-001", "TASK-009", "TASK-003"];
        let next = next_sequential_id(existing.iter().copied(), "TASK-", 3);
        assert_eq!(next, "TASK-010");
    }

    #[test]
    fn ignores_foreign_prefixes() {
        let existing = ["REQ-014", "TASK-007", "garbage"];
        let next = next_sequential_id(existing.iter().copied(), "TASK-", 3);
        assert_eq!(next, "TASK-008");
    }

    #[test]
    fn matches_in_tree_three_digit_pad() {
        // The in-tree formatter is `{next_seq:03}`. Width 3 must produce
        // the same string for every value <= 999.
        for n in 1u32..=999 {
            let id = format!("TASK-{n:03}");
            let next = next_sequential_id([id.as_str()], "TASK-", 3);
            let expected = format!("TASK-{:03}", n + 1);
            assert_eq!(next, expected);
        }
    }
}
