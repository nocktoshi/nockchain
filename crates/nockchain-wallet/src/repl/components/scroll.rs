//! Wrapped-line estimates for scroll clamping (must be ≥ ratatui word-wrap rows).

/// Rough wrapped-row upper bound for clamping scroll (must be ≥ ratatui word-wrap rows).
pub(crate) fn estimate_wrapped_source_lines(text: &str, inner_w: u16) -> usize {
    let w = inner_w.max(1) as usize;
    text.split('\n')
        .map(|line| {
            let n = line.chars().count().max(1);
            (n + w - 1) / w
        })
        .sum::<usize>()
        .max(1)
}
