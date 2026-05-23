//! Clipboard insert for bracketed paste ([`crossterm::event::Event::Paste`]).

use super::screens::TextThen;

/// Single-line fields (addresses, amounts, paths): use the first non-empty line, trimmed.
pub(crate) fn paste_single_line(buf: &mut String, pasted: &str) {
    let chunk = pasted.trim();
    let chunk = chunk.lines().next().unwrap_or(chunk).trim();
    buf.push_str(chunk);
}

/// Multiline-capable prompts (message bodies, memos).
pub(crate) fn paste_multiline(buf: &mut String, pasted: &str) {
    buf.push_str(pasted);
}

pub(crate) fn text_prompt_allows_multiline(then: &TextThen) -> bool {
    matches!(then, TextThen::SignMsgStepMessage | TextThen::VerifyMsgM)
}
