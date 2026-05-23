//! Copy text to the system clipboard (TUI receive screen, etc.).

/// Copy `text` to the system clipboard. Returns a user-facing error on failure.
pub(crate) fn copy_to_clipboard(text: &str) -> Result<(), String> {
    use arboard::Clipboard;
    let mut clip = Clipboard::new().map_err(|e| format!("Clipboard unavailable: {e}"))?;
    clip.set_text(text)
        .map_err(|e| format!("Failed to copy: {e}"))
}
