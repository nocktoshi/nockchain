//! Create-transaction wizard rendering.

use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Wrap};
use ratatui::Frame;

use crate::repl::create_tx::{OptSub, Phase, RecSub};

use super::menus::{BOOL, NOTE_ORDER};

fn list_inline(lines: &mut Vec<Line>, items: &[&str], sel: usize) {
    for (i, s) in items.iter().enumerate() {
        let style = if i == sel {
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        };
        lines.push(Line::from(Span::styled(format!("  {s}"), style)));
    }
}

pub(crate) fn draw_create_tx(
    f: &mut Frame<'_>,
    area: ratatui::layout::Rect,
    w: &crate::repl::create_tx::CreateTxWizard,
    tick: u64,
) {
    let spin = ["|", "/", "-", "\\"][tick as usize % 4];
    let mut lines: Vec<Line> = vec![
        Line::from(Span::styled(
            w.title_line(),
            Style::default().add_modifier(Modifier::BOLD),
        )),
        Line::from(""),
    ];
    if let Some(s) = &w.status {
        lines.push(Line::from(Span::styled(
            s.clone(),
            Style::default().fg(Color::Red),
        )));
        lines.push(Line::from(""));
    }
    match &w.phase {
        Phase::Recipients { list, sub } => {
            lines.push(Line::from(format!("Recipients added: {}", list.len())));
            match sub {
                RecSub::Address { line } => {
                    lines.push(Line::from("Recipient address (empty when done):"));
                    lines.push(Line::from(format!("> {line}")));
                }
                RecSub::Amount { addr, line } => {
                    lines.push(Line::from(format!("Address: {addr}")));
                    lines.push(Line::from("Amount (>0):"));
                    lines.push(Line::from(format!("> {line}")));
                }
                RecSub::Memo { addr, amount, line } => {
                    lines.push(Line::from(format!("{addr}  amount={amount}")));
                    lines.push(Line::from("Memo (optional):"));
                    lines.push(Line::from(format!("> {line}")));
                }
                RecSub::Blob {
                    addr,
                    amount,
                    memo,
                    line,
                } => {
                    lines.push(Line::from(format!(
                        "{addr}  amount={amount}  memo={:?}",
                        memo
                    )));
                    lines.push(Line::from("Blob path (optional):"));
                    lines.push(Line::from(format!("> {line}")));
                }
                RecSub::AddAnother { sel } => {
                    lines.push(Line::from("Add another recipient?"));
                    list_inline(&mut lines, BOOL, *sel);
                }
            }
        }
        Phase::Options { sub, .. } => match sub {
            OptSub::Names { line } => {
                lines.push(Line::from("Manual note names (comma-separated, optional):"));
                lines.push(Line::from(format!("> {line}")));
            }
            OptSub::Fee { line } => {
                lines.push(Line::from("Fee override (empty for auto):"));
                lines.push(Line::from(format!("> {line}")));
            }
            OptSub::AllowLowFee { sel } => {
                lines.push(Line::from("Allow fee below estimated minimum?"));
                list_inline(&mut lines, BOOL, *sel);
            }
            OptSub::Refund { line } => {
                lines.push(Line::from("Refund PKH (optional):"));
                lines.push(Line::from(format!("> {line}")));
            }
            OptSub::Index { line } => {
                lines.push(Line::from("Signing key index (optional):"));
                lines.push(Line::from(format!("> {line}")));
            }
            OptSub::Hardened { sel } => {
                lines.push(Line::from("Hardened signing key?"));
                list_inline(&mut lines, BOOL, *sel);
            }
            OptSub::IncludeData { sel } => {
                lines.push(Line::from("Include note data in output?"));
                list_inline(&mut lines, BOOL, *sel);
            }
            OptSub::SignKeys { line } => {
                lines.push(Line::from("Extra --sign-key entries (comma-separated):"));
                lines.push(Line::from(format!("> {line}")));
            }
            OptSub::SaveRaw { sel } => {
                lines.push(Line::from("Save raw tx jam (debug)?"));
                list_inline(&mut lines, BOOL, *sel);
            }
            OptSub::NoteSelection { sel } => {
                lines.push(Line::from(
                    "Note selection order — Enter submits transaction",
                ));
                list_inline(&mut lines, NOTE_ORDER, *sel);
                lines.push(Line::from(format!("  {spin} Ready to plan & execute")));
            }
        },
    }
    let p = Paragraph::new(lines).wrap(Wrap { trim: true });
    f.render_widget(p, area);
}
