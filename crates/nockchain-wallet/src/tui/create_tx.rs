//! Create-tx wizard state (TUI TUI).

use crate::command::{Commands, NoteSelectionStrategyCli};
use crate::recipient::RecipientSpecToken;

/// Sub-steps while collecting one recipient.
#[derive(Debug, Clone)]
pub(crate) enum RecSub {
    Address {
        line: String,
    },
    Amount {
        addr: String,
        line: String,
    },
    Memo {
        addr: String,
        amount: u64,
        line: String,
    },
    Blob {
        addr: String,
        amount: u64,
        memo: Option<String>,
        line: String,
    },
    AddAnother {
        sel: usize,
    },
}

/// Option wizard substeps.
#[derive(Debug, Clone)]
pub(crate) enum OptSub {
    Names { line: String },
    Fee { line: String },
    AllowLowFee { sel: usize },
    Refund { line: String },
    Index { line: String },
    Hardened { sel: usize },
    IncludeData { sel: usize },
    SignKeys { line: String },
    SaveRaw { sel: usize },
    NoteSelection { sel: usize },
}

#[derive(Debug, Clone)]
pub(crate) enum Phase {
    Recipients {
        list: Vec<RecipientSpecToken>,
        sub: RecSub,
    },
    Options {
        recipients: Vec<RecipientSpecToken>,
        names: Option<String>,
        fee: Option<u64>,
        allow_low_fee: bool,
        refund_pkh: Option<String>,
        index: Option<u64>,
        hardened: bool,
        include_data: bool,
        sign_keys: Vec<String>,
        save_raw_tx: bool,
        note_selection_strategy: NoteSelectionStrategyCli,
        sub: OptSub,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct CreateTxWizard {
    pub phase: Phase,
    pub status: Option<String>,
}

impl CreateTxWizard {
    pub fn new() -> Self {
        Self {
            phase: Phase::Recipients {
                list: Vec::new(),
                sub: RecSub::Address {
                    line: String::new(),
                },
            },
            status: None,
        }
    }

    pub fn from_command(cmd: &Commands) -> Option<Self> {
        let Commands::CreateTx {
            names,
            recipients,
            fee,
            allow_low_fee,
            refund_pkh,
            index,
            hardened,
            include_data,
            sign_keys,
            save_raw_tx,
            note_selection_strategy,
        } = cmd
        else {
            return None;
        };
        Some(Self {
            phase: Phase::Options {
                recipients: recipients.clone(),
                names: names.clone(),
                fee: *fee,
                allow_low_fee: *allow_low_fee,
                refund_pkh: refund_pkh.clone(),
                index: *index,
                hardened: *hardened,
                include_data: *include_data,
                sign_keys: sign_keys.clone(),
                save_raw_tx: *save_raw_tx,
                note_selection_strategy: *note_selection_strategy,
                sub: OptSub::Names {
                    line: names.clone().unwrap_or_default(),
                },
            },
            status: None,
        })
    }

    pub fn title_line(&self) -> &'static str {
        match &self.phase {
            Phase::Recipients { .. } => "Create transaction — recipients",
            Phase::Options { .. } => "Create transaction — options",
        }
    }

    pub fn build_command(&self) -> Option<Commands> {
        let Phase::Options {
            recipients,
            names,
            fee,
            allow_low_fee,
            refund_pkh,
            index,
            hardened,
            include_data,
            sign_keys,
            save_raw_tx,
            note_selection_strategy,
            sub: OptSub::NoteSelection { .. },
        } = &self.phase
        else {
            return None;
        };
        Some(Commands::CreateTx {
            names: names.clone(),
            recipients: recipients.clone(),
            fee: *fee,
            allow_low_fee: *allow_low_fee,
            refund_pkh: refund_pkh.clone(),
            index: *index,
            hardened: *hardened,
            include_data: *include_data,
            sign_keys: sign_keys.clone(),
            save_raw_tx: *save_raw_tx,
            note_selection_strategy: *note_selection_strategy,
        })
    }
}
