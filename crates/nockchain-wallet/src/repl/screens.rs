//! REPL TUI screen state.

use crate::command::Commands;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NnsBuyFocus {
    Name,
    Search,
    Cancel,
    Register,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SendSimpleFocus {
    Amount,
    Recipient,
    Cancel,
    Continue,
}

#[derive(Debug, Clone)]
pub(crate) enum SendSimplePhase {
    Form,
    Planning,
    Review {
        cmd: Commands,
        preview: String,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReplControl {
    Continue,
    Quit,
}

#[derive(Debug, Clone)]
pub(crate) enum ErrorCtx {
    Retry(Commands),
    CreateTx { cmd: Commands },
}

#[derive(Debug, Clone)]
pub(crate) enum Screen {
    /// Branded welcome; any key returns to home.
    Splash,
    /// Wallet dashboard (Wallet / Menu tabs).
    Home,
    Receive {
        address: Option<String>,
        loading: bool,
        error: Option<String>,
        copy_focused: bool,
    },
    NnsBuy {
        value: String,
        cursor: usize,
        focus: NnsBuyFocus,
        status: Option<String>,
        lookup_busy: bool,
        /// Set after a successful search for the current normalized name.
        verified_name: Option<String>,
    },
    /// Simple send form from home (amount + address + giant buttons).
    SendSimple {
        amount: String,
        recipient: String,
        amount_cursor: usize,
        recipient_cursor: usize,
        focus: SendSimpleFocus,
        phase: SendSimplePhase,
        status: Option<String>,
        review_scroll: u16,
    },
    Keys {
        sel: usize,
    },
    KeysImport {
        sel: usize,
    },
    Notes {
        sel: usize,
    },
    Transactions {
        sel: usize,
    },
    Watch {
        sel: usize,
    },
    SignVerify {
        sel: usize,
    },
    Settings {
        sel: usize,
    },
    Quick {
        line: String,
    },
    TextPrompt {
        underlay: Box<Screen>,
        title: String,
        value: String,
        then: TextThen,
    },
    Confirm {
        underlay: Box<Screen>,
        title: String,
        sel: usize,
        labels: &'static [&'static str],
        then: ConfirmThen,
    },
    CreateTx {
        w: super::create_tx::CreateTxWizard,
    },
    ExitConfirm {
        underlay: Box<Screen>,
        sel: usize,
    },
    ErrorScreen {
        msg: String,
        sel: usize,
        actions: &'static [&'static str],
        ctx: ErrorCtx,
    },
    /// Wallet command in progress (async job); `restore` is the screen to return to on completion.
    Running {
        label: String,
        restore: Box<Screen>,
        cmd: Commands,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum TextThen {
    /// First prompt: parse u64 index, then ask hardened (Confirm).
    KeysDeriveIndex,
    /// After hardened choice + optional label line, run derive.
    KeysDeriveRun {
        index: u64,
        hardened: bool,
    },
    KeysImportFile,
    KeysImportExtended,
    KeysImportSeed,
    KeysImportSeedVersion {
        seed: String,
    },
    KeysSetActive,
    KeysImportMaster,
    NotesListByAddr,
    NotesListCsv,
    TxSendPath,
    TxShowPath,
    TxSignMultisigTxFile,
    TxSignMultisigKeys {
        transaction: String,
    },
    TxMultisigThreshold,
    TxMultisigParticipants {
        threshold: u64,
    },
    TxMigrateDest,
    SettingsGrpcEndpoint,
    SettingsApiListen,
    WatchAddr,
    WatchPubkey,
    SignMsgStepMessage,
    SignMsgStepIndex {
        message: String,
    },
    VerifyMsgM,
    VerifyMsgS {
        message: String,
    },
    VerifyMsgP {
        message: String,
        sig_path: String,
    },
    SignHashGetHash,
    SignHashIndex {
        hash_b58: String,
    },
    VerifyHashFirst,
    VerifyHashSig {
        hash_b58: String,
    },
    VerifyHashPk {
        hash_b58: String,
        sig_path: String,
    },
}

#[derive(Debug, Clone)]
pub(crate) enum ConfirmThen {
    /// "Hardened?" — Yes at sel 0.
    KeysDeriveAfterIndex {
        index: u64,
    },
    KeysKeyTree,
    SignMsgHardened {
        message: Option<String>,
        message_file: Option<String>,
        message_pos: Option<String>,
        index: Option<u64>,
    },
    SignHashHardened {
        hash_b58: String,
        index: Option<u64>,
    },
    NnsRegisterConfirm {
        name: String,
    },
}

impl Screen {
    pub(crate) fn receive_new(loading: bool) -> Self {
        Screen::Receive {
            address: None,
            loading,
            error: None,
            copy_focused: true,
        }
    }

    pub(crate) fn nns_buy_new() -> Self {
        Screen::NnsBuy {
            value: String::new(),
            cursor: 0,
            focus: NnsBuyFocus::Name,
            status: None,
            lookup_busy: false,
            verified_name: None,
        }
    }

    /// Sub-flow screens hide the home tab bar.
    pub(crate) fn hides_home_tabs(&self) -> bool {
        !matches!(self, Screen::Home)
    }
}
