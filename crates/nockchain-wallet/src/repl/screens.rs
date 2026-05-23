//! REPL TUI screen state.

use crate::command::Commands;

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
    /// Branded welcome; any key returns to the main menu.
    Splash,
    Main {
        sel: usize,
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
        title: String,
        value: String,
        then: TextThen,
    },
    Confirm {
        title: String,
        sel: usize,
        labels: &'static [&'static str],
        then: ConfirmThen,
    },
    CreateTx {
        w: super::create_tx::CreateTxWizard,
    },
    ExitConfirm {
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
    NnsRegisterName,
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
