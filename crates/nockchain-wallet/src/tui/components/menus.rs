//! Static menu labels and error-action lists for list widgets and command_runner.

pub(crate) const MAIN_MENU: &[&str] = &[
    "Keys & addresses", "Notes & balance", "Transactions", "Watch-only", "Sign / verify",
    "Settings & help", "Quick commands (command line)", "Exit",
];

pub(crate) const KEYS_MENU: &[&str] = &[
    "Keygen", "Derive child key", "Import keys (file / extended key / seed)", "Export keys",
    "Show seed phrase", "Show master zpub", "Show master zprv", "Show key tree",
    "List active addresses", "List master addresses", "Set active master address",
    "Import master pubkey", "Export master pubkey", "Back",
];

pub(crate) const IMPORT_SRC: &[&str] = &["File", "Extended key", "Seed phrase", "Back"];

pub(crate) const NOTES_MENU: &[&str] = &[
    "List all notes", "List notes by address (required)", "List notes by address (CSV)",
    "Show balance", "Back",
];

pub(crate) const TX_MENU: &[&str] = &[
    "Create transaction (planner)", "Send transaction file", "Show transaction file",
    "Sign multisig transaction", "Migrate v0 notes", "Register .nock name (NNS)", "Back",
];

pub(crate) const WATCH_MENU: &[&str] = &["Address or pubkey", "Pubkey only", "Multisig", "Back"];

pub(crate) const SIGN_MENU: &[&str] =
    &["Sign message", "Verify message", "Sign hash", "Verify hash", "Back"];

pub(crate) const SETTINGS_MENU: &[&str] = &[
    "Public gRPC server", "JSON API listen", "API token & curl examples", "Show help again",
    "Verbose / logging info", "Back",
];

pub(crate) const BOOL: &[&str] = &["Yes", "No"];

pub(crate) const NOTE_ORDER: &[&str] = &["Ascending", "Descending"];

pub(crate) const CT_ERR_ACTIONS: &[&str] = &[
    "Retry", "Edit planning options", "Start over (new recipients)", "Back to Transactions menu",
];

pub(crate) const GENERIC_ERR: &[&str] = &["Retry", "Back"];
