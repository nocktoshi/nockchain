use clap::{Parser, Subcommand};
use nockapp::driver::Operation;
use nockapp::kernel::boot::Cli as BootCli;
use nockapp::noun::slab::NounSlab;
use nockapp::wire::{Wire, WireRepr};
use nockapp::NockAppError;
use nockvm::noun::{Noun, D, T};

use crate::connection::ConnectionCli;
/// Represents a timelock range with optional min and max page numbers
#[derive(Debug, Clone)]
pub struct TimelockRange {
    pub min: Option<u64>,
    pub max: Option<u64>,
}

impl TimelockRange {
    pub fn new(min: Option<u64>, max: Option<u64>) -> Self {
        Self { min, max }
    }

    pub fn none() -> Self {
        Self {
            min: None,
            max: None,
        }
    }

    /// Convert to noun representation: [min=(unit page-number) max=(unit page-number)]
    pub fn to_noun(&self, slab: &mut NounSlab) -> Noun {
        let min_noun = match self.min {
            Some(val) => T(slab, &[D(0), D(val)]), // unit: [~ value]
            None => D(0),                          // unit: ~
        };
        let max_noun = match self.max {
            Some(val) => T(slab, &[D(0), D(val)]), // unit: [~ value]
            None => D(0),                          // unit: ~
        };
        T(slab, &[min_noun, max_noun])
    }
}

/// Represents a timelock intent - optional constraint for output notes
#[derive(Debug, Clone)]
pub struct TimelockIntent {
    pub absolute: Option<TimelockRange>,
    pub relative: Option<TimelockRange>,
}

impl TimelockIntent {
    pub fn none() -> Self {
        Self {
            absolute: None,
            relative: None,
        }
    }

    pub fn absolute_only(range: TimelockRange) -> Self {
        Self {
            absolute: Some(range),
            relative: Some(TimelockRange::none()),
        }
    }

    pub fn relative_only(range: TimelockRange) -> Self {
        Self {
            absolute: Some(TimelockRange::none()),
            relative: Some(range),
        }
    }

    /// Convert to noun representation: (unit [absolute=timelock-range relative=timelock-range])
    pub fn to_noun(&self, slab: &mut NounSlab) -> Noun {
        match (&self.absolute, &self.relative) {
            (None, None) => D(0), // unit: ~ (no intent)
            (abs, rel) => {
                let default_abs_range = TimelockRange::none();
                let default_rel_range = TimelockRange::none();
                let abs_range = abs.as_ref().unwrap_or(&default_abs_range);
                let rel_range = rel.as_ref().unwrap_or(&default_rel_range);

                let abs_noun = abs_range.to_noun(slab);
                let rel_noun = rel_range.to_noun(slab);
                let content = T(slab, &[abs_noun, rel_noun]);
                T(slab, &[D(0), content]) // unit: [~ content]
            }
        }
    }
}

#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct WalletCli {
    #[command(flatten)]
    pub boot: BootCli,

    #[command(flatten)]
    pub connection: ConnectionCli,

    /// Include watch-only pubkeys when synchronizing wallet balance
    #[arg(long, global = true, default_value_t = false)]
    pub include_watch_only: bool,

    #[command(subcommand)]
    pub command: Commands,
}

#[derive(clap::ValueEnum, Debug, Clone, PartialEq, Eq)]
pub enum ClientType {
    Public,
    Private,
}

#[derive(Debug)]
pub enum WalletWire {
    ListNotes,
    UpdateBalance,
    UpdateBlock,
    Exit,
    Command(Commands),
}

impl Wire for WalletWire {
    const VERSION: u64 = 1;
    const SOURCE: &str = "wallet";

    fn to_wire(&self) -> WireRepr {
        let tags = match self {
            WalletWire::ListNotes => vec!["list-notes".into()],
            WalletWire::UpdateBalance => vec!["update-balance".into()],
            WalletWire::UpdateBlock => vec!["update-block".into()],
            WalletWire::Exit => vec!["exit".into()],
            WalletWire::Command(command) => {
                vec!["command".into(), command.as_wire_tag().into()]
            }
        };
        WireRepr::new(WalletWire::SOURCE, WalletWire::VERSION, tags)
    }
}

/// Represents a Noun that the wallet kernel can handle
pub type CommandNoun<T> = Result<(T, Operation), NockAppError>;

fn validate_label(s: &str) -> Result<String, String> {
    if s.chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        Ok(s.to_string())
    } else {
        Err("Label must contain only lowercase letters, numbers, and hyphens".to_string())
    }
}

#[derive(Subcommand, Debug, Clone)]
pub enum Commands {
    /// Generate a new key pair
    Keygen,

    /// Derive child key (pub, private or both) from the current master key
    DeriveChild {
        /// Index of the child key to derive, should be in range [0, 2^31)
        #[arg(value_parser = clap::value_parser!(u64).range(0..2 << 31))]
        index: u64,

        /// Hardened or unhardened child key
        #[arg(short, long)]
        hardened: bool,

        /// Label for the child key
        #[arg(short, long, value_parser = validate_label, default_value = None)]
        label: Option<String>,
    },

    /// Import keys from a file, extended key, seed phrase, or master private key
    #[command(group = clap::ArgGroup::new("import_source").required(true).args(&["file", "key", "seedphrase", "master_privkey", "watch_only_pubkey"]))]
    ImportKeys {
        /// Path to the jammed keys file
        #[arg(short = 'f', long = "file", value_name = "FILE")]
        file: Option<String>,

        /// Extended key string (e.g., "zprv..." or "zpub...")
        #[arg(short = 'k', long = "key", value_name = "EXTENDED_KEY")]
        key: Option<String>,

        /// Seed phrase to generate master private key
        #[arg(short = 's', long = "seedphrase", value_name = "SEEDPHRASE")]
        seedphrase: Option<String>,

        /// Master private key (base58-encoded) - requires --chain-code
        #[arg(short = 'm', long = "master-privkey", value_name = "MASTER_PRIVKEY")]
        master_privkey: Option<String>,

        /// Pubkey (watch only)
        #[arg(short = 'c', long = "watch-only", value_name = "WATCH_ONLY")]
        watch_only_pubkey: Option<String>,

        /// Chain code (base58-encoded) - required with --master-privkey
        #[arg(short = 'c', long = "chain-code", value_name = "CHAIN_CODE")]
        chain_code: Option<String>,
    },

    /// Export keys to a file
    ExportKeys,

    /// Perform a simple scan of the blockchain
    Scan {
        /// Master public key to scan for
        #[arg(short, long)]
        master_pubkey: String,
        /// Optional search depth (default 100)
        #[arg(short, long, default_value = "100")]
        search_depth: u64,
        /// Include timelocks in scan
        #[arg(long, default_value = "false")]
        include_timelocks: bool,
        /// Include multisig in scan
        #[arg(long, default_value = "false")]
        include_multisig: bool,
    },

    /// List all notes in the wallet
    ListNotes,

    /// List notes by public key
    ListNotesByPubkey {
        /// Optional public key to filter notes
        pubkey: Option<String>,
    },

    /// List notes by public key in CSV format
    ListNotesByPubkeyCsv {
        /// Public key to filter notes
        pubkey: String,
    },

    /// Create a transaction from a transaction file
    SendTx {
        /// Transaction file to create transaction from
        transaction: String,
    },

    /// Display a transaction file contents
    ShowTx {
        /// Transaction file to display
        transaction: String,
    },

    /// Query whether a transaction was accepted by the node
    TxAccepted {
        /// Base58-encoded transaction ID
        #[arg(value_name = "TX_ID")]
        tx_id: String,
    },

    /// Signs a transaction (for multisigs only)
    SignTx {
        /// Path to input bundle file
        transaction: String,

        /// Optional key index to use for signing [0, 2^31)
        #[arg(short, long, value_parser = clap::value_parser!(u64).range(0..2 << 31))]
        index: Option<u64>,
        /// Hardened or unhardened child key
        #[arg(short, long, default_value = "false")]
        hardened: bool,
    },

    /// Create a transaction
    CreateTx {
        /// Names of notes to spend (comma-separated)
        #[arg(long)]
        names: String,
        /// Recipient addresses (comma-separated)
        #[arg(long)]
        recipients: String,
        /// Amounts to send (comma-separated)
        #[arg(long)]
        gifts: String,
        /// Transaction fee
        #[arg(long)]
        fee: u64,
        /// Optional key index to use for signing [0, 2^31), if not provided, we use the master key
        #[arg(short, long, value_parser = clap::value_parser!(u64).range(0..2 << 31))]
        index: Option<u64>,
        /// Type of timelock intent: "absolute", "relative", or "none"
        #[arg(long, default_value = "none")]
        timelock_intent: String,
        /// Minimum block height for absolute timelock or relative delay in blocks
        #[arg(long)]
        timelock_min: Option<u64>,
        /// Hardened or unhardened child key
        #[arg(short, long, default_value = "false")]
        hardened: bool,
    },

    /// Export a master public key
    ExportMasterPubkey,

    /// Import a master public key
    ImportMasterPubkey {
        // Path to keys file generated from export-master-pubkey
        key_path: String,
    },

    /// Lists all public keys in the wallet
    ListPubkeys,

    /// Show the seed phrase for the current master key
    ShowSeedphrase,

    /// Show the master public key
    ShowMasterPubkey,

    /// Show the master private key
    ShowMasterPrivkey,

    /// Fetch confirmation depth for a transaction ID
    // Confirmations {
    //     /// Base58-encoded transaction ID
    //     #[arg(value_name = "TX_ID")]
    //     tx_id: String,
    // },

    /// Sign an arbitrary message
    #[command(group = clap::ArgGroup::new("message_source").required(true).args(&["message", "message_file", "message_pos"]))]
    SignMessage {
        /// Message to sign (raw string)
        #[arg(short = 'm', long = "message", group = "message_source")]
        message: Option<String>,

        /// Path to file containing raw bytes to sign
        #[arg(short = 'f', long = "message-file", group = "message_source")]
        message_file: Option<String>,

        /// Positional message to sign (equivalent to --message)
        #[arg(value_name = "MESSAGE", group = "message_source")]
        message_pos: Option<String>,

        /// Optional key index to use for signing [0, 2^31)
        #[arg(short, long, value_parser = clap::value_parser!(u64).range(0..2 << 31))]
        index: Option<u64>,
        /// Hardened or unhardened child key
        #[arg(short, long, default_value = "false")]
        hardened: bool,
    },

    /// Sign an already-computed tip5 hash (base58)
    SignHash {
        /// Positional base58-encoded tip5 hash to sign
        #[arg(value_name = "HASH")]
        hash_b58: String,

        /// Optional key index to use for signing [0, 2^31)
        #[arg(short, long, value_parser = clap::value_parser!(u64).range(0..2 << 31))]
        index: Option<u64>,
        /// Hardened or unhardened child key
        #[arg(short, long, default_value = "false")]
        hardened: bool,
    },

    /// Verify an arbitrary message signature
    VerifyMessage {
        /// Message to verify (raw string)
        #[arg(short = 'm', long = "message")]
        message: Option<String>,

        /// Path to file containing raw bytes of message to verify
        #[arg(short = 'f', long = "message-file")]
        message_file: Option<String>,

        /// Positional message to verify (equivalent to --message)
        #[arg(value_name = "MESSAGE", conflicts_with_all = ["message", "message_file"])]
        message_pos: Option<String>,

        /// Path to jammed signature file produced by sign-message
        #[arg(short = 's', long = "signature")]
        signature_path: Option<String>,

        /// Positional signature path (equivalent to --signature)
        #[arg(value_name = "SIGNATURE_FILE")]
        signature_pos: Option<String>,

        /// Base58-encoded schnorr public key
        #[arg(short = 'p', long = "pubkey")]
        pubkey: Option<String>,

        /// Positional public key (equivalent to --pubkey)
        #[arg(value_name = "PUBKEY")]
        pubkey_pos: Option<String>,
    },

    /// Verify a signature against an already-computed tip5 hash (base58)
    VerifyHash {
        /// Positional base58-encoded tip5 hash
        #[arg(value_name = "HASH")]
        hash_b58: String,

        /// Path to jammed signature file produced by signing
        #[arg(short = 's', long = "signature")]
        signature_path: Option<String>,
        /// Positional signature path
        #[arg(value_name = "SIGNATURE_FILE")]
        signature_pos: Option<String>,

        /// Base58-encoded schnorr public key
        #[arg(short = 'p', long = "pubkey")]
        pubkey: Option<String>,
        /// Positional public key
        #[arg(value_name = "PUBKEY")]
        pubkey_pos: Option<String>,
    },
}

impl Commands {
    fn as_wire_tag(&self) -> &'static str {
        match self {
            Commands::Keygen => "keygen",
            Commands::DeriveChild { .. } => "derive-child",
            Commands::ImportKeys { .. } => "import-keys",
            Commands::ExportKeys => "export-keys",
            Commands::SignTx { .. } => "sign-tx",
            Commands::Scan { .. } => "scan",
            Commands::ListNotes => "list-notes",
            Commands::ListNotesByPubkey { .. } => "list-notes-by-pubkey",
            Commands::ListNotesByPubkeyCsv { .. } => "list-notes-by-pubkey-csv",
            Commands::CreateTx { .. } => "create-tx",
            Commands::SendTx { .. } => "send-tx",
            Commands::ShowTx { .. } => "show-tx",
            Commands::ExportMasterPubkey => "export-master-pubkey",
            Commands::ImportMasterPubkey { .. } => "import-master-pubkey",
            Commands::ListPubkeys => "list-pubkeys",
            Commands::ShowSeedphrase => "show-seedphrase",
            Commands::ShowMasterPubkey => "show-master-pubkey",
            Commands::ShowMasterPrivkey => "show-master-privkey",
            Commands::SignMessage { .. } => "sign-message",
            Commands::VerifyMessage { .. } => "verify-message",
            Commands::SignHash { .. } => "sign-hash",
            Commands::VerifyHash { .. } => "verify-hash",
            Commands::TxAccepted { .. } => "tx-accepted",
        }
    }
}
