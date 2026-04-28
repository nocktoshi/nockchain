use std::str::FromStr;

use clap::builder::BoolishValueParser;
use clap::{ArgAction, Parser, Subcommand, ValueEnum};
use nockapp::driver::Operation;
use nockapp::kernel::boot::Cli as BootCli;
use nockapp::wire::{Wire, WireRepr};
use nockapp::NockAppError;
use nockchain_math::belt::Belt;
use nockchain_types::tx_engine::v0;

use crate::connection::ConnectionCli;
use crate::recipient::{parse_recipient_arg, RecipientSpecToken};

/// CLI helper that captures optional lower and upper bounds for timelocks.
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TimelockRangeCli {
    min: Option<u64>,
    max: Option<u64>,
}

#[allow(dead_code)]
impl TimelockRangeCli {
    pub fn absolute(&self) -> v0::TimelockRangeAbsolute {
        v0::TimelockRangeAbsolute::new(
            self.min.map(|value| v0::BlockHeight(Belt(value))),
            self.max.map(|value| v0::BlockHeight(Belt(value))),
        )
    }

    pub fn relative(&self) -> v0::TimelockRangeRelative {
        v0::TimelockRangeRelative::new(
            self.min.map(|value| v0::BlockHeightDelta(Belt(value))),
            self.max.map(|value| v0::BlockHeightDelta(Belt(value))),
        )
    }

    pub fn has_upper_bound(&self) -> bool {
        self.max.is_some()
    }

    pub fn from_bounds(min: Option<u64>, max: Option<u64>) -> Result<Self, String> {
        if let (Some(lo), Some(hi)) = (min, max) {
            if lo > hi {
                return Err(format!(
                    "timelock range must have min <= max, got {}..{}",
                    lo, hi
                ));
            }
        }

        Ok(Self { min, max })
    }

    fn parse_bound(component: &str) -> Result<Option<u64>, String> {
        let trimmed = component.trim();
        if trimmed.is_empty() {
            Ok(None)
        } else {
            trimmed
                .parse::<u64>()
                .map(Some)
                .map_err(|err| format!("invalid timelock bound '{}': {}", trimmed, err))
        }
    }
}

#[doc = include_str!("docs/usage/timelock-intent.doc.txt")]
#[allow(dead_code)]
#[derive(Debug, Clone, Default)]
pub struct TimelockIntentCli {
    absolute: Option<TimelockRangeCli>,
    relative: Option<TimelockRangeCli>,
}

#[allow(dead_code)]
impl TimelockIntentCli {
    pub fn absolute_range(&self) -> Option<v0::TimelockRangeAbsolute> {
        self.absolute.as_ref().map(|range| range.absolute())
    }

    pub fn relative_range(&self) -> Option<v0::TimelockRangeRelative> {
        self.relative.as_ref().map(|range| range.relative())
    }

    pub fn has_upper_bound(&self) -> bool {
        self.absolute
            .as_ref()
            .is_some_and(TimelockRangeCli::has_upper_bound)
            || self
                .relative
                .as_ref()
                .is_some_and(TimelockRangeCli::has_upper_bound)
    }
}

impl FromStr for TimelockIntentCli {
    type Err = String;

    fn from_str(spec: &str) -> Result<Self, Self::Err> {
        let trimmed = spec.trim();
        if trimmed.is_empty() {
            return Err("timelock spec cannot be empty".into());
        }

        let mut intent = TimelockIntentCli::default();
        for part in trimmed.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            if let Some(rest) = part.strip_prefix("absolute=") {
                if intent.absolute.is_some() {
                    return Err("absolute timelock specified more than once".into());
                }
                intent.absolute = Some(rest.parse()?);
            } else if let Some(rest) = part.strip_prefix("relative=") {
                if intent.relative.is_some() {
                    return Err("relative timelock specified more than once".into());
                }
                intent.relative = Some(rest.parse()?);
            } else {
                if intent.absolute.is_some() {
                    return Err(
                        "ambiguous timelock spec; prefix additional ranges with 'absolute=' or 'relative='"
                            .into(),
                    );
                }
                intent.absolute = Some(part.parse()?);
            }
        }

        if intent.absolute.is_none() && intent.relative.is_none() {
            return Err(
                "timelock spec must include an absolute=... or relative=... component".into(),
            );
        }

        Ok(intent)
    }
}

impl FromStr for TimelockRangeCli {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let trimmed = s.trim();
        if trimmed.is_empty() {
            return Err("timelock range cannot be empty".into());
        }

        if let Some((min_str, max_str)) = trimmed.split_once("..") {
            let min = Self::parse_bound(min_str)?;
            let max = Self::parse_bound(max_str)?;
            TimelockRangeCli::from_bounds(min, max)
        } else {
            let min = Self::parse_bound(trimmed)?;
            TimelockRangeCli::from_bounds(min, None)
        }
    }
}

/// CLI-facing note selection strategy for create-tx ordering.
#[derive(Copy, Clone, Debug, ValueEnum)]
pub enum NoteSelectionStrategyCli {
    Ascending,
    Descending,
}

impl NoteSelectionStrategyCli {
    pub fn tas_label(&self) -> &'static str {
        match self {
            NoteSelectionStrategyCli::Ascending => "asc",
            NoteSelectionStrategyCli::Descending => "desc",
        }
    }
}

/// Top-level wallet CLI definition.
#[derive(Parser, Debug, Clone)]
#[command(author, version, about, long_about = None)]
pub struct WalletCli {
    #[command(flatten)]
    pub boot: BootCli,

    #[arg(long, default_value = "false")]
    pub fakenet: bool,

    #[command(flatten)]
    pub connection: ConnectionCli,

    #[command(subcommand)]
    pub command: Commands,
}

/// Supported watch subcommands for addresses and lock forms.
#[derive(Subcommand, Debug, Clone)]
pub enum WatchSubcommand {
    /// Add a watch-only address (base58 pkh or schnorr pubkey)
    Address {
        #[arg(
            value_name = "address",
            help = "Base58-encoded address or schnorr pubkey"
        )]
        address: String,
    },
    /// Add a watch-only schnorr pubkey
    Pubkey {
        #[arg(
            value_name = "pubkey",
            help = "Base58-encoded schnorr pubkey"
        )]
        pubkey: String,
    },
    /// Import a multisig lock for watch-only tracking
    Multisig {
        #[arg(
            short = 't',
            long = "threshold",
            help = "Threshold (m) value for the m-of-n multisig"
        )]
        threshold: u64,
        #[arg(
            long,
            help = "Comma-separated list of base58 pubkey hashes for the multisig"
        )]
        participants: String,
    },
    //FirstName {
    //    #[arg(value_name = "first-name")]
    //    first_name: String,
    //},
}

/// gRPC client mode used for wallet network operations.
#[derive(clap::ValueEnum, Debug, Clone, PartialEq, Eq)]
pub enum ClientType {
    Public,
    Private,
}

/// Internal wallet event wires used for nockapp routing.
#[derive(Debug)]
#[allow(dead_code)]
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

/// Validates label strings accepted by key-derivation CLI paths.
fn validate_label(s: &str) -> Result<String, String> {
    if s.chars()
        .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        Ok(s.to_string())
    } else {
        Err("Label must contain only lowercase letters, numbers, and hyphens".to_string())
    }
}

/// Wallet command surface for key, note, and transaction operations.
#[derive(Subcommand, Debug, Clone)]
pub enum Commands {
    /// Generates a new version 1 key pair
    Keygen,

    /// Derive child key (pub, private or both) from the current master key
    DeriveChild {
        #[arg(
            value_parser = clap::value_parser!(u64).range(0..2 << 31),
            help = "Index of the child key to derive, should be in range [0, 2^31)"
        )]
        index: u64,

        #[arg(long, help = "Hardened or unhardened child key")]
        hardened: bool,

        #[arg(
            short,
            long,
            value_parser = validate_label,
            default_value = None,
            help = "Label for the child key"
        )]
        label: Option<String>,
    },

    /// Import keys from a file, extended key, seed phrase, or master private key
    #[command(group = clap::ArgGroup::new("import_source").required(true).args(&["file", "key", "seedphrase"]))]
    ImportKeys {
        #[arg(
            short = 'f',
            long = "file",
            value_name = "FILE",
            help = "Path to the jammed keys file"
        )]
        file: Option<String>,

        #[arg(
            short = 'k',
            long = "key",
            value_name = "EXTENDED_KEY",
            help = "Extended key string (e.g., \"zprv...\" or \"zpub...\")"
        )]
        key: Option<String>,

        #[arg(
            short = 's',
            long = "seedphrase",
            value_name = "SEEDPHRASE",
            help = include_str!("docs/usage/import-keys.arg.seedphrase.txt")
        )]
        seedphrase: Option<String>,

        #[arg(
            long = "version",
            value_name = "VERSION",
            requires = "seedphrase",
            help = "Master key version to use when generating from seed phrase"
        )]
        version: Option<u64>,
    },

    /// Watch addresses, pubkeys, multisigs, or first-names
    Watch {
        #[command(subcommand)]
        subcommand: WatchSubcommand,
    },

    /// Export keys to a file
    ExportKeys,

    /// List all notes in the wallet
    ListNotes,

    /// List notes by public key
    ListNotesByAddress {
        #[arg(help = "Optional public key to filter notes")]
        address: Option<String>,
    },

    /// List notes by public key in CSV format
    ListNotesByAddressCsv {
        #[arg(help = "Public key to filter notes")]
        address: String,
    },

    /// Create a transaction from a transaction file
    SendTx {
        #[arg(help = "Transaction file to create transaction from")]
        transaction: String,
    },

    /// Display a transaction file contents
    ShowTx {
        #[arg(help = "Transaction file to display")]
        transaction: String,
    },

    /// Summarize the wallet balance
    ShowBalance,

    /// Query whether a transaction was accepted by the node
    TxAccepted {
        #[arg(
            value_name = "TX_ID",
            help = "Base58-encoded transaction ID"
        )]
        tx_id: String,
    },

    /// Create a transaction (use --refund-pkh when spending legacy v0 notes)
    #[command(
        name = "create-tx",
        override_usage = include_str!("docs/usage/create-tx.override_usage.txt")
    )]
    CreateTx {
        #[arg(
            long,
            help = "Optional names of notes to spend (comma-separated) for manual selection."
        )]
        names: Option<String>,
        #[arg(
            long = "recipient",
            value_name = "RECIPIENT",
            value_parser = parse_recipient_arg,
            action = ArgAction::Append,
            help = "Recipient specifications (repeat --recipient for each output)"
        )]
        recipients: Vec<RecipientSpecToken>,
        #[arg(long, help = "Optional transaction fee override.")]
        fee: Option<u64>,
        #[arg(
            long,
            default_value = "false",
            help = "Allow fees below the estimated minimum (unsafe, testing only)"
        )]
        allow_low_fee: bool,
        #[arg(
            long = "refund-pkh",
            value_name = "REFUND_PKH",
            help = "Optional refund recipient pubkey hash (base58). Required for legacy v0 notes; v1 notes default to the note owner."
        )]
        refund_pkh: Option<String>,
        #[arg(
            short,
            long,
            value_parser = clap::value_parser!(u64).range(0..2 << 31),
            help = "Optional key index to use for signing [0, 2^31), if not provided, we use the master key"
        )]
        index: Option<u64>,
        #[arg(
            long,
            default_value = "false",
            help = "Hardened or unhardened child key"
        )]
        hardened: bool,
        #[arg(
            long,
            action = ArgAction::Set,
            value_parser = BoolishValueParser::new(),
            default_value_t = true,
            help = "Include note data in output note"
        )]
        include_data: bool,
        #[arg(
            long = "sign-key",
            value_name = "INDEX[:HARDENED]",
            action = ArgAction::Append,
            help = "Additional signing keys. Accepts `index` or `index:hardened`."
        )]
        sign_keys: Vec<String>,
        #[arg(
            long,
            default_value = "false",
            help = "For debugging purposes. If true, the raw-tx jam will be saved in the txs-debug folder in the current working directory."
        )]
        save_raw_tx: bool,
        #[arg(
            long = "note-selection",
            value_enum,
            default_value = "ascending",
            help = "Note selection strategy (ascending selects smallest notes first)"
        )]
        note_selection_strategy: NoteSelectionStrategyCli,
    },

    /// Sweep all spendable legacy v0 notes into one v1 destination address.
    #[command(name = "migrate-v0-notes")]
    MigrateV0Notes {
        #[arg(
            long = "destination",
            value_name = "DESTINATION",
            help = "Base58-encoded v1 pay-to-pubkey-hash address that receives the migrated funds."
        )]
        destination: String,
    },

    /// Sign a multisig transaction
    SignMultisigTx {
        #[arg(help = "Path to transaction file")]
        transaction: String,
        #[arg(
            long,
            help = "Comma-separated list of key indices to sign with (format: index:hardened). If not provided, uses master key."
        )]
        sign_keys: Option<String>,
    },

    /// Export a master public key
    ExportMasterPubkey,

    /// Import a master public key
    ImportMasterPubkey {
        #[arg(help = "Path to keys file generated from export-master-pubkey")]
        key_path: String,
    },

    /// Set the active master address. Any child keys derived from that address will also become active.
    SetActiveMasterAddress {
        #[arg(
            value_name = "ADDRESS_B58",
            help = "Base58-encoded address to promote to master"
        )]
        address_b58: String,
    },

    /// Lists all addresses in the wallet under the active master address, including child addresses
    ListActiveAddresses,

    /// Lists all master addresses
    ListMasterAddresses,

    /// Show the seed phrase for the current master key
    ShowSeedphrase,

    /// Show the master zpub extended public key
    #[command(name = "show-master-zpub")]
    ShowMasterZPub,

    /// Show the master zprv extended private key
    #[command(name = "show-master-zprv")]
    ShowMasterZPrv,

    /// Show the key tree structure
    #[command(name = "show-key-tree")]
    ShowKeyTree {
        #[arg(long, help = "Include values at each path")]
        include_values: bool,
    },

    // Confirmations {
    //     #[arg(value_name = "TX_ID")]
    //     tx_id: String,
    // },

    /// Sign an arbitrary message
    #[command(group = clap::ArgGroup::new("message_source").required(true).args(&["message", "message_file", "message_pos"]))]
    SignMessage {
        #[arg(
            short = 'm',
            long = "message",
            group = "message_source",
            help = "Message to sign (raw string)"
        )]
        message: Option<String>,

        #[arg(
            short = 'f',
            long = "message-file",
            group = "message_source",
            help = "Path to file containing raw bytes to sign"
        )]
        message_file: Option<String>,

        #[arg(
            value_name = "MESSAGE",
            group = "message_source",
            help = "Positional message to sign (equivalent to --message)"
        )]
        message_pos: Option<String>,

        #[arg(
            short,
            long,
            value_parser = clap::value_parser!(u64).range(0..2 << 31),
            help = "Optional key index to use for signing [0, 2^31)"
        )]
        index: Option<u64>,
        #[arg(
            long,
            default_value = "false",
            help = "Hardened or unhardened child key"
        )]
        hardened: bool,
    },

    /// Sign an already-computed tip5 hash (base58)
    SignHash {
        #[arg(
            value_name = "HASH",
            help = "Positional base58-encoded tip5 hash to sign"
        )]
        hash_b58: String,

        #[arg(
            short,
            long,
            value_parser = clap::value_parser!(u64).range(0..2 << 31),
            help = "Optional key index to use for signing [0, 2^31)"
        )]
        index: Option<u64>,
        #[arg(
            long,
            default_value = "false",
            help = "Hardened or unhardened child key"
        )]
        hardened: bool,
    },

    /// Verify an arbitrary message signature
    VerifyMessage {
        #[arg(
            short = 'm',
            long = "message",
            help = "Message to verify (raw string)"
        )]
        message: Option<String>,

        #[arg(
            short = 'f',
            long = "message-file",
            help = "Path to file containing raw bytes of message to verify"
        )]
        message_file: Option<String>,

        #[arg(
            value_name = "MESSAGE",
            conflicts_with_all = ["message", "message_file"],
            help = "Positional message to verify (equivalent to --message)"
        )]
        message_pos: Option<String>,

        #[arg(
            short = 's',
            long = "signature",
            help = "Path to jammed signature file produced by sign-message"
        )]
        signature_path: Option<String>,

        #[arg(
            value_name = "SIGNATURE_FILE",
            help = "Positional signature path (equivalent to --signature)"
        )]
        signature_pos: Option<String>,

        #[arg(
            short = 'p',
            long = "pubkey",
            help = "Base58-encoded schnorr public key"
        )]
        pubkey: Option<String>,

        #[arg(
            value_name = "PUBKEY",
            help = "Positional public key (equivalent to --pubkey)"
        )]
        pubkey_pos: Option<String>,
    },

    /// Verify a signature against an already-computed tip5 hash (base58)
    VerifyHash {
        #[arg(
            value_name = "HASH",
            help = "Positional base58-encoded tip5 hash"
        )]
        hash_b58: String,

        #[arg(
            short = 's',
            long = "signature",
            help = "Path to jammed signature file produced by signing"
        )]
        signature_path: Option<String>,
        #[arg(
            value_name = "SIGNATURE_FILE",
            help = "Positional signature path"
        )]
        signature_pos: Option<String>,

        #[arg(
            short = 'p',
            long = "pubkey",
            help = "Base58-encoded schnorr public key"
        )]
        pubkey: Option<String>,
        #[arg(
            value_name = "PUBKEY",
            help = "Positional public key"
        )]
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
            Commands::ListNotes => "list-notes",
            Commands::ListNotesByAddress { .. } => "list-notes-by-address",
            Commands::ListNotesByAddressCsv { .. } => "list-notes-by-address-csv",
            Commands::SetActiveMasterAddress { .. } => "set-active-master-address",
            Commands::CreateTx { .. } => "create-tx",
            Commands::MigrateV0Notes { .. } => "migrate-v0-notes",
            Commands::SignMultisigTx { .. } => "sign-multisig-tx",
            Commands::SendTx { .. } => "send-tx",
            Commands::ShowTx { .. } => "show-tx",
            Commands::ShowBalance => "show",
            Commands::ExportMasterPubkey => "export-master-pubkey",
            Commands::ImportMasterPubkey { .. } => "import-master-pubkey",
            Commands::ListActiveAddresses => "list-active-addresses",
            Commands::ListMasterAddresses => "list-master-addresses",
            Commands::ShowSeedphrase => "show-seed-phrase",
            Commands::ShowMasterZPub => "show-master-zpub",
            Commands::ShowMasterZPrv => "show-master-zprv",
            Commands::ShowKeyTree { .. } => "show-key-tree",
            Commands::SignMessage { .. } => "sign-message",
            Commands::VerifyMessage { .. } => "verify-message",
            Commands::SignHash { .. } => "sign-hash",
            Commands::VerifyHash { .. } => "verify-hash",
            Commands::TxAccepted { .. } => "tx-accepted",
            Commands::Watch { subcommand } => match subcommand {
                WatchSubcommand::Address { .. } => "watch-address",
                WatchSubcommand::Pubkey { .. } => "watch-address",
                //WatchSubcommand::FirstName { .. } => "watch-first-name",
                WatchSubcommand::Multisig { .. } => "watch-address-multisig",
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_P2PKH: &str = "9yPePjfWAdUnzaQKyxcRXKRa5PpUzKKEwtpECBZsUYt9Jd7egSDEWoV";

    #[test]
    fn create_tx_defaults_to_ascending_note_selection() {
        let cli = WalletCli::try_parse_from([
            "nockchain-wallet",
            "create-tx",
            "--recipient",
            &format!("{SAMPLE_P2PKH}:100"),
        ])
        .expect("create-tx CLI should parse");

        let Commands::CreateTx {
            note_selection_strategy,
            ..
        } = cli.command
        else {
            panic!("expected create-tx command");
        };

        assert!(matches!(
            note_selection_strategy,
            NoteSelectionStrategyCli::Ascending
        ));
    }

    #[test]
    fn create_tx_accepts_descending_note_selection_override() {
        let cli = WalletCli::try_parse_from([
            "nockchain-wallet",
            "create-tx",
            "--recipient",
            &format!("{SAMPLE_P2PKH}:100"),
            "--note-selection",
            "descending",
        ])
        .expect("create-tx CLI should parse");

        let Commands::CreateTx {
            note_selection_strategy,
            ..
        } = cli.command
        else {
            panic!("expected create-tx command");
        };

        assert!(matches!(
            note_selection_strategy,
            NoteSelectionStrategyCli::Descending
        ));
    }

    #[test]
    fn migrate_v0_notes_requires_destination() {
        let cli = WalletCli::try_parse_from([
            "nockchain-wallet", "migrate-v0-notes", "--destination", SAMPLE_P2PKH,
        ])
        .expect("migrate-v0-notes CLI should parse");

        let Commands::MigrateV0Notes { destination } = cli.command else {
            panic!("expected migrate-v0-notes command");
        };

        assert_eq!(destination, SAMPLE_P2PKH);
    }
}
