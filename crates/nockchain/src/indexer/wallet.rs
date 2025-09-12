use std::collections::HashMap;

use nockapp::{AtomExt, NounExt};
use nockvm::mem::NockStack;
use nockvm::noun::Atom;
use nockvm::serialization;
use rocksdb::DB;

#[derive(Debug)]
pub struct WalletBalance {
    pub notes: HashMap<String, u64>,
    pub locked_balances: Vec<u64>,
    pub block_id: Option<String>,
    pub total_balance: Option<u64>,
}

#[derive(Debug)]
pub struct DbDump {
    pub balances: HashMap<String, u64>,
    pub locked_balances: Vec<u64>,
    pub metadata: HashMap<String, String>,
}

impl WalletBalance {
    pub fn dump_db(db: &DB) -> Result<DbDump, Box<dyn std::error::Error>> {
        let mut dump = DbDump {
            balances: HashMap::new(),
            locked_balances: vec![],
            metadata: HashMap::new(),
        };

        let iter = db.iterator(rocksdb::IteratorMode::Start);
        for item in iter {
            let (key, value) = item?;
            let key_str = String::from_utf8_lossy(&key);

            if key_str.starts_with("balance_") {
                let address = key_str.strip_prefix("balance_").unwrap();
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&value);
                let balance = u64::from_be_bytes(buf);
                dump.balances.insert(address.to_string(), balance);
            } else if key_str.starts_with("locked_") {
                let mut buf = [0u8; 8];
                buf.copy_from_slice(&value);
                let balance = u64::from_be_bytes(buf);
                dump.locked_balances.push(balance);
            } else {
                dump.metadata.insert(
                    key_str.to_string(),
                    String::from_utf8_lossy(&value).to_string(),
                );
            }
        }

        Ok(dump)
    }

    pub fn from_jammed_data(data: &[u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let mut stack = NockStack::new(1 << 20, 0);
        let jammed_atom = Atom::new(&mut stack, u64::from_be_bytes(data[0..8].try_into()?));
        let noun = serialization::cue(&mut stack, jammed_atom)
            .map_err(|e| format!("Failed to cue jammed data: {:?}", e))?;

        let mut notes = HashMap::new();
        let mut locked_balances = Vec::new();
        let mut block_id = None;
        let mut total_balance = None;

        if let Ok(cell) = noun.as_cell() {
            let head = cell.head();
            if head.is_atom() {
                if !head.eq_bytes(b"wallet-balance") {
                    return Err("Not a wallet balance".into());
                }
            }

            let tail = cell.tail();
            if let Ok(data_cell) = tail.as_cell() {
                // Parse notes
                let head = data_cell.head();
                if let Ok(mut current_cell) = head.as_cell() {
                    loop {
                        let note = current_cell.head();
                        if let Ok(note_data) = note.as_cell() {
                            let name_noun = note_data.head();
                            let balance_noun = note_data.tail();
                            if name_noun.is_atom() && balance_noun.is_atom() {
                                if name_noun.eq_bytes(b"wallet-balance") {
                                    let name =
                                        String::from_utf8_lossy(b"wallet-balance").to_string();
                                    if let Ok(atom) = balance_noun.as_atom() {
                                        if let Ok(balance) = atom.as_u64() {
                                            notes.insert(name, balance);
                                        }
                                    }
                                }
                            }
                        }

                        let next_tail = current_cell.tail();
                        if let Ok(next_cell) = next_tail.as_cell() {
                            current_cell = next_cell;
                        } else {
                            break;
                        }
                    }
                }

                // Parse locked balances
                if let Ok(tail_cell) = data_cell.tail().as_cell() {
                    if let Ok(locked_cell) = tail_cell.head().as_cell() {
                        let mut current_cell = locked_cell;
                        loop {
                            let head = current_cell.head();
                            if head.is_atom() {
                                if let Ok(atom) = head.as_atom() {
                                    if let Ok(balance) = atom.as_u64() {
                                        locked_balances.push(balance);
                                    }
                                }
                            }

                            let next_tail = current_cell.tail();
                            if let Ok(next_cell) = next_tail.as_cell() {
                                current_cell = next_cell;
                            } else {
                                break;
                            }
                        }
                    }

                    // Parse block ID
                    if let Ok(block_cell) = tail_cell.tail().as_cell() {
                        let block_head = block_cell.head();
                        if block_head.is_atom() && block_head.eq_bytes(b"wallet-balance") {
                            block_id = Some(String::from_utf8_lossy(b"wallet-balance").to_string());
                        }
                    }

                    // Parse total balance
                    if let Ok(total_cell) = tail_cell.tail().as_cell() {
                        let total_noun = total_cell.head();
                        if total_noun.is_atom() {
                            if let Ok(atom) = total_noun.as_atom() {
                                if let Ok(balance) = atom.as_u64() {
                                    total_balance = Some(balance);
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(WalletBalance {
            notes,
            locked_balances,
            block_id,
            total_balance,
        })
    }
}
