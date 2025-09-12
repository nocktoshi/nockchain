use std::path::PathBuf;

use rocksdb::DB;
use structopt::StructOpt;

use crate::indexer::wallet::WalletBalance;

#[derive(StructOpt, Debug)]
#[structopt(name = "db-viewer")]
struct Opt {
    /// Path to the RocksDB database
    #[structopt(parse(from_os_str))]
    db_path: PathBuf,
}

pub async fn view_db(path: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    println!("Opening database at {:?}", path);

    let db = DB::open_for_read_only(&rocksdb::Options::default(), path, true)?;
    let dump = WalletBalance::dump_db(&db)?;

    println!("\n=== Wallet Balances ===");
    for (address, balance) in &dump.balances {
        println!("Address: {}\nBalance: {}\n", address, balance);
    }

    println!("\n=== Locked Balances ===");
    for (i, balance) in dump.locked_balances.iter().enumerate() {
        println!("Lock {}: {}", i, balance);
    }

    println!("\n=== Metadata ===");
    for (key, value) in &dump.metadata {
        println!("{}: {}", key, value);
    }

    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opt = Opt::from_args();
    view_db(opt.db_path).await
}
