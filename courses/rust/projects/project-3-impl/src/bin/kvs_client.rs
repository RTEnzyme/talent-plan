use clap::{arg, command, Command, Arg};
use kvs::{KvStore, Result};
use std::{env::current_dir, process::exit};

fn main() -> Result<()> {
    let matches = command!() // requires `cargo` feature
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about("kvs client")

        .subcommands(vec![
            Command::new("get")
                .about("get a value by key")
                .arg(arg!([key] "key").required(true)),
            Command::new("set")
                .about("set a key-value")
                .arg(arg!([key] "key").required(true))
                .arg(arg!([value] "value").required(true)),
            Command::new("rm")
                .about("remove a key-value")
                .arg(arg!([key] "key").required(true)),
        ])
        .get_matches();
    match matches.subcommand() {
        Some(("get", m)) => {
            let key: &String = m.get_one("key").unwrap();
            
            let mut store = KvStore::open(current_dir()?)?;
            if let Some(v) = store.get(key.to_owned())? {
                println!("{v:}");
            } else {
                println!("Key not found");
            }
        }
        Some(("set", m)) => {
            let key: &String = m.get_one("key").unwrap();
            let value: &String = m.get_one("value").unwrap();

            let mut store = KvStore::open(current_dir()?)?;
            store.set(key.to_owned(), value.to_owned())?;            
        }
        Some(("rm", m)) => {
            let key: &String = m.get_one("key").unwrap();

            let mut store = KvStore::open(current_dir()?)?;
            match store.remove(key.to_owned()) {
                Ok(_) => {},
                Err(_) => {
                    println!("Key not found");
                    exit(1);
                },
            } 
        }
        _ => {
            unreachable!("unimplemented");
        }
    };
    Ok(())
}
