use clap::{arg, command, Command, Arg};
use kvs::{KvStore, Result, addr_check, Client};
use std::{env::current_dir, process::exit, net::IpAddr, io::BufRead};


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
        .arg(Arg::new("addr")
        .long("addr")
        .name("addr")
        .help("the server ip-port")
        .default_value("127.0.0.1:4000")
        .takes_value(true)
    )
        .get_matches();
    let ip_port = matches.get_one::<String>("addr").expect("please give a valid ip:port");
    if !addr_check(&ip_port) {
        eprintln!("incorrect ip:port");
        exit(1);
    }
    match matches.subcommand() {
        Some(("get", m)) => {
            let key: &String = m.get_one("key").unwrap();
            
            let mut client = Client::connect(ip_port)?;
            let value = client.get(key.to_owned())?;
            match value {
                Some(v) => println!("{}", v),
                None => print!("nil"),
            }
        }
        Some(("set", m)) => {
            let key: &String = m.get_one("key").unwrap();
            let value: &String = m.get_one("value").unwrap();

            let mut client = Client::connect(ip_port)?;
            match client.set(key.to_owned(), value.to_owned()) {
                Ok(_) => println!("ok"),
                Err(e) => println!("{}", e),
            }           
        }
        Some(("rm", m)) => {
            let key: &String = m.get_one("key").unwrap();

            let mut client = Client::connect(ip_port)?;
            match client.remove(key.to_owned()) {
                Ok(_) => println!("ok"),
                Err(e) => println!("{}", e),
            }
        }
        _ => {
            unreachable!("unimplemented");
        }
    };
    Ok(())
}
