use clap::{command, Arg};
use kvs::{KvStore, Result, addr_check, Server, KvsEngine, Engine, SledKvsEngine};
use tracing::{warn, info, error, Level};
use std::{env::current_dir, process::exit, fs};
use tracing_subscriber;

fn main() {
    let tgt = "svr-main";
    tracing_subscriber::fmt()
        .json()
        .with_max_level(Level::DEBUG)
        .flatten_event(true)
        .init();
    info!(target=tgt, "starting the server");
    let matches = command!() // requires `cargo` feature
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about("kvs server")
        .arg(Arg::new("engine")
            .long("engine")
            .value_name("ENGINE_NAME")
            .value_parser(["kvs", "sled"])
            .help("use [ENGINE_NAME] store engine, chosen in kvs and sled, default using last used")
            .takes_value(true)
        )
        .arg(
            Arg::new("addr")
            .long("addr")
            .value_name("IP-PORT")
            .default_value("127.0.0.1:4000")
            .help("exec this kv store in ip:port")
            .takes_value(true)
        )
        .get_matches();
    let res = current_engine().and_then(move |curr_engine| {
        let ip_port = matches.get_one::<String>("addr").expect("please give a valid ip:port");
        if !addr_check(&ip_port) {
            error!(msg="incorrect ip:port format");
            exit(1);
        }
        let mut engine = matches.get_one("engine");
        if engine.is_none() {
            engine = curr_engine.as_ref();
        }
        if curr_engine.is_some() && engine != curr_engine.as_ref() {
            error!(msg="Mismatched engine!");
            exit(1);
        }
        info!(msg="finish config", engine=engine, ip_port=ip_port);
        run(engine.unwrap(), ip_port)
    });
    if let Err(e) = res {
        error!(msg="running error", err=%e);
        exit(1);
    }
}

fn run(engine: &str, ip_port: &str) -> Result<()> {
    let current_dir = current_dir()?;
    // change the engine option in dir
    fs::write(current_dir.join("engine"), format!("{}", engine))?;
    info!(msg="flush engine option to engine file", engine=engine);
    match engine {
        "kvs" => {
            Server::new(KvsEngine::open(current_dir)?).run(ip_port)
        },
        "sled" => {
            Server::new(SledKvsEngine::open(current_dir)).run(ip_port)
        },
        _ => unreachable!(),
    }
}

fn current_engine() -> Result<Option<String>> {
    let engine = current_dir()?.join("engine");
    if !engine.exists() {
        return Ok(None);
    }

    match fs::read_to_string(engine) {
        Ok(engine) => Ok(Some(engine)),
        Err(e) => {
            warn!(target="load engines", msg="The content of engine file is invalid", content=%e);
            Ok(None)
        }
    }
}
