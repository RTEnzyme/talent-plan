use clap::{arg, command, Command, Arg};
use kvs::{KvStore, Result};
use tracing::{warn, info, error};
use std::{env::current_dir, process::exit, fs};
use tracing_subscriber;

fn main() {
    let tgt = "svr-main";
    tracing_subscriber::fmt().json().init();
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
        let mut engine = matches.get_one("ENGINE_NAME");
        if engine.is_none() {
            engine = curr_engine.as_ref();
        }
        if curr_engine.is_some() && engine != curr_engine.as_ref() {
            error!("Wrong engine!");
            exit(1);
        }
        // run(opt)
        Ok(())
    });
    if let Err(e) = res {
        error!("{}", e);
        exit(1);
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
