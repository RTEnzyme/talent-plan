use clap::{arg, command, Command};

fn main() {
    let matches = command!() // requires `cargo` feature
        .version(env!("CARGO_PKG_VERSION"))
        .author(env!("CARGO_PKG_AUTHORS"))
        .about("Does awesome things")
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
        Some(("get", get)) => {
            unimplemented!("unimplemented");
        }
        Some(("set", set)) => {
            unimplemented!("unimplemented");
        }
        Some(("rm", rm)) => {
            unimplemented!("unimplemented");
        }
        _ => {
            unreachable!("unimplemented");
        }
    }
}
