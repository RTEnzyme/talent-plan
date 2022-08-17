use std::{net::{TcpListener, TcpStream}, io::{BufReader, BufWriter, Write}, fmt::Debug};

use serde_json::Deserializer;
use tracing::{warn, info, debug, error, Level, instrument};


use crate::{Engine, Result, Request, GetResp, SetResp, RemoveResp};

#[derive(Debug)]
pub struct Server<E: Engine+Debug> {
    engine: E,
}


impl<E: Engine+Debug> Server<E> {
    pub fn new(engine: E) -> Self {
        Self { engine }
    }

    pub fn run(mut self, ip_port: &str) -> Result<()> {
        let listener = TcpListener::bind(ip_port)?;

        // accept connections and process them serially
        for stream in listener.incoming() {
            match stream {
                Ok(s) => {
                    if let Err(e) = self.handle_client(s) {
                        error!(msg="handle commands error", err=%e);
                    }
                },
                Err(e) => {
                    error!(msg="handle TCP connection error", err=%e);
                }
            }
        }
        Ok(())
    }

    #[instrument]
    fn handle_client(&mut self, stream: TcpStream) -> Result<()> {
        let peer_addr = stream.peer_addr()?;
        let reader = BufReader::new(&stream);
        let mut writer = BufWriter::new(&stream);
        let reqs = Deserializer::from_reader(reader).into_iter::<Request>();
        info!(msg="recieve a request", from=format!("{}", peer_addr));

        macro_rules! send_resp {
            ($resp:expr) => {{
                let resp = $resp;
                serde_json::to_writer(&mut writer, &resp)?;
                writer.flush()?;
                debug!(msg="Response sent", to=format!("{}", peer_addr), resp=?resp);
            };};
        }

        for req in reqs {
            match req? {
                Request::Get { key } => send_resp!(match self.engine.get(key) {
                    Ok(value) => GetResp::Ok(value),
                    Err(e) => GetResp::Err(format!("{}", e)),
                }),
                Request::Set { key, value } => send_resp!(match self.engine.set(key, value) {
                    Ok(_) => SetResp::Ok(()),
                    Err(e) => SetResp::Err(format!("{}", e)),
                }),
                Request::Remove { key } => send_resp!(match self.engine.remove(key) {
                    Ok(_) => RemoveResp::Ok(()),
                    Err(e) => RemoveResp::Err(format!("{}", e)),
                })
            }
        }
        Ok(())
    }
}

