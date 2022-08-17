use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub enum Request {
    Get {key: String},
    Set {key: String, value: String},
    Remove {key: String},
}

#[derive(Debug, Deserialize, Serialize)]
pub enum GetResp {
    Ok(Option<String>),
    Err(String),
}

#[derive(Debug, Deserialize, Serialize)]
pub enum SetResp {
    Ok(()),
    Err(String),
}

#[derive(Debug, Deserialize, Serialize)]
pub enum RemoveResp {
    Ok(()),
    Err(String)
}