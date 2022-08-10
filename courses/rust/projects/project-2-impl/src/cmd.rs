use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub enum Cmd {
    Set {
        key: String,
        value: String,
    },
    Remove { key: String },
}


