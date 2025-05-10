use serde::Deserialize;
use tracing::error;
use std::{fs, env};
use std::process::exit;
use toml;
use lazy_static::lazy_static;

#[derive(Deserialize, Debug, Clone)]
pub struct Tennet {
    pub api_url: String,
    pub api_key: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct DB {
    pub user: String,
    pub password: String,
    pub name: String,
    pub host: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub tennet: Tennet,
    pub db: DB,
}

pub fn load_config () -> Config {

    let mut path = env::current_dir().unwrap();
    path.push("config.toml");

    println!("{:?}", path);

    let contents = match fs::read_to_string(path) {
        Ok(c) => c,
        Err(_) => {
            error!("Could not find config.toml file.");
            exit(1);
        }
    };

    let config: Config = match toml::from_str(&contents) {
        Ok(d) => d,
        Err(_) => {
            error!("Could not read config.toml file.");
            exit(1);
        }
    };

    tracing::info!("config loaded");

    config
}

lazy_static! {
    pub static ref CONFIG: Config = {
        load_config()
    };
}