use serde::Deserialize;
use tracing::error;
use std::{fs, env};
use std::process::exit;
use lazy_static::lazy_static;

#[derive(Deserialize, Debug, Clone)]
#[serde(default)]
pub struct MqttOptions {
    pub enabled: bool,
    pub client_id: String,
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
    pub root_topic: String,
}

impl Default for MqttOptions {
    fn default() -> Self {
        MqttOptions {
            enabled: false,
            client_id: "tennet-rs-server".to_string(),
            host: "localhost".to_string(),
            port: 1883,
            username: None,
            password: None,
            root_topic: "/tennet".to_string(),
        }
    }
}

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
pub struct Data {
    pub path: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
    pub tennet: Tennet,
    pub db: DB,
    pub mqtt: MqttOptions,
    pub data: Data,
}

pub fn load_config () -> Config {

    let config_path = match env::var("CONFIG_PATH") {
        Ok(val) => match val.parse::<String>() {
            Ok(val) => val,
            Err(_) => "config.toml".to_string(),
        },
        Err(_) => "config.toml".to_string(),
    };

    let contents = match fs::read_to_string(config_path) {
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