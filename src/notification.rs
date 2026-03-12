use std::time::Duration;
use rumqttc::v5::{mqttbytes::QoS, AsyncClient, MqttOptions};
use tokio::task;
use crate::config::CONFIG;

pub struct Mqtt {
    client: Option<AsyncClient>
}

impl Mqtt {

    pub fn init () -> Self {

        if !&CONFIG.mqtt.enabled {
            return Mqtt {
                client: None
            };
        }

        let mut mqtt_options = MqttOptions::new(
            &CONFIG.mqtt.client_id,
            &CONFIG.mqtt.host,
            CONFIG.mqtt.port,
        );

        mqtt_options.set_keep_alive(Duration::from_secs(5));

        if
            let Some(username) = &CONFIG.mqtt.username &&
            let Some(password) = &CONFIG.mqtt.password
        {
            mqtt_options.set_credentials(username, password);
        }

        let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);

        task::spawn(async move {
            loop {
                let event = eventloop.poll().await;
                match &event {
                    Ok(_v) => {}
                    Err(err) => {
                        tracing::error!("mqtt: {}", err);
                    }
                }
            }
        });

        Mqtt {
            client: Some(client),
        }
    }

    pub async fn publish (&self, topic: &str, payload: String) {

        if let Some(client) = &self.client {
            match client.publish(CONFIG.mqtt.root_topic.to_string() + topic, QoS::ExactlyOnce, false, payload).await {
                Ok(_) => (),
                Err(err) => {
                    tracing::error!("Failed to publish mqtt message: {}", err)
                }
            }
        }
    }
}
