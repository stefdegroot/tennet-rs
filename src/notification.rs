use std::time::Duration;
use rumqttc::v5::{mqttbytes::QoS, AsyncClient, MqttOptions};
use tokio::task;
use crate::config::CONFIG;

pub struct Mqtt {
    client: AsyncClient
}

impl Mqtt {

    pub fn init () -> Self {

        let mut mqtt_options = MqttOptions::new(
            "tennet-rs-server",
            "localhost",
            1883,
        );
    
        mqtt_options.set_keep_alive(Duration::from_secs(5));
        mqtt_options.set_credentials(&CONFIG.mosquitto.username, &CONFIG.mosquitto.password);
    
        let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);

        task::spawn(async move {
            loop {
                let event = eventloop.poll().await;
                match &event {
                    Ok(_v) => {
                        // println!("Event = {v:?}");
                    }
                    Err(_e) => {
                        // println!("Error = {e:?}");
                    }
                }
            }
        });

        Mqtt {
            client,
        }
    }

    pub async fn publish (&self, topic: &str, payload: String) {

        self.client.publish(topic, QoS::ExactlyOnce, false, payload)
            .await
            .unwrap();

    }
}