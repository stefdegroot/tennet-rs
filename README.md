
Imbalance market data service on top of the [Tennet Publication API](https://developer.tennet.eu/).

**Goals:**
- convert REST API into a notification/subscription service
- store and sync historical data to avoid request limits
- improve/standardize data structure and time formatting

### Data sources

**api.tennet.eu/publications**
- [x] Balance Delta
- [x] Balance Delta High Res
- [x] Merit Order List
- [x] Settlement Prices
- [ ] Emergency Power
- [ ] Metered Injections
- [ ] Settled Imbalance Volumes
- [ ] Volume of Settled Activated Restoration and Emergency Reserve (aFRR)
- [ ] Reconcilation Prices

---

### Config

The service requires a `config.toml` file in the root folder with the following properties.

#### Tennet

```toml
[tennet]
api_url = "https://api.tennet.eu/publications"
api_key = ""
```

#### Database

```toml
[db]
user        = "admin"
password    = ""
name        = "test_db"
host        = "localhost"
```

#### Mqtt

By default publishing updates for data sources on mqtt is disabled. You can turn this on with the following configuration:

```toml
[mqtt]
enabled     = true
client_id   = "tennet-rs-server"
host        = "localhost"
port        = 1883
username    = ""
password    = ""
root_topic  = "/tennet"
```

All properties are backed up by a default, as shown in the example above. And can be changed set individually, expect for `username` and `password` which are mutually exclusive to turn on/off authentication when connecting to the broker.

#### Data

```toml
[data]
path        = "./data"
```

---

### Using the notification service

When turned on in the configuration the service will publish the latest updates for all sources on separate topics. The topics start with the `root_topic` from the configuration, which defaults to `/tennet` if not specified. This is followed by the source path:
- Balance Delta             => `/balance-delta`
- Balance Delta High Res    => `/balance-delta-high-res`
- Merit Order List          => `/merit-order`
- Settlement Prices         => `/settlement-prices`

---

### Syncing historical data




### Local development

Setup local Postgres instance ([external docs](https://medium.com/@jewelski/quickly-set-up-a-local-postgres-database-using-docker-5098052a4726)) and MQTT broker.

```shell
docker-compose up -d
```

### Building and deploying

#### Clippy

All pull-requests should pass the whole Clippy specification, no errors, warning or suggestions are allowed.

The response from:

```shell
cargo clippy
```

should be clean.
