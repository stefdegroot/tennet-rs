
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

The syncing service requires an API key from the TenneT API Developer Portal, you can register an account [here](https://developer.tennet.eu/register/). Without this key the service can still operate, but only provides historical data loaded in from the `/data` folder.

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

If the data folder is configured, the service will [syncing historical data](#syncing-historical-data) from the provided files. If turned off, the service will start syncing based on the API, which due to rate limiting can take a while, and it is therefore not recommended.

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

To sync historical data correctly the data folder should follow the format below:

```
data
└─── balance_delta_high_res
│    │─ BALANCE_DELTA_HIGH_RES_2025-01.csv
│    └─ BALANCE_DELTA_HIGH_RES_2025-02-01.csv
└─── balance_delta
│    │─ BALANCE_DELTA_2025-01.csv
│    └─ BALANCE_DELTA_2025-02-01.csv
└─── merit_order
│    └─ MERIT_ORDER_2025-01.csv
└─── settlement_prices
     │─ SETTLEMENT_PRICES_2024.csv
     └─ SETTLEMENT_PRICES_2025-01.csv
```

If you want to gather the historical data yourself you can download the individual CSV files from the [TenneT Website](https://www.tennet.eu/nl-en/markets/transparency/download-page-transparency). Alternatively, you can use the pre bundled dataset from Birch Systems, which can be downloaded [here](https://birch-systems.s3.nl-ams.scw.cloud/tennet/tennet_export.zip), to speed up your process. (In a future release, importing historical data will be fully automated)

