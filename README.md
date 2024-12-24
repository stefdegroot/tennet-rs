
Imbalance market data service on top of the Tennet Publication API.

**Goals:**
- convert REST API into a notification/subscription service
- store and sync historical data to avoid request limits
- improve/standardize data structure and time formatting

### Data sources

**api.tennet.eu/publications**
- [x] Balance Delta
- [x] Merit Order List
- [x] Settlement Prices
- [ ] Emergency Power
- [ ] Metered Injections
- [ ] Settled Imbalance Volumes
- [ ] Volume of Settled Activated Restoration and Emergency Reserve (aFRR)
- [ ] Reconcilation Prices

### Syncing historical data


### Using the notification service


### Local development

Setup local Postgres instance ([external docs](https://medium.com/@jewelski/quickly-set-up-a-local-postgres-database-using-docker-5098052a4726)) and MQTT broker.

```shell
docker-compose up -d
```