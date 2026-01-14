
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

### Building and deploying
docker login rg.nl-ams.scw.cloud/birch-systems -u nologin --password=""

docker build -t birch-systems/tennet:0.0.2 .

docker tag birch-systems/tennet:0.0.2 rg.nl-ams.scw.cloud/birch-systems/tennet:0.0.2
docker tag birch-systems/tennet:0.0.2 rg.nl-ams.scw.cloud/birch-systems/tennet:latest

docker push rg.nl-ams.scw.cloud/birch-systems/tennet:0.0.2
docker push rg.nl-ams.scw.cloud/birch-systems/tennet:latest

docker run -d --restart unless-stopped --network=host --volume /root/tennet:/data -e CONFIG_PATH='/data/config.toml' --name tennet rg.nl-ams.scw.cloud/birch-systems/tennet:latest

docker run -d \
--restart unless-stopped \
--name watchtower \
-e REPO_USER='' \
-e REPO_PASS='' \
-v /var/run/docker.sock:/var/run/docker.sock \
containrrr/watchtower \
-i 60 tennet