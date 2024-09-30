# rawdata-client-provider-postgres

![Build Status](https://img.shields.io/github/actions/workflow/status/descoped/rawdata-client-provider-postgres/coverage-and-sonar-analysis.yml)
![Latest Tag](https://img.shields.io/github/v/tag/descoped/rawdata-client-provider-postgres)
![Renovate](https://img.shields.io/badge/renovate-enabled-brightgreen.svg)
[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=descoped_rawdata-client-provider-postgres&metric=alert_status)](https://sonarcloud.io/summary/new_code?id=descoped_rawdata-client-provider-postgres) [![Coverage](https://sonarcloud.io/api/project_badges/measure?project=descoped_rawdata-client-provider-postgres&metric=coverage)](https://sonarcloud.io/summary/new_code?id=descoped_rawdata-client-provider-postgres)
[![Snyk Security Score](https://snyk.io/test/github/descoped/rawdata-client-provider-postgres/badge.svg)](https://snyk.io/test/github/descoped/rawdata-client-provider-postgres)

## Start and initialize database

```
docker-compose up
```

```
./docker-postgres-init-db.bash
```

### Configuration

application.properties:

```
state.provider=postgres
postgres.driver.host=localhost
postgres.driver.port=5432
postgres.driver.user=rdc
postgres.driver.password=rdc
postgres.driver.database=rawdata_client
```
