# Demo

How to load plugins to Kafka Connect installations.

## Requirements

To run the demo, you need:
- Docker compose
- `curl`
- `jq`

## Demo

To install transforms into a Kafka Connect installation, it needs:

- Build `elasticsearch-connector-for-apache-kafka` libraries
- Add libraries to Kafka Connect nodes
- Configure Kafka Connect `plugin.path`

### Build `elasticsearch-connector-for-apache-kafka` libraries

To build libraries, use `gradlew installDist` command.
e.g. Dockerfile build:

```dockerfile
FROM eclipse-temurin:11-jdk AS base

ADD ./ connector
WORKDIR connector

RUN ./gradlew installDist
```

This generates the set of libraries to be installed in Kafka Connect workers.

### Add libraries to Kafka Connect nodes

Copy the directory with libraries into your Kafka Connect nodes.
e.g. add directory to Docker images:

```dockerfile
FROM confluentinc/cp-kafka-connect:7.3.3

COPY --from=base /connector/build/install /connector-plugins
```

### Configure Kafka Connect `plugin.path`

On Kafka Connect configuration file, set `plugin.path` to indicate where to load plugins from,
e.g. with Docker compose:

```yaml
connect:
  # ...
  environment:
    # ...
    CONNECT_PLUGIN_PATH: /usr/share/java,/connector-plugins # added on Dockerfile build
```

## Running

1. Build docker images: `make build` or `docker compose build`
2. Start environment: `make up` or `docker compose up -d`
3. Test connect plugins are loaded: `make test`

Sample response:
```json lines
{
  "class": "io.aiven.connect.elasticsearch.ElasticsearchSinkConnector",
  "type": "sink",
  "version": "6.2.0-SNAPSHOT"
}
```
