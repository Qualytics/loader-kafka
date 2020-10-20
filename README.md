# loader-kafka

This is a [Meltano](https://meltano.com/) target that reads JSON-formatted data
following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md) and publishes it to a Kafka topic.

## Settings Requirements

```
settings: kafka_brokers, topic

```

This loader requires the name of topic you are publishing to and the Kafka broker(s) that host the topic. Once you assign these settings values via the meltano.yml or the Meltano UI, Meltano will configure it internally as JSON. __init__.py will then convert the JSON to a dictionary for ease of access.
