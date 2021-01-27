#!/usr/bin/env python3
import io
import json
import sys
import uuid

import singer
from singer import utils
import re
import dateutil
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from loader_kafka.configuration import Config

logger = singer.get_logger()

# Convert a Singer schema to an AVRO schema
def _avsc(a):
    field_list = []
    dates_list = []
    type_switcher = {"integer": "int",
                     "number": "double",
                     "date-time": "long"}
    default_switcher = {"integer": 0,
                        "number": 0.0,
                        "date-time": 0}
    for k, v in a.items():
        # figure out selected
        new_key = k
        type_list = ["null"]
        default_val = None
        type_dict = None
        types = [v.get("type")] if isinstance(v.get("type"), str) else v.get("type")
        # Check for date-time formatConvert & legacy "anyOf" and empty field types conversion to string
        if v.get("type") is None:
            if v.get("anyOf"):
                for ao_iter in v.get("anyOf"):
                    if ao_iter.get("format") == "date-time":
                        v["format"] = "date-time"
                types = ["null", "string"]
            else:
                # In the case of undefined types in JSON any basic type is accepted in the AVSC
                types = ["null", "string", "boolean", "int", "float", "bytes"]

        for t in types:
            if t == "object" or t == "dict":
                props_list, recurs_dates = _avsc(v["properties"])
                type_dict = { "type": "record",
                             "name": "{0}".format(k),
                             "fields": list(props_list)
                             }
                dates_list = dates_list + recurs_dates
                default_val = {}
            elif t == "array":
                type_list.append(type_switcher.get("string", "string"))
                default_val = default_switcher.get("string", None)
            elif t == "string" and v.get("format") == "date-time":
                dates_list.append(new_key)
                type_list.append(type_switcher.get("date-time", t))
                default_val = default_switcher.get("date-time", None)
            elif t == "null":
                pass
            else:
                type_list.append(type_switcher.get(t, t))
                default_val = default_switcher.get(t, None)

        if len(types) > 2:
            default_val = None

        if t == "object" or t == "dict":
            new_element = {"name": new_key, "type": type_dict, "default": default_val}
        else:
            new_element = {"name": new_key, "type": type_list, "default": default_val}
        # Handle all disallowed avro characters in the field name with alias
        pattern = r"[^A-Za-z0-9_]"
        if re.search(pattern, k):
            new_element["alias"] = new_key
            new_element["name"] = re.sub(pattern, "_", k)

        # Pass along any schema metadata
        if v.get("additionalProperties"):
            new_element["additionalProperties"] = v.get("additionalProperties")

        field_list.append(new_element)

    return list(field_list), list(dates_list)


# Convert any date field to the number of days since the epoch (per Avro spec)
#
# date_fields holds the keys (of any depth) that map to date values
def convert_dates_to_avro(date_fields, record):
    for df_iter in date_fields:
        if df_iter in record and record[df_iter] is not None:
            dt_value = dateutil.parser.parse(record[df_iter])
            record[df_iter] = int(dt_value.strftime("%s"))
    for k,v in record.items():
        if type(v) == dict:
            convert_dates_to_avro(date_fields, v)

def create_topic_as_needed(config, kafka_consumer, admin_client, stream_name, topics):
    logger.debug("Checking for target topic existence.")

    for topic in topics:
        if topic not in kafka_consumer.topics():
            logger.info(f"Creating topic {topic}")
            topic_list = [NewTopic(name=topic, num_partitions=config.get('topic_partitions', 1), replication_factor=config.get('topic_replication', 1))]
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
        else:
            logger.debug("Target topic already exists.")

def derive_records_topic_name(config, stream_name):
    return config["topic_prefix"] + "." + stream_name + ".records"

def derive_state_topic_name(config):
    return config["topic_prefix"] + ".state"

def derive_schema_topic_name(config, stream_name):
    return config["topic_prefix"] + "." + stream_name + ".schema"

def persist_messages_registry(config, avro_producer, json_producer, kafka_consumer, admin_client, messages):
    stream_to_date_fields = {}
    stream_to_schema = {}
    unique_per_run = str(uuid.uuid1())
    state_msg_counter = 0

    for idx, message in enumerate(messages):
        o = json.loads(message)

        if o['type'] == 'RECORD':
            stream_name = o['stream']
            # Convert date fields in the record
            convert_dates_to_avro(stream_to_date_fields[stream_name], o['record'])
            avro_producer.produce(topic=derive_records_topic_name(config, stream_name), value=o['record'], value_schema=stream_to_schema[stream_name])

        elif o['type'] == 'SCHEMA':
            stream_name = o['stream']
            # Creating the records topic here for efficiency
            topics = [derive_state_topic_name(config), derive_records_topic_name(config, stream_name)]
            create_topic_as_needed(config, kafka_consumer, admin_client, stream_name, topics)

            avsc_fields, stream_to_date_fields[stream_name] = _avsc(a=o['schema']["properties"])

            avsc_dict = {"namespace": "{0}.avro".format(stream_name),
                         "type": "record",
                         "name": "{0}".format(stream_name),
                         "fields": list(avsc_fields)}

            stream_to_schema[stream_name] = avro.loads(json.dumps(avsc_dict))

        elif o['type'] == 'STATE':
            # State messages have no defined spec so we must simply record them as json blobs
            message_bytes = bytes(message, encoding='utf-8')
            key_bytes = bytes((unique_per_run + "-" + str(state_msg_counter)), encoding='utf-8')
            state_msg_counter += 1
            try:
                json_producer.send(derive_state_topic_name(config), value=message_bytes, key=key_bytes)
                json_producer.flush()
            except Exception as err:
                logger.error(f"Unable to send a state message to kafka:", err)
                raise err

    avro_producer.flush()


def persist_messages_raw(config, json_producer, kafka_consumer, admin_client, messages):
    unique_per_run = str(uuid.uuid1())

    for idx, message in enumerate(messages):
        o = json.loads(message)
        if o['type'] == 'SCHEMA':
            stream_name = o['stream']

            # Creating the records topic here for efficiency
            topic = derive_schema_topic_name(config, stream_name)
            topics = [derive_state_topic_name(config), topic, derive_records_topic_name(config, stream_name)]
            create_topic_as_needed(config, kafka_consumer, admin_client, stream_name, topics)

        elif o['type'] == "RECORD":
            topic = derive_records_topic_name(config, stream_name)

        elif o['type'] == "STATE":
            topic = derive_state_topic_name(config)

        message_bytes = bytes(message, encoding='utf-8')
        key_bytes = bytes((unique_per_run+"-"+str(idx)), encoding='utf-8')

        try:
            json_producer.send(topic, value=message_bytes, key=key_bytes)
        except Exception as err:
            logger.error(f"Unable to send a record to kafka:",err)
            raise err

    json_producer.flush()

def main():
    args = utils.parse_args([]) #added argument to avoid TypeError: parse_args() missing 1 required positional argument: 'required_config_keys'
    config = Config.validate(args.config)

    json_producer = KafkaProducer(bootstrap_servers=config['kafka_brokers'], retries=3)
    kafka_consumer = KafkaConsumer(bootstrap_servers=config['kafka_brokers'], client_id='loader-kafka')
    admin_client = KafkaAdminClient(
        bootstrap_servers=config['kafka_brokers'],
        client_id='loader-kafka'
    )

    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')

    if 'schema_registry_url' in config and config['schema_registry_url']:

        avro_producer = AvroProducer({
            'bootstrap.servers': config['kafka_brokers'],
            'schema.registry.url': config['schema_registry_url']
        })

        persist_messages_registry(config, avro_producer, json_producer, kafka_consumer, admin_client, input_messages)
    else:
        persist_messages_raw(config, json_producer, kafka_consumer, admin_client, input_messages)

    json_producer.close()
    kafka_consumer.close()
    admin_client.close()

    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
