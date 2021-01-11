#!/usr/bin/env python3
import argparse
import io
import json
import sys
import singer
import uuid
import re
import collections
import dateutil
from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
import loader_kafka.conversion as conversion

logger = singer.get_logger()

def flatten(d, parent_key='', flatten_delimiter='__'):
    items = []
    for k, v in d.items():
        new_key = parent_key + flatten_delimiter + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, flatten_delimiter=flatten_delimiter).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)


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
        if True or (v.get("selected") == "true" or v.get("selected") == True or v.get("inclusion") == "automatic" or parent_key)\
                and v.get("inclusion") != "unsupported":
            # logger.info(v)
            #new_key = parent_key + flatten_delimiter + k if parent_key else k
            new_key = k
            type_list = ["null"]
            default_val = None
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
                    # recurs_avsc, recurs_dates = _flatten_avsc(v["properties"],
                    #                                           parent_key=new_key,
                    #                                           flatten_delimiter=flatten_delimiter)
                    # field_list.extend(recurs_avsc)
                    # dates_list.extend(recurs_dates)
                    # Set a default element of string
                    new_dict = { "type": "record",
                                 "name": "{0}".format(k),
                                 "fields": _avsc(v["properties"], parent_key=new_key, flatten_delimiter=flatten_delimiter)})
                    type_list.append(new_dict)
                    #type_list.append(type_switcher.get("string", "string"))
                    #default_val = default_switcher.get("string", None)
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

            new_element = {"name": new_key, "type": type_list, "default": default_val}

            # Handle all disallowed avro characters in the field name with alias
            pattern = r"[^A-Za-z0-9_]"
            if re.search(pattern, k):
                new_element["alias"] = new_key
                new_element["name"] = re.sub(pattern, "_", k)

            field_list.append(new_element)

    return list(field_list), list(dates_list)

def persist_messages(messages, config):
    schema_date_fields = {}
    avro_files = {}

    logger.info("Verifying target topic existence.")
    kafka_consumer = KafkaConsumer(bootstrap_servers=config['kafka_brokers'], client_id='loader-kafka')
    if config['kafka_topic'] not in kafka_consumer.topics():
        logger.info(f"Creating topic {config['kafka_topic']}")
        admin_client = KafkaAdminClient(
            bootstrap_servers=config['kafka_brokers'],
            client_id='loader-kafka'
        )
        topic_list = [NewTopic(name=config['kafka_topic'], num_partitions=config.get('topic_partitions', 1), replication_factor=config.get('topic_replication', 1))]
        admin_client.create_topics(new_topics=topic_list, validate_only=False)

    producer = KafkaProducer(bootstrap_servers=config['kafka_brokers'], retries=3)
    unique_per_run = str(uuid.uuid1())

    avroProducer = AvroProducer({
    'bootstrap.servers': config['kafka_brokers'],
    'schema.registry.url': config['schema_url'] ##ADD THIS TO THE CONFIG
    })


    value_schema = {}

    for idx, message in enumerate(messages):
        o = json.loads(message)
        if 'RECORD' in message:
            if 'stream' not in o:
                raise Exception("Line is missing required key 'stream': {}".format(line))

            # Convert date fields in the record
            for df_iter in schema_date_fields[o['stream']]:
                if o['record'][df_iter] is not None:
                    dt_value = dateutil.parser.parse(o['record'][df_iter])
                    o['record'][df_iter] = int(dt_value.strftime("%s"))

            #flattened_record = flatten(o['record'], flatten_delimiter="__")

            avroProducer.produce(topic=config['kafka_topic'], value=o['record'], value_schema = value_schema)
            avroProducer.flush()

        if 'STATE' in message:
            logger.info("in state")
            props_schema = conversion.infer_schemas(o)["properties"]
            # inferred_schema = {
            #     'type': 'object',
            #     'properties': props_schema
            # }
            # logger.info(inferred_schema)

            schema_date_fields["state"] = []
            # logger.info("props")
            # logger.info(props_schema)
            avsc_fields, schema_date_fields["state"] = _avsc(a=props_schema)
            # logger.info("fields")
            # logger.info(avsc_fields)

            avsc_dict = {"namespace": "{0}.avro".format("state"),
                         "type": "record",
                         "name": "{0}".format("state"),
                         "fields": list(avsc_fields)}

            value_schema = avro.loads(json.dumps(avsc_dict))
            logger.info(o["value"])
            # logger.info(value_schema)
            logger.info(schema_date_fields["state"])
            #flattened_value = flatten(o['value'], flatten_delimiter="__")
            value = o['value']
            for df_iter in schema_date_fields["state"]:
                if value[df_iter] is not None:
                    dt_value = dateutil.parser.parse(value[df_iter])
                    value[df_iter] = int(dt_value.strftime("%s"))
            logger.info(value)


            avroProducer.produce(topic=config["state_topic"], value=value, value_schema = value_schema)
            avroProducer.flush()


            if o['type'] == 'STATE':
                emit_state(o['value'])
        if 'SCHEMA' in message:
            if o['type'] == 'SCHEMA':
                if 'stream' not in o:
                    raise Exception("Line is missing required key 'stream': {}".format(line))
                stream = o['stream']

                schema_date_fields[stream] = []
                avsc_fields, schema_date_fields[stream] = _flatten_avsc(a=o['schema']["properties"],
                                                                        flatten_delimiter="__")

                avsc_dict = {"namespace": "{0}.avro".format(stream),
                             "type": "record",
                             "name": "{0}".format(stream),
                             "fields": list(avsc_fields)}

                value_schema = avro.loads(json.dumps(avsc_dict))


        # message_bytes = bytes(message, encoding='utf-8')
        # key_bytes = bytes((unique_per_run+"-"+str(idx)), encoding='utf-8')
        # try:
        #     producer.send(config['kafka_topic'], value=message_bytes, key=key_bytes)
        # except Exception as err:
        #     logger.error(f"Unable to send a record to kafka:",err)
        #     raise err

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input_json:
            config = json.load(input_json)
    else:
        config = {}
    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    persist_messages(input_messages, config)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
