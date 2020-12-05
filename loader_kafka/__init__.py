#!/usr/bin/env python3
import argparse
import io
import json
import os
import sys
from datetime import datetime
import singer
import uuid
from dotenv import load_dotenv, find_dotenv
import time
import random
import kafka
from kafka import KafkaProducer


logger = singer.get_logger()

def persist_messages(messages, config):
    producer = KafkaProducer(bootstrap_servers=config['kafka_brokers'],retries=3)
    unique_per_run = str(uuid.uuid1())

    for idx, message in enumerate(messages):
        if 'STATE' in message:
            o = json.loads(message)
            if o['type'] == 'STATE':
                emit_state(o['value'])

        message_bytes = bytes(message, encoding='utf-8')
        key_bytes = bytes((unique_per_run+"-"+str(idx)), encoding='utf-8')
        try:
            producer.send(config['kafka_topic'], value=message_bytes, key=key_bytes)
        except Exception as err:
            logger.error(f"Unable to send a record to kafka:",err)
            raise err

def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()

def main():
    logger.info("in target")
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
