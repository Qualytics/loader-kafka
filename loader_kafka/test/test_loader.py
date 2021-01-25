import unittest
from unittest.mock import MagicMock

from loader_kafka import Config, persist_messages, convert_dates_to_avro

COMPLETE_TEST_SPEC = {
    "kafka_brokers": "http://localhost:4242",
    "schema_registry_url": "http://registry:3333",
    "topic_prefix": "qualytics.topics",
    "topic_partitions": 3,
    "topic_replication": 2
}
PARTIAL_TEST_SPEC = {
    "kafka_brokers": "http://localhost:4242",
    "schema_registry_url": "http://registry:3333",
    "topic_prefix": "qualytics.topics"
}


class TestFormatHandler(unittest.TestCase):

    def test_config(self):
        Config.validate(COMPLETE_TEST_SPEC)
        Config.validate(PARTIAL_TEST_SPEC)

    def test_date_conversion(self):
        record = {
            'id': 42,
            'date': '2021-01-20',
            'misc': '01/19/1976',
            'adate': '2001/01/09'
        }
        expected_conversion = {'id': 42, 'date': 1611118800, 'misc': 190875600, 'adate': 979016400}
        convert_dates_to_avro(["date","misc","adate"],record)
        assert record == expected_conversion

    def test_parse_and_persist_logic(self):
        config = Config.validate(COMPLETE_TEST_SPEC)
        test_filename_uri = './loader_kafka/test/tap_output.json'
        with open(test_filename_uri, 'r') as messages:
            avro_producer = MagicMock()
            json_producer = MagicMock()
            class MockTopics:
                def topics(self):
                    return ["pre-existing-topic"]
            kafka_consumer = MagicMock(spec=MockTopics)
            admin_client = MagicMock()
            persist_messages(config, avro_producer, json_producer, kafka_consumer, admin_client, messages)
            # 1999 records published plus one call to flush
            assert len(avro_producer.method_calls) == 2000
            # One state message published plus a call to flush
            assert len(json_producer.method_calls) == 2
            # Did we invoke admin client to create the kafka topic as expected?
            assert len(admin_client.method_calls) == 1
    
    def test_schema_registry_url_logic(self):
        config = Config.validate(COMPLETE_TEST_SPEC)
        test_filename_uri = './loader_kafka/test/tap_output.json'
