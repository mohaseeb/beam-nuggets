import unittest

from apache_beam.testing.test_pipeline import TestPipeline
import apache_beam as beam

from beam_nuggets.io import kafkaio


class TestKafkaProduceTransform(unittest.TestCase):

    def setUp(self):
        super(TestKafkaProduceTransform, self).setUp()

    def test_ProduceKafkaMessage(self):
        #create messages and push messages into Apache Kafka
        with TestPipeline() as p:
            (p | "Creating records" >> beam.Create([('dev_1', '{"device": "0001", status": "healthy"}')])
               | "Produce kafka message" >> kafkaio.KafkaProduce(topic="test_stream", servers="localhost:9092")
                )

class TestKafkaConsumeTransform(unittest.TestCase):

    def setUp(self):
        super(TestKafkaConsumeTransform, self).setUp()

    def test_ConsumeFromKafka(self):
        kafka_config = {"topic": "test_stream",
                        "bootstrap_servers": "localhost:9092",
                        "group_id": "test_group"}
        #create a streaming Kafka consumer
        with TestPipeline() as p:
            p | "Consume kafka messages" >> kafkaio.KafkaConsume(kafka_config)

if __name__ == '__main__':
    unittest.main()