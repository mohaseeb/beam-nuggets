from apache_beam import PTransform, ParDo, DoFn, Create, pvalue, Windowing
from apache_beam.transforms.window import GlobalWindows

from kafka import KafkaConsumer, KafkaProducer
import json

class ReadFromKafka(PTransform):
	"""A ``PTransform`` for reading from Apache Kafka"""

	def __init__(self, topic=None, servers='127.0.0.1:9092', group_id=None):
		"""Initializes ``ReadFromKafka``

		Args:
			topic: Kafka topic to read from
			servers: list of Kafka servers to listen to
			group_id: specify consumer group for dynamic partition assignment and offset commits

		"""
		super(ReadFromKafka, self).__init__()
		self._attributes = dict(
			topic=topic, 
			servers=servers, 
			group_id=group_id)

	def expand(self, pcoll):
		return (
			pcoll 
			| Create([self._attributes]) 
			| ParDo(_ConsumeKafkaTopic(self._attributes))
		)

class _ConsumeKafkaTopic(DoFn):
	"""Internal ``DoFn`` to read from Kafka topic and return messages"""

	def process(self, attributes):
		consumer = KafkaConsumer(attributes["topic"], bootstrap_servers=attributes["servers"])
			
		for msg in consumer:
			try:
				yield (msg.key, json.loads(msg.value.decode()))
			except Exception as e:
				print(e)
				continue