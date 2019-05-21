from apache_beam import PTransform, ParDo, DoFn, Create, pvalue, Windowing
from apache_beam.transforms.window import GlobalWindows

from kafka import KafkaConsumer, KafkaProducer
import json

class KafkaConsume(PTransform):
    """A :class:`~apache_beam.transforms.ptransform.PTransform` for reading from an Apache Kafka Topic

    It outputs a :class:`~apache_beam.pvalue.PCollection` of
    ``key-values:s``, each object is a Kafka message in the form (msg-key, msg)

    Examples:
        Consuming from a Kafka Topic `notifications` ::

            import apache_beam as beam
            from apache_beam.options.pipeline_options import PipelineOptions
            from beam_nuggets.io import kafkaio

            kafka_topic = 'notifications'

            with beam.Pipeline(options=PipelineOptions()) as p:
                notifications = p | "Reading messages from Kafka" >> kafkaio.KafkaConsume(
                    topic=kafka_topic,
                    servers="localhost:9092",
                    group_id="notification_consumer_group"
                )
                notifications | 'Writing to stdout' >> beam.Map(print)

        The output will be something like ::

            ("device 1", {"status": "healthy"})
            ("job #2647", {"status": "failed"})

        Where the key is the Kafka message key and the element is the Kafka message being passed through the topic
    """

    def __init__(self, topic=None, servers='127.0.0.1:9092', group_id=None):
        """Initializes ``KafkaConsume``

        Args:
            topic: Kafka topic to read from
            servers: list of Kafka servers to listen to
            group_id: specify consumer group for dynamic partition assignment and offset commits

        """
        super(KafkaConsume, self).__init__()
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
                yield (msg.key, msg.value.decode())
            except Exception as e:
                print(e)
                continue

class KafkaProduce(PTransform):
    """A :class:`~apache_beam.transforms.ptransform.PTransform` for push messages
    into an Apache Kafka topic.

    Examples:
        Examples:
        Pushing message to a Kafka Topic `notifications` ::

            import apache_beam as beam
            from apache_beam.options.pipeline_options import PipelineOptions
            from beam_nuggets.io import kafkaio

            kafka_topic = 'notifications'

            with beam.Pipeline(options=PipelineOptions()) as p:
                notifications = (p 
                                 | "Creating data" >> beam.Create(['{"device": "0001", status": "healthy"}'])
                                 | "Pushing messages to Kafka" >> kafkaio.KafkaProduce(
                                                                                        topic=kafka_topic,
                                                                                        servers="localhost:9092"
                                                                                    )
                                )
                notifications | 'Writing to stdout' >> beam.Map(print)

        The output will be something like ::

            ("notifications", '{"device": "0001", status": "healthy"}')

        Where the key is the Kafka topic published to and the element is the Kafka message produced
    """

    def __init__(self, topic=None, servers='127.0.0.1:9092'):
        """Initializes ``KafkaProduce``

        Args:
            topic: Kafka topic to publish to
            servers: list of Kafka servers to listen to

        """
        super(KafkaProduce, self).__init__()
        self._attributes = dict(
            topic=topic, 
            servers=servers)

    def expand(self, pcoll):
        return (
            pcoll 
            | ParDo(_ProduceKafkaMessage(self._attributes))
        )

class _ProduceKafkaMessage(DoFn):
    """Internal ``DoFn`` to publish message to Kafka topic"""

    def __init__(self, attributes, *args, **kwargs):
        super(_ProduceKafkaMessage, self).__init__(*args, **kwargs)
        self.attributes = attributes

    def process(self, element):
        producer = KafkaProducer(bootstrap_servers=self.attributes["servers"])

        try:
            producer.send(attributes['topic'], element.encode())
            yield (attributes['topic'], element)
        except Exception as e:
            print(e)