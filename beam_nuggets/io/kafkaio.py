from __future__ import division, print_function

from apache_beam import PTransform, ParDo, DoFn, Create
from kafka import KafkaConsumer, KafkaProducer


class KafkaConsume(PTransform):
    """A :class:`~apache_beam.transforms.ptransform.PTransform` for reading from an Apache Kafka topic. This is a streaming
    Transform that never returns. The transform uses `KafkaConsumer` from the
    `kafka` python library.

    It outputs a :class:`~apache_beam.pvalue.PCollection` of
    ``key-values:s``, each object is a Kafka message in the form (msg-key, msg)

    Args:
        consumer_config (dict): the kafka consumer configuration. The
            supported configurations are those of `KafkaConsumer` from
            the `kafka` python library.

    Examples:
        Consuming from a Kafka Topic `notifications` ::

            from __future__ import print_function
            import apache_beam as beam
            from apache_beam.options.pipeline_options import PipelineOptions
            from beam_nuggets.io import kafkaio

            kafka_topic = "notifications"
            kafka_config = {"topic": kafka_topic,
                            "bootstrap_servers": "localhost:9092",
                            "group_id": "notification_consumer_group"}

            with beam.Pipeline(options=PipelineOptions()) as p:
                notifications = p | "Reading messages from Kafka" >> kafkaio.KafkaConsume(kafka_config)
                notifications | 'Writing to stdout' >> beam.Map(print)

        The output will be something like ::

            ("device 1", {"status": "healthy"})
            ("job #2647", {"status": "failed"})

        Where the first element of the tuple is the Kafka message key and the second element is the Kafka message being passed through the topic
    """

    def __init__(self, consumer_config, *args, **kwargs):
        """Initializes ``KafkaConsume``
        """
        super(KafkaConsume, self).__init__()
        self._config = consumer_config

    def expand(self, pcoll):
        return (
            pcoll
            | Create([self._config])
            | ParDo(_ConsumeKafkaTopic())
        )


class _ConsumeKafkaTopic(DoFn):
    """Internal ``DoFn`` to read from Kafka topic and return messages"""

    def process(self, config):
        consumer_config = dict(config)
        topic = consumer_config.pop('topic')
        consumer = KafkaConsumer(topic, **consumer_config)

        for msg in consumer:
            try:
                yield (msg.key, msg.value.decode())
            except Exception as e:
                print(e)
                continue


class KafkaProduce(PTransform):
    """A :class:`~apache_beam.transforms.ptransform.PTransform` for pushing messages
    into an Apache Kafka topic. This class expects a tuple with the first element being the message key
    and the second element being the message. The transform uses `KafkaProducer`
    from the `kafka` python library.

    Args:
        topic: Kafka topic to publish to
        servers: list of Kafka servers to listen to

    Examples:
        Examples:
        Pushing message to a Kafka Topic `notifications` ::

            from __future__ import print_function
            import apache_beam as beam
            from apache_beam.options.pipeline_options import PipelineOptions
            from beam_nuggets.io import kafkaio

            with beam.Pipeline(options=PipelineOptions()) as p:
                notifications = (p 
                                 | "Creating data" >> beam.Create([('dev_1', '{"device": "0001", status": "healthy"}')])
                                 | "Pushing messages to Kafka" >> kafkaio.KafkaProduce(
                                                                                        topic='notifications',
                                                                                        servers="localhost:9092"
                                                                                    )
                                )
                notifications | 'Writing to stdout' >> beam.Map(print)

        The output will be something like ::

            ("dev_1", '{"device": "0001", status": "healthy"}')

        Where the key is the Kafka topic published to and the element is the Kafka message produced
    """

    def __init__(self, topic=None, servers='127.0.0.1:9092'):
        """Initializes ``KafkaProduce``
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

    def start_bundle(self):
        self._producer = KafkaProducer(bootstrap_servers=self.attributes["servers"])

    def process(self, element):
        try:
            self._producer.send(self.attributes['topic'], element[1].encode(), key=element[0].encode())
            yield element
        except Exception as e:
            raise
        finally:
            self._producer.close()