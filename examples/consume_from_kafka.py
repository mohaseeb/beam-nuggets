import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_nuggets.io import kafkaio

consumer_config = {"topic": "notifications",
                   "bootstrap_servers": "localhost:9092",
                   "group_id": "notification_consumer_group"}

with beam.Pipeline(options=PipelineOptions()) as p:
    notifications = p | "Reading messages from Kafka" >> kafkaio.KafkaConsume(
        consumer_config=consumer_config,
        value_decoder=bytes.decode,  # optional
    )
    notifications | 'Writing to stdout' >> beam.Map(print)
