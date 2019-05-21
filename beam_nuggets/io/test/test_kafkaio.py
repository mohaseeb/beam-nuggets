import unittest

from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

from beam_nuggets.io import kafkaio
from .test_base import TransformBaseTest


class TestKafkaReadTransform(TransformBaseTest):

    def setUp(self):
        super(TestKafkaReadTransform, self).setUp()

    def test_ConsumeFromKafka(self):
        # create read pipeline, execute it and compare retrieved to actual rows
        with TestPipeline() as p:
            assert_that(
                p | "Reading records from db" >> relational_db.Read(
                    source_config=self.source_config,
                    table_name=self.table_name
                ),
                equal_to(self.table_rows)
            )