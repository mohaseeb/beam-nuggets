from __future__ import division, print_function

import json

import apache_beam as beam


class ParseJson(beam.DoFn):

    def __init__(self, only_keys=None, *unused_args, **unused_kwargs):
        self.only_keys = only_keys
        super(ParseJson, self).__init__(*unused_args, **unused_kwargs)

    def process(self, element):
        """
        Args:
            element(dict):
        """
        yield {
            k: json.loads(v) if k in self.only_keys else v
            for k, v in element.iteritems()
        }
