from __future__ import division, print_function

import apache_beam as beam


class SelectKeys(beam.DoFn):
    """
    input:
        {
            'a': {
                'b': 3
                'c': {
                    'd': 2
                }
            }
        }

    select:
        [
           'a.b',
           'a.c.d'
        ]

    output:
        {
            'a.b': 3,
            'a.c.d': 2
        }

    """

    def __init__(self, keys, *unused_args, **unused_kwargs):
        super(SelectKeys, self).__init__(*unused_args, **unused_kwargs)
        self.keys = [
            (
                split_key[-1],  # key name:  'b'
                split_key # key parts ['a', 'b']
            )
            for split_key in [key.split('.') for key in keys]
        ]

    def process(self, element):
        """
        Args:
            element(dict):
        """

        yield {
            key_name: self._retrieve(key_parts, element)
            for key_name, key_parts in self.keys
        }

    @staticmethod
    def _retrieve(key_parts, element):
        for key_part in key_parts:
            element = element[key_part]
        return element
