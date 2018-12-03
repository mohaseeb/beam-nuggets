from __future__ import division, print_function

import apache_beam as beam


class AssignUniqueId(beam.DoFn):
    def __init__(self, id_key='id', *unused_args, **unused_kwargs):
        super(AssignUniqueId, self).__init__(*unused_args, **unused_kwargs)
        self.id_key = id_key
        self.id_counter = 0  # FIXME likely not enough for parallel jobs

    def process(self, element):
        """
        Args:
            element(dict):
        """
        element.update({self.id_key: self.id_counter})
        self.id_counter += 1
        yield element