from __future__ import division, print_function

import apache_beam as beam


class AssignIncrId(beam.DoFn):
    def __init__(self, id_key='id', *unused_args, **unused_kwargs):
        super(AssignIncrId, self).__init__(*unused_args, **unused_kwargs)
        self.id_key = id_key
        self.id_counter = 0  # FIXME likely not enough!

    def process(self, element):
        """
        Args:
            element(dict):
        """
        element.update({self.id_key: self.id_counter})
        self.id_counter += 1
        yield element


class ReferBy(beam.DoFn):
    def __init__(self, key='id', *args, **kwargs):
        super(ReferBy, self).__init__(*args, **kwargs)
        self.key = key

    def process(self, element):
        yield (element[self.key], element)
