from __future__ import division, print_function

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

from beam_nuggets.transforms import SelectFromNestedDict

inputs = [
    {
        'name': {'first': 'Star', 'second': 'Light'},
        'address': {'st': 'Jupiter', 'flat': 3},
        'email': 's@l.no'
    },
    {
        'name': {'first': 'Mark', 'second': 'Sight'},
        'address': {'st': 'Loon', 'flat': 5},
        'email': 'm@s.no'
    }
]
with beam.Pipeline(options=PipelineOptions()) as p:
    nested = p | "Reading nested dicts" >> beam.Create(inputs)
    transformed = nested | "filtering" >> beam.ParDo(SelectFromNestedDict(
        keys=['name.first', 'address.st', 'email'],
        # deepest_key_as_name=True,
    ))
    transformed | 'Writing to stdout' >> beam.Map(print)

"""
$ python get_from_nested_dict.py 
{'address_st': 'Jupiter', 'name_first': 'Star', 'email': 's@l.no'}
{'address_st': 'Loon', 'name_first': 'Mark', 'email': 'm@s.no'}
# if deepest_key_as_name is set True, output will be
# {'st': 'Jupiter', 'email': 's@l.no', 'first': 'Star'}
# {'st': 'Loon', 'email': 'm@s.no', 'first': 'Mark'}
"""
