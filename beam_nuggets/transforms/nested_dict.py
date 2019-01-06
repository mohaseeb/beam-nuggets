from __future__ import division, print_function

import apache_beam as beam


class SelectFromNestedDict(beam.DoFn):
    """A :class:`~apache_beam.DoFn` for selecting subsets of records formed of
    nested dictionaries.

    Args:
        keys (list): list of dictionary keys to be selected. Each key is a
            string formed of dot separated words, each is used for selecting
            from a dict in the nested dicts. The order of the word in the
            "dot.separated.string" correspond to the dict level to select from.
            For instance, if the input record is ``{'a': {'b': 3, 'c': {'d': 2} }}``
            and keys is ``['a.b', 'a.c.d']``, the transform output will
            ``{'a_b': 3, 'a_c_d': 2} or {'b': 3, 'd': 2}`` depending on
            ``deepest_key_as_name `` below.
        deepest_key_as_name (bool): if set to True, the deepest selected fields
            keys will be used as names for the output dict keys.

    Examples:
        Select from records formed of nested dicts. ::

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

        The output will be something like:

            {'address_st': 'Jupiter', 'name_first': 'Star', 'email': 's@l.no'}
            {'address_st': 'Loon', 'name_first': 'Mark', 'email': 'm@s.no'}

    """

    def __init__(self, keys, deepest_key_as_name=False, *args, **kwargs):
        super(SelectFromNestedDict, self).__init__(*args, **kwargs)
        self._compiled_keys = self._compile_keys(keys, deepest_key_as_name)

    def process(self, element):
        """
        Args:
            element(dict):
        """

        yield {
            out_key: self._retrieve(nested_keys, element)
            for nested_keys, out_key in self._compiled_keys
        }

    @staticmethod
    def _retrieve(nested_keys, element):
        for key in nested_keys:
            element = element[key]
        return element

    @staticmethod
    def _compile_keys(keys, deepest_key_as_name):
        def _get_out_dict_key(nested_keys):
            if deepest_key_as_name:
                return nested_keys[-1]
            else:
                return '_'.join(nested_keys)

        return [
            (
                nested_keys,  # ['a', 'b'] used for retrieving nested values
                _get_out_dict_key(nested_keys),  # 'a_b' or 'b'
            )
            for nested_keys in [key.split('.') for key in keys]  # ['a.b']
        ]
