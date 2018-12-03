from __future__ import division, print_function

import apache_beam as beam


class SelectFromNestedDict(beam.DoFn):
    """
    input:
        {'a': {'b': 3, 'c': {'d': 2} }}

    keys:
        ['a.b', 'a.c.d']

    output:
        {'a_b': 3, 'a_c_d': 2} or {'b': 3, 'd': 2}
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
