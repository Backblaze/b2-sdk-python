import collections
from typing import Union

from apiver_deps import Bucket, BucketStructure, ValueNotSet


def get_all_annotations(class_: type):
    return dict(
        collections.ChainMap(*(getattr(cls, '__annotations__', {}) for cls in class_.__mro__))
    )


def test_bucket_annotations():
    expected_structure_annotations = {}
    for instance_var_name, type_ in get_all_annotations(Bucket).items():
        if instance_var_name == 'api':
            continue
        expected_structure_annotations[instance_var_name] = Union[type_, ValueNotSet]
    assert expected_structure_annotations == get_all_annotations(BucketStructure)
