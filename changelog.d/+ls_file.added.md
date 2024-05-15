Add `folder_to_list_can_be_a_file` parameter to `b2sdk.v2.Bucket.ls`, that if set to `True` will allow listing a file versions if path is an exact match.
This parameter won't be included in `b2sdk.v3.Bucket.ls` and unless supplied `path` ends with `/`, the possibility of path pointing to file will be considered first.
