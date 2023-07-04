######################################################################
#
# File: b2sdk/v1/sync/file_to_path_translator.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk import v2

from .file import B2File, B2FileVersion, File, FileVersion


# The goal is to create v1.File objects together with v1.FileVersion objects from v2.SyncPath objects
def make_files_from_paths(
    dest_path: v2.AbstractSyncPath, source_path: v2.AbstractSyncPath, sync_type: str
) -> tuple[File, File]:
    assert sync_type in ('b2-to-b2', 'b2-to-local', 'local-to-b2')
    sync_type_split = sync_type.split('-')

    dest_type = sync_type_split[-1]
    dest_file = _path_translation_map[dest_type](dest_path)

    source_type = sync_type_split[0]
    source_file = _path_translation_map[source_type](source_path)

    return dest_file, source_file


def _translate_b2_path_to_file(path: v2.B2SyncPath) -> B2File:
    versions = [B2FileVersion(version) for version in path.all_versions]
    return B2File(path.relative_path, versions)


def _translate_local_path_to_file(path: v2.LocalSyncPath) -> File:
    version = FileVersion(
        id_=path.absolute_path,
        file_name=path.relative_path,
        mod_time=path.mod_time,
        action='upload',
        size=path.size,
    )
    return File(path.relative_path, [version])


_path_translation_map = {'b2': _translate_b2_path_to_file, 'local': _translate_local_path_to_file}


# The goal is to create v2.SyncPath objects from v1.File objects
def make_paths_from_files(dest_file: File, source_file: File,
                          sync_type: str) -> tuple[v2.AbstractSyncPath, v2.AbstractSyncPath]:
    assert sync_type in ('b2-to-b2', 'b2-to-local', 'local-to-b2')
    sync_type_split = sync_type.split('-')

    dest_type = sync_type_split[-1]
    dest_path = _file_translation_map[dest_type](dest_file)

    source_type = sync_type_split[0]
    source_path = _file_translation_map[source_type](source_file)

    return dest_path, source_path


def _translate_b2_file_to_path(file: B2File) -> v2.AbstractSyncPath:
    versions = [file_version.file_version_info for file_version in file.versions]

    return v2.B2SyncPath(
        relative_path=file.name, selected_version=versions[0], all_versions=versions
    )


def _translate_local_file_to_path(file: File) -> v2.AbstractSyncPath:
    return v2.LocalSyncPath(
        absolute_path=file.latest_version().id_,
        relative_path=file.name,
        mod_time=file.latest_version().mod_time,
        size=file.latest_version().size
    )


_file_translation_map = {'b2': _translate_b2_file_to_path, 'local': _translate_local_file_to_path}
