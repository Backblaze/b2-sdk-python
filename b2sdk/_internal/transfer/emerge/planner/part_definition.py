######################################################################
#
# File: b2sdk/_internal/transfer/emerge/planner/part_definition.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from abc import ABCMeta, abstractmethod
from functools import partial
from typing import TYPE_CHECKING

from b2sdk._internal.stream.chained import ChainedStream
from b2sdk._internal.stream.range import wrap_with_range
from b2sdk._internal.utils import hex_sha1_of_unlimited_stream

if TYPE_CHECKING:
    from b2sdk._internal.transfer.emerge.unbound_write_intent import UnboundSourceBytes


class BaseEmergePartDefinition(metaclass=ABCMeta):
    @abstractmethod
    def get_length(self):
        pass

    @abstractmethod
    def get_part_id(self):
        pass

    @abstractmethod
    def get_execution_step(self, execution_step_factory):
        pass

    def is_hashable(self):
        return False

    def get_sha1(self):
        return None


class UploadEmergePartDefinition(BaseEmergePartDefinition):
    def __init__(self, upload_source: UnboundSourceBytes, relative_offset, length):
        self.upload_source = upload_source
        self.relative_offset = relative_offset
        self.length = length
        self._sha1 = None

    def __repr__(self):
        return (
            '<{classname} upload_source={upload_source} relative_offset={relative_offset} '
            'length={length}>'
        ).format(
            classname=self.__class__.__name__,
            upload_source=repr(self.upload_source),
            relative_offset=self.relative_offset,
            length=self.length,
        )

    def get_length(self):
        return self.length

    def get_part_id(self):
        return self.get_sha1()

    def is_hashable(self):
        return True

    def get_sha1(self):
        if self._sha1 is None:
            if self.relative_offset == 0 and self.length == self.upload_source.get_content_length():
                # this is part is equal to whole upload source - so we use `get_content_sha1()`
                # and if sha1 is already given, we skip computing it again
                self._sha1 = self.upload_source.get_content_sha1()
            else:
                with self._get_stream() as stream:
                    self._sha1, _ = hex_sha1_of_unlimited_stream(stream)
        return self._sha1

    def get_execution_step(self, execution_step_factory):
        return execution_step_factory.create_upload_execution_step(
            self._get_stream,
            stream_length=self.length,
            stream_sha1=self.get_sha1(),
        )

    def _get_stream(self):
        fp = self.upload_source.open()
        return wrap_with_range(
            fp, self.upload_source.get_content_length(), self.relative_offset, self.length
        )


class UploadSubpartsEmergePartDefinition(BaseEmergePartDefinition):
    def __init__(self, upload_subparts):
        self.upload_subparts = upload_subparts
        self._is_hashable = all(subpart.is_hashable() for subpart in upload_subparts)
        self._sha1 = None

    def __repr__(self):
        return f'<{self.__class__.__name__} upload_subparts={repr(self.upload_subparts)}>'

    def get_length(self):
        return sum(subpart.length for subpart in self.upload_subparts)

    def get_part_id(self):
        if self.is_hashable():
            return self.get_sha1()
        else:
            return tuple(subpart.get_subpart_id() for subpart in self.upload_subparts)

    def is_hashable(self):
        return self._is_hashable

    def get_sha1(self):
        if self._sha1 is None and self.is_hashable():
            with self._get_stream() as stream:
                self._sha1, _ = hex_sha1_of_unlimited_stream(stream)
        return self._sha1

    def get_execution_step(self, execution_step_factory):
        return execution_step_factory.create_upload_execution_step(
            partial(self._get_stream, emerge_execution=execution_step_factory.emerge_execution),
            stream_length=self.get_length(),
            stream_sha1=self.get_sha1(),
        )

    def _get_stream(self, emerge_execution=None):
        return ChainedStream(
            [
                subpart.get_stream_opener(emerge_execution=emerge_execution)
                for subpart in self.upload_subparts
            ]
        )


class CopyEmergePartDefinition(BaseEmergePartDefinition):
    def __init__(self, copy_source, relative_offset, length):
        self.copy_source = copy_source
        self.relative_offset = relative_offset
        self.length = length

    def __repr__(self):
        return (
            '<{classname} copy_source={copy_source} relative_offset={relative_offset} '
            'length={length}>'
        ).format(
            classname=self.__class__.__name__,
            copy_source=repr(self.copy_source),
            relative_offset=self.relative_offset,
            length=self.length,
        )

    def get_length(self):
        return self.length

    def get_part_id(self):
        return (self.copy_source.file_id, self.relative_offset, self.length)

    def get_execution_step(self, execution_step_factory):
        return execution_step_factory.create_copy_execution_step(
            self.copy_source.get_copy_source_range(self.relative_offset, self.length)
        )
