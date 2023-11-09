######################################################################
#
# File: test/unit/internal/test_emerge_planner.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

from b2sdk.http_constants import (
    GIGABYTE,
    MEGABYTE,
)
from b2sdk.transfer.emerge.planner.part_definition import (
    CopyEmergePartDefinition,
    UploadEmergePartDefinition,
    UploadSubpartsEmergePartDefinition,
)
from b2sdk.transfer.emerge.planner.planner import EmergePlanner
from b2sdk.transfer.emerge.planner.upload_subpart import (
    LocalSourceUploadSubpart,
    RemoteSourceUploadSubpart,
)
from b2sdk.transfer.emerge.write_intent import WriteIntent
from b2sdk.transfer.outbound.copy_source import CopySource as OrigCopySource
from b2sdk.transfer.outbound.upload_source import UploadSourceStream

from ..test_base import TestBase


class UploadSource(UploadSourceStream):
    def __init__(self, length):
        super().__init__(lambda: None, length)


class CopySource(OrigCopySource):
    def __init__(self, length):
        super().__init__(id(self), length=length)


def part(source_or_def_list, *offset_len):
    """ Helper for building emerge parts from outbound sources defs. Makes planner tests easier to read.

    Possible "def" structures:

        ``outbound_source`` - will build correct emerge part using type introspection with
                              ``0`` for ``offset`` and ``outbound_source.get_content_length()`` for ``length``

        ``outbound_source, offset, length`` - same as above, but given ``offset`` and ``length``

        ``[outbound_source_def,...]``` - will build emerge part using ``UploadSubpartsEmergePartDefinition``
                                         and type introspection for upload subparts; ``outbound_source_def`` is
                                         similar to above, either outbound source or tuple with source, offset, length
    Please notice that ``part([copy_source])`` is a single "small copy" - first download then upload, and
    ``part(copy_source) is "regular copy" - ``length`` is not verified here

    """
    if isinstance(source_or_def_list, list):
        assert not offset_len
        subparts = []
        for sub_part_meta in source_or_def_list:
            if isinstance(sub_part_meta, tuple):
                source, offset, length = sub_part_meta
            else:
                source = sub_part_meta
                offset = 0
                length = source.get_content_length()
            if isinstance(source, UploadSource):
                subparts.append(LocalSourceUploadSubpart(source, offset, length))
            else:
                subparts.append(RemoteSourceUploadSubpart(source, offset, length))
        return UploadSubpartsEmergePartDefinition(subparts)
    else:
        source = source_or_def_list
        if offset_len:
            offset, length = offset_len
        else:
            offset = 0
            length = source.get_content_length()
        if isinstance(source, UploadSource):
            return UploadEmergePartDefinition(source, offset, length)
        else:
            return CopyEmergePartDefinition(source, offset, length)


class TestEmergePlanner(TestBase):
    def setUp(self):
        # we want to hardcode here current production settings and change them
        # whenever these are changed on production, and relate all test lengths
        # to those sizes - if test assumes something about those values
        # (like `min_part_size` > 2 * MEGABYTE), then one should add assertion
        # at the beginning of test function body
        self.recommended_size = 100 * MEGABYTE
        self.min_size = 5 * MEGABYTE
        self.max_size = 5 * GIGABYTE
        self.planner = self._get_emerge_planner()

    def _get_emerge_planner(self):
        return EmergePlanner(
            min_part_size=self.min_size,
            recommended_upload_part_size=self.recommended_size,
            max_part_size=self.max_size,
        )

    # yapf: disable

    def test_part_sizes(self):
        self.assertGreater(self.min_size, 0)
        self.assertGreaterEqual(self.recommended_size, self.min_size)
        self.assertGreaterEqual(self.max_size, self.recommended_size)

    def test_simple_concatenate(self):
        sources = [
            CopySource(self.recommended_size),
            UploadSource(self.recommended_size),
            CopySource(self.recommended_size),
            UploadSource(self.recommended_size),
        ]

        self.verify_emerge_plan_for_write_intents(
            WriteIntent.wrap_sources_iterator(sources),
            [part(source) for source in sources],
        )

    def test_single_part_upload(self):
        source = UploadSource(self.recommended_size)

        self.verify_emerge_plan_for_write_intents(
            [WriteIntent(source)],
            [part(source)],
        )

    def test_single_part_copy(self):
        source = CopySource(self.max_size)

        self.verify_emerge_plan_for_write_intents(
            [WriteIntent(source)],
            [part(source)],
        )

    def test_single_multipart_upload(self):
        self.assertGreater(self.recommended_size, 2 * self.min_size)

        remainder = 2 * self.min_size
        source = UploadSource(self.recommended_size * 5 + remainder)
        expected_part_sizes = [self.recommended_size] * 5 + [remainder]

        self.verify_emerge_plan_for_write_intents(
            [WriteIntent(source)],
            self.split_source_to_part_defs(source, expected_part_sizes),
        )

    def test_recommended_part_size_decrease(self):
        source_upload1 = UploadSource(self.recommended_size * 10001)

        write_intents = [
            WriteIntent(source_upload1),
        ]
        emerge_plan = self.planner.get_emerge_plan(write_intents)
        assert len(emerge_plan.emerge_parts) < 10000

    def test_single_multipart_copy(self):
        source = CopySource(5 * self.max_size)

        self.verify_emerge_plan_for_write_intents(
            [WriteIntent(source)],
            self.split_source_to_part_defs(source, [self.max_size] * 5)
        )

    def test_single_multipart_copy_remainder(self):
        self.assertGreaterEqual(self.min_size, 2)

        source = CopySource(5 * self.max_size + int(self.min_size / 2))
        expected_part_count = 7
        base_part_size = int(source.get_content_length() / expected_part_count)
        size_remainder = source.get_content_length() % expected_part_count
        expected_part_sizes = (
            [base_part_size + 1] * size_remainder +
            [base_part_size] * (expected_part_count - size_remainder)
        )

        self.verify_emerge_plan_for_write_intents(
            [WriteIntent(source)],
            self.split_source_to_part_defs(source, expected_part_sizes),
        )

    def test_single_small_copy(self):
        source = CopySource(self.min_size - 1)

        self.verify_emerge_plan_for_write_intents(
            [WriteIntent(source)],
            [
                # single small copy should be processed using `copy_file`
                # which does not have minimum file size limit
                part(source),
            ],
        )

    def test_copy_then_small_copy(self):
        source_copy = CopySource(self.recommended_size)
        source_small_copy = CopySource(self.min_size - 1)
        write_intents = WriteIntent.wrap_sources_iterator([source_copy, source_small_copy])

        self.verify_emerge_plan_for_write_intents(
            write_intents,
            [
                part(source_copy),
                part([source_small_copy]),  # this means: download and then upload
            ],
        )

    def test_small_copy_then_copy(self):
        self.assertGreater(self.min_size, MEGABYTE)

        source_small_copy = CopySource(self.min_size - MEGABYTE)
        source_copy = CopySource(self.recommended_size)
        write_intents = WriteIntent.wrap_sources_iterator([source_small_copy, source_copy])

        self.verify_emerge_plan_for_write_intents(
            write_intents,
            [
                part([
                    source_small_copy,
                    (source_copy, 0, MEGABYTE),
                ]),
                part(source_copy, MEGABYTE, self.recommended_size - MEGABYTE)
            ],
        )

    def test_upload_small_copy_then_copy(self):
        source_upload = UploadSource(self.recommended_size)
        source_small_copy = CopySource(self.min_size - 1)
        source_copy = CopySource(self.recommended_size)
        write_intents = WriteIntent.wrap_sources_iterator([source_upload, source_small_copy, source_copy])

        self.verify_emerge_plan_for_write_intents(
            write_intents,
            [
                part([
                    source_upload,
                    source_small_copy,
                ]),
                part(source_copy),
            ]
        )

    def test_upload_small_copy_x2_then_copy(self):
        source_upload = UploadSource(self.recommended_size)
        source_small_copy1 = CopySource(length=self.min_size - 1)
        source_small_copy2 = CopySource(length=self.min_size - 1)
        source_copy = CopySource(self.recommended_size)
        write_intents = WriteIntent.wrap_sources_iterator(
            [source_upload, source_small_copy1, source_small_copy2, source_copy]
        )

        self.verify_emerge_plan_for_write_intents(
            write_intents,
            [
                part(source_upload),
                part([
                    source_small_copy1,
                    source_small_copy2,
                ]),
                part(source_copy),
            ],
        )

    def test_upload_multiple_sources(self):
        self.assertEqual(self.recommended_size % 8, 0)

        unit_part_size = int(self.recommended_size / 8)
        uneven_part_size = 3 * unit_part_size
        sources = [
            UploadSource(uneven_part_size)
            for i in range(8)
        ]

        self.verify_emerge_plan_for_write_intents(
            WriteIntent.wrap_sources_iterator(sources),
            [
                part([
                    sources[0],
                    sources[1],
                    (sources[2], 0, 2 * unit_part_size),
                ]),
                part([
                    (sources[2], 2 * unit_part_size, unit_part_size),
                    sources[3],
                    sources[4],
                    (sources[5], 0, unit_part_size),
                ]),
                part([
                    (sources[5], unit_part_size, 2 * unit_part_size),
                    sources[6],
                    sources[7],
                ]),
            ],
        )

    def test_small_upload_not_enough_copy_then_upload(self):
        self.assertGreater(self.min_size, 2 * MEGABYTE)

        source_small_upload = UploadSource(self.min_size - 2 * MEGABYTE)
        source_copy = CopySource(self.min_size + MEGABYTE)
        source_upload = UploadSource(self.recommended_size)

        write_intents = WriteIntent.wrap_sources_iterator(
            [source_small_upload, source_copy, source_upload]
        )
        small_parts_len = source_small_upload.get_content_length() + source_copy.get_content_length()
        source_upload_split_offset = self.recommended_size - small_parts_len

        self.verify_emerge_plan_for_write_intents(
            write_intents,
            [
                part([
                    source_small_upload,
                    source_copy,
                    (source_upload, 0, source_upload_split_offset),
                ]),
                part(source_upload, source_upload_split_offset, small_parts_len),
            ],
        )

    def test_basic_local_overlap(self):
        source1 = UploadSource(self.recommended_size * 2)
        source2 = UploadSource(self.recommended_size * 2)
        write_intents = [
            WriteIntent(source1),
            WriteIntent(source2, destination_offset=self.recommended_size),
        ]

        self.verify_emerge_plan_for_write_intents(
            write_intents,
            [part(source1, 0, self.recommended_size)] +
            self.split_source_to_part_defs(source2, [self.recommended_size] * 2),
        )

    def test_local_stairs_overlap(self):
        """
        intent 0 ####
        intent 1  ####
        intent 2   ####
        intent 3    ####
        """
        self.assertEqual(self.recommended_size % 4, 0)

        shift = int(self.recommended_size / 4)
        sources = [UploadSource(self.recommended_size) for i in range(4)]
        write_intents = [
            WriteIntent(source, destination_offset=i * shift)
            for i, source in enumerate(sources)
        ]

        three_quarters = int(3 * self.recommended_size / 4)
        #      1234567
        # su1: ****
        # su2:  XXXX
        # su3:   XXXX
        # su4:    X***
        self.verify_emerge_plan_for_write_intents(
            write_intents,
            [
                part([
                    (sources[0], 0, three_quarters),
                    (sources[-1], 0, shift)
                ]),
                part(sources[-1], shift, three_quarters),
            ],
        )

    def test_local_remote_overlap_start(self):
        source_upload = UploadSource(self.recommended_size * 2)
        source_copy = CopySource(self.recommended_size)
        write_intents = [
            WriteIntent(source_upload),
            WriteIntent(source_copy),
        ]

        self.verify_emerge_plan_for_write_intents(
            write_intents,
            [
                part(source_copy),
                part(source_upload, self.recommended_size, self.recommended_size),
            ],
        )

    def test_local_remote_overlap_end(self):
        source_upload = UploadSource(self.recommended_size * 2)
        source_copy = CopySource(self.recommended_size)
        write_intents = [
            WriteIntent(source_upload),
            WriteIntent(source_copy, destination_offset=self.recommended_size),
        ]

        self.verify_emerge_plan_for_write_intents(
            write_intents,
            [
                part(source_upload, 0, self.recommended_size),
                part(source_copy),
            ],
        )

    def test_local_remote_overlap_middle(self):
        source_upload = UploadSource(self.recommended_size * 3)
        source_copy = CopySource(self.recommended_size)
        write_intents = [
            WriteIntent(source_upload),
            WriteIntent(source_copy, destination_offset=self.recommended_size),
        ]

        self.verify_emerge_plan_for_write_intents(
            write_intents,
            [
                part(source_upload, 0, self.recommended_size),
                part(source_copy),
                part(source_upload, 2 * self.recommended_size, self.recommended_size),
            ],
        )

    def test_local_small_copy_overlap(self):
        self.assertGreater(self.recommended_size, self.min_size * 3 - 3)
        source_upload = UploadSource(self.recommended_size)
        small_size = self.min_size - 1
        source_copy1 = CopySource(small_size)
        source_copy2 = CopySource(small_size)
        source_copy3 = CopySource(small_size)
        write_intents = [
            WriteIntent(source_upload),
            WriteIntent(source_copy1),
            WriteIntent(source_copy2, destination_offset=small_size),
            WriteIntent(source_copy3, destination_offset=2 * small_size),
        ]

        self.verify_emerge_plan_for_write_intents(
            write_intents,
            [part(source_upload)],
        )

    def test_overlap_cause_small_copy_remainder_2_intent_case(self):
        self.assertGreater(self.min_size, 2 * MEGABYTE)
        copy_size = self.min_size + MEGABYTE
        copy_overlap_offset = copy_size - 2 * MEGABYTE

        source_copy1 = CopySource(copy_size)
        source_copy2 = CopySource(self.min_size)

        write_intents = [
            WriteIntent(source_copy1),
            WriteIntent(source_copy2, destination_offset=copy_overlap_offset),
        ]

        #      123456789
        # sc1: ******
        # sc2:     XX***
        self.verify_emerge_plan_for_write_intents(
            write_intents,
            [
                part(source_copy1, 0, copy_size),
                part([
                    (source_copy2, 2 * MEGABYTE, self.min_size - 2 * MEGABYTE),  # this means: download and then upload
                ]),
            ],
        )

    def test_overlap_cause_small_copy_remainder_3_intent_case(self):
        self.assertGreater(self.min_size, MEGABYTE)
        copy_size = self.min_size + MEGABYTE
        copy_overlap_offset = copy_size - 2 * MEGABYTE

        source_copy1 = CopySource(copy_size)
        source_copy2 = CopySource(copy_size)
        source_copy3 = CopySource(copy_size)

        write_intents = [
            WriteIntent(source_copy1),
            WriteIntent(source_copy2, destination_offset=copy_overlap_offset),
            WriteIntent(source_copy3, destination_offset=2 * copy_overlap_offset),
        ]

        #      12345678901234
        # sc1: *****X
        # sc2:     X*****
        # sc3:         XX****
        self.verify_emerge_plan_for_write_intents(
            write_intents,
            [
                part(source_copy1, 0, self.min_size),
                part(source_copy2, MEGABYTE, self.min_size),
                part([
                    (source_copy3, 2 * MEGABYTE, copy_overlap_offset),  # this means: download and then upload
                ]),
            ],
        )

    def test_overlap_protected_copy_and_upload(self):
        self.assertGreater(self.min_size, MEGABYTE)
        self.assertGreater(self.recommended_size, 2 * self.min_size)
        copy_size = self.min_size + MEGABYTE
        copy_overlap_offset = copy_size - 2 * MEGABYTE

        source_upload = UploadSource(self.recommended_size)
        source_copy1 = CopySource(copy_size)
        source_copy2 = CopySource(copy_size)

        write_intents = [
            WriteIntent(source_upload),
            WriteIntent(source_copy1),
            WriteIntent(source_copy2, destination_offset=copy_overlap_offset),
        ]

        upload_offset = copy_overlap_offset + copy_size
        #      123456789012
        #  su: XXXXXXXXXX**(...)
        # sc1: *****X
        # sc2:     X*****
        self.verify_emerge_plan_for_write_intents(
            write_intents,
            [
                part(source_copy1, 0, self.min_size),
                part(source_copy2, MEGABYTE, self.min_size),
                part(source_upload, upload_offset, self.recommended_size - upload_offset),
            ],
        )

    def test_overlap_copy_and_small_copy_remainder_and_upload(self):
        self.assertGreater(self.min_size, 2 * MEGABYTE)
        self.assertGreater(self.recommended_size, self.min_size + MEGABYTE)

        copy_size = self.min_size + MEGABYTE
        copy_overlap_offset = copy_size - 2 * MEGABYTE

        source_upload = UploadSource(self.recommended_size)
        source_copy1 = CopySource(copy_size)
        source_copy2 = CopySource(self.min_size)

        write_intents = [
            WriteIntent(source_upload),
            WriteIntent(source_copy1),
            WriteIntent(source_copy2, destination_offset=copy_overlap_offset),
        ]

        #      12345678901
        #  su: XXXXXX*****(...)
        # sc1: ******
        # sc2:     XXXXX
        self.verify_emerge_plan_for_write_intents(
            write_intents,
            [
                part(source_copy1, 0, copy_size),
                part(source_upload, copy_size, self.recommended_size - copy_size),
            ],
        )

    def test_raise_on_hole(self):
        source_upload1 = UploadSource(self.recommended_size)
        source_upload2 = UploadSource(self.recommended_size)
        source_copy1 = CopySource(self.recommended_size)
        source_copy2 = CopySource(self.recommended_size)

        write_intents = [
            WriteIntent(source_upload1),
            WriteIntent(source_upload2, destination_offset=self.recommended_size + 2 * MEGABYTE),
            WriteIntent(source_copy1, destination_offset=MEGABYTE),
            WriteIntent(source_copy2, destination_offset=self.recommended_size + 3 * MEGABYTE),
        ]

        hole_msg = ('Cannot emerge file with holes. '
                    'Found hole range: ({}, {})'.format(
                        write_intents[2].destination_end_offset,
                        write_intents[1].destination_offset,
                    ))
        with self.assertRaises(ValueError, hole_msg):
            self.planner.get_emerge_plan(write_intents)

    def test_empty_upload(self):
        source_upload = UploadSource(0)
        self.verify_emerge_plan_for_write_intents(
            [WriteIntent(source_upload)],
            [part(source_upload)],
        )

    # yapf: enable

    def verify_emerge_plan_for_write_intents(self, write_intents, expected_part_defs):
        emerge_plan = self.planner.get_emerge_plan(write_intents)

        self.assert_same_part_definitions(emerge_plan, expected_part_defs)

    def split_source_to_part_defs(self, source, part_sizes):
        if isinstance(source, UploadSource):
            def_class = UploadEmergePartDefinition
        else:
            def_class = CopyEmergePartDefinition

        expected_part_defs = []
        current_offset = 0
        for part_size in part_sizes:
            expected_part_defs.append(def_class(source, current_offset, part_size))
            current_offset += part_size
        return expected_part_defs

    def assert_same_part_definitions(self, emerge_plan, expected_part_defs):
        emerge_parts = list(emerge_plan.emerge_parts)
        self.assertEqual(len(emerge_parts), len(expected_part_defs))
        for emerge_part, expected_part_def in zip(emerge_parts, expected_part_defs):
            emerge_part_def = emerge_part.part_definition
            self.assertIs(emerge_part_def.__class__, expected_part_def.__class__)
            if isinstance(emerge_part_def, UploadSubpartsEmergePartDefinition):
                upload_subparts = emerge_part_def.upload_subparts
                expected_subparts = expected_part_def.upload_subparts
                self.assertEqual(len(upload_subparts), len(expected_subparts))
                for subpart, expected_subpart in zip(upload_subparts, expected_subparts):
                    self.assertIs(subpart.__class__, expected_subpart.__class__)
                    self.assertIs(subpart.outbound_source, expected_subpart.outbound_source)
                    self.assertEqual(subpart.relative_offset, expected_subpart.relative_offset)
                    self.assertEqual(subpart.length, expected_subpart.length)
            else:
                if isinstance(emerge_part_def, UploadEmergePartDefinition):
                    self.assertIs(emerge_part_def.upload_source, expected_part_def.upload_source)
                else:
                    self.assertIs(emerge_part_def.copy_source, expected_part_def.copy_source)
                self.assertEqual(emerge_part_def.relative_offset, expected_part_def.relative_offset)
                self.assertEqual(emerge_part_def.length, expected_part_def.length)
