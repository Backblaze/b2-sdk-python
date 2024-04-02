######################################################################
#
# File: b2sdk/_internal/transfer/emerge/planner/planner.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import hashlib
import json
import typing
from abc import ABCMeta, abstractmethod
from collections import deque
from math import ceil

from b2sdk._internal.exception import InvalidUserInput
from b2sdk._internal.http_constants import (
    DEFAULT_MAX_PART_SIZE,
    DEFAULT_MIN_PART_SIZE,
    DEFAULT_RECOMMENDED_UPLOAD_PART_SIZE,
)
from b2sdk._internal.transfer.emerge.planner.part_definition import (
    CopyEmergePartDefinition,
    UploadEmergePartDefinition,
    UploadSubpartsEmergePartDefinition,
)
from b2sdk._internal.transfer.emerge.planner.upload_subpart import (
    LocalSourceUploadSubpart,
    RemoteSourceUploadSubpart,
)
from b2sdk._internal.utils import iterator_peek

if typing.TYPE_CHECKING:
    from b2sdk._internal.account_info.abstract import AbstractAccountInfo


class UploadBuffer:
    """ data container used by EmergePlanner for temporary storage of write intents """

    def __init__(self, start_offset, buff=None):
        self._start_offset = start_offset
        self._buff = buff or []
        if self._buff:
            self._end_offset = self._buff[-1][1]
        else:
            self._end_offset = self._start_offset

    @property
    def start_offset(self):
        return self._start_offset

    @property
    def end_offset(self):
        return self._end_offset

    @property
    def length(self):
        return self.end_offset - self.start_offset

    def intent_count(self):
        return len(self._buff)

    def get_intent(self, index):
        return self.get_item(index)[0]

    def get_item(self, index):
        return self._buff[index]

    def iter_items(self):
        return iter(self._buff)

    def append(self, intent, fragment_end):
        self._buff.append((intent, fragment_end))
        self._end_offset = fragment_end

    def get_slice(self, start_idx=None, end_idx=None, start_offset=None):
        start_idx = start_idx or 0
        buff_slice = self._buff[start_idx:end_idx]
        if start_offset is None:
            if start_idx == 0:
                start_offset = self.start_offset
            else:
                start_offset = self._buff[start_idx - 1:start_idx][0][1]
        return self.__class__(start_offset, buff_slice)


def _filter_out_none(*args):
    return (arg for arg in args if arg is not None)


class EmergePlanner:
    """ Creates a list of actions required for advanced creation of an object in the cloud from an iterator of write intent objects """

    def __init__(
        self,
        min_part_size: int | None = None,
        recommended_upload_part_size: int | None = None,
        max_part_size: int | None = None,
    ):
        # ensure default values do not break min<=recommended<=max condition,
        # while respecting user input and not auto fixing if something was provided explicitly
        self.min_part_size = min(
            DEFAULT_MIN_PART_SIZE, *_filter_out_none(recommended_upload_part_size, max_part_size)
        ) if min_part_size is None else min_part_size
        self.recommended_upload_part_size = recommended_upload_part_size or max(
            DEFAULT_RECOMMENDED_UPLOAD_PART_SIZE, self.min_part_size
        )
        self.max_part_size = max_part_size or max(
            DEFAULT_MAX_PART_SIZE, self.recommended_upload_part_size
        )
        if self.min_part_size > self.recommended_upload_part_size:
            raise InvalidUserInput(
                f"min_part_size value ({self.min_part_size}) exceeding recommended_upload_part_size value ({self.recommended_upload_part_size})"
            )
        if self.recommended_upload_part_size > self.max_part_size:
            raise InvalidUserInput(
                f"recommended_upload_part_size value ({self.recommended_upload_part_size}) exceeding max_part_size value ({self.max_part_size})"
            )

    @classmethod
    def from_account_info(
        cls,
        account_info: AbstractAccountInfo,
        min_part_size=None,
        recommended_upload_part_size=None,
        max_part_size=None
    ):
        if recommended_upload_part_size is None:
            recommended_upload_part_size = account_info.get_recommended_part_size()
            # AccountInfo defaults should not break the min<=recommended<=max condition when
            # other params were provided explicitly
            if min_part_size is not None:
                recommended_upload_part_size = max(recommended_upload_part_size, min_part_size)
            if max_part_size is not None:
                recommended_upload_part_size = min(recommended_upload_part_size, max_part_size)
        kwargs = {
            'min_part_size': min_part_size,
            'recommended_upload_part_size': recommended_upload_part_size,
            'max_part_size': max_part_size,
        }
        return cls(**{key: value for key, value in kwargs.items() if value is not None})

    def get_emerge_plan(self, write_intents):
        write_intents = sorted(write_intents, key=lambda intent: intent.destination_offset)

        # the upload part size recommended by the server causes errors with files larger than 1TB
        # (with the current 100MB part size and 10000 part count limit).
        # Therefore here we increase the recommended upload part size if needed.
        # the constant is for handling mixed upload/copy in concatenate etc
        max_destination_offset = max(intent.destination_end_offset for intent in write_intents)
        self.recommended_upload_part_size = max(
            self.recommended_upload_part_size,
            min(
                ceil(1.5 * max_destination_offset / 10000),
                self.max_part_size,
            )
        )
        assert self.min_part_size <= self.recommended_upload_part_size <= self.max_part_size, (
            self.min_part_size, self.recommended_upload_part_size, self.max_part_size
        )
        return self._get_emerge_plan(write_intents, EmergePlan)

    def get_streaming_emerge_plan(self, write_intent_iterator):
        return self._get_emerge_plan(write_intent_iterator, StreamingEmergePlan)

    def get_unbound_emerge_plan(self, write_intent_iterator):
        """
        For unbound streams we skip the whole process of bunching different parts together,
        validating them and splitting by operation type. We can do this, because:
        1. there will be no copy operations at all;
        2. we don't want to pull more data than actually needed;
        3. all the data is ordered;
        4. we don't want anything else to touch our buffers.
        Furthermore, we're using StreamingEmergePlan, as it checks whether we have one or more
        chunks to work with, and picks a proper upload method.
        """
        return StreamingEmergePlan(self._get_simple_emerge_parts(write_intent_iterator))

    def _get_simple_emerge_parts(self, write_intent_iterator):
        # Assumption here is that we need to do no magic. We are receiving
        # a read-only stream that cannot be seeked and is only for uploading
        # purposes. Moreover, we assume that each write intent we received is
        # a nice, enclosed buffer with enough data to make the cloud happy.
        for write_intent in write_intent_iterator:
            yield UploadEmergePartDefinition(
                write_intent.outbound_source,
                relative_offset=0,
                length=write_intent.length,
            )

    def _get_emerge_plan(self, write_intent_iterator, plan_class):
        return plan_class(
            self._get_emerge_parts(
                self._select_intent_fragments(self._validatation_iterator(write_intent_iterator))
            )
        )

    def _get_emerge_parts(self, intent_fragments_iterator):
        # This is where the magic happens. Instead of describing typical inputs and outputs here,
        # We've put them in tests. It is recommended to read those tests before trying to comprehend
        # the implementation details of this function.
        min_part_size = self.min_part_size

        # this stores current intent that we need to process - we may get
        # it in fragments so we want to glue just by updating `current_end`
        current_intent = None
        current_end = 0

        upload_buffer = UploadBuffer(0)
        first = True
        last = False
        for intent, fragment_end in intent_fragments_iterator:
            if current_intent is None:
                # this is a first loop run - just initialize current intent
                current_intent = intent
                current_end = fragment_end
                continue

            if intent is current_intent:
                # new intent is the same as previously processed intent, so lets glue them together
                # this happens when the caller splits N overlapping intents into overlapping fragments
                # and two fragments from the same intent end up streaming into here one after the other
                current_end = fragment_end
                continue

            if intent is None:
                last = True

            # incoming intent is different - this means that now we have to decide what to do:
            # if this is a copy intent and we want to copy it server-side, then we have to
            # flush the whole upload buffer we accumulated so far, but OTOH we may decide that we just want to
            # append it to upload buffer (see complete, untrivial logic below) and then maybe
            # flush some upload parts from upload buffer (if there is enough in the buffer)

            current_len = current_end - upload_buffer.end_offset
            # should we flush the upload buffer or do we have to add a chunk of the copy first?
            if current_intent.is_copy() and current_len >= min_part_size:
                # check if we can flush upload buffer or there is some missing bytes to fill it to `min_part_size`
                if upload_buffer.intent_count() > 0 and upload_buffer.length < min_part_size:
                    missing_length = min_part_size - upload_buffer.length
                else:
                    missing_length = 0
                if missing_length > 0 and current_len - missing_length < min_part_size:
                    # current intent is *not* a "small copy", but upload buffer is small
                    # and current intent is too short with the buffer to reach the minimum part size
                    # so we append current intent to upload buffer
                    upload_buffer.append(current_intent, current_end)
                else:
                    if missing_length > 0:
                        # we "borrow" a fragment of current intent to upload buffer
                        # to fill it to minimum part size
                        upload_buffer.append(
                            current_intent, upload_buffer.end_offset + missing_length
                        )
                    # completely flush the upload buffer
                    for upload_buffer_part in self._buff_split(upload_buffer):
                        yield self._get_upload_part(upload_buffer_part)
                    # split current intent (copy source) to parts and yield
                    copy_parts = self._get_copy_parts(
                        current_intent,
                        start_offset=upload_buffer.end_offset,
                        end_offset=current_end,
                    )
                    for part in copy_parts:
                        yield part
                    upload_buffer = UploadBuffer(current_end)
            else:
                if current_intent.is_copy() and first and last:
                    # this is a single intent "small copy" - we force use of `copy_file`
                    copy_parts = self._get_copy_parts(
                        current_intent,
                        start_offset=upload_buffer.end_offset,
                        end_offset=current_end,
                    )
                    for part in copy_parts:
                        yield part
                else:
                    # this is a upload source or "small copy" source (that is *not* single intent)
                    # either way we just add them to upload buffer
                    upload_buffer.append(current_intent, current_end)
                    upload_buffer_parts = list(self._buff_split(upload_buffer))
                    # we flush all parts excluding last one - we may want to extend
                    # this last part with "incoming" intent in next loop run
                    for upload_buffer_part in upload_buffer_parts[:-1]:
                        yield self._get_upload_part(upload_buffer_part)
                    upload_buffer = upload_buffer_parts[-1]
            current_intent = intent
            first = False
            current_end = fragment_end
            if current_intent is None:
                # this is a sentinel - there would be no more fragments - we have to flush upload buffer
                for upload_buffer_part in self._buff_split(upload_buffer):
                    yield self._get_upload_part(upload_buffer_part)

    def _get_upload_part(self, upload_buffer):
        """ Build emerge part from upload buffer. """
        if upload_buffer.intent_count() == 1 and upload_buffer.get_intent(0).is_upload():
            intent = upload_buffer.get_intent(0)
            relative_offset = upload_buffer.start_offset - intent.destination_offset
            length = upload_buffer.length
            definition = UploadEmergePartDefinition(intent.outbound_source, relative_offset, length)
        else:
            subparts = []
            fragment_start = upload_buffer.start_offset
            for intent, fragment_end in upload_buffer.iter_items():
                relative_offset = fragment_start - intent.destination_offset
                length = fragment_end - fragment_start
                if intent.is_upload():
                    subpart_class = LocalSourceUploadSubpart
                elif intent.is_copy():
                    subpart_class = RemoteSourceUploadSubpart
                else:
                    raise RuntimeError('This cannot happen!!!')
                subparts.append(subpart_class(intent.outbound_source, relative_offset, length))
                fragment_start = fragment_end
            definition = UploadSubpartsEmergePartDefinition(subparts)
        return EmergePart(definition)

    def _get_copy_parts(self, copy_intent, start_offset, end_offset):
        """ Split copy intent to emerge parts. """
        fragment_length = end_offset - start_offset
        part_count = int(fragment_length / self.max_part_size)
        last_part_length = fragment_length % self.max_part_size
        if last_part_length == 0:
            last_part_length = self.max_part_size
        else:
            part_count += 1

        if part_count == 1:
            part_sizes = [last_part_length]
        else:
            if last_part_length < int(fragment_length / (part_count + 1)):
                part_count += 1
            base_part_size = int(fragment_length / part_count)
            size_remainder = fragment_length % part_count
            part_sizes = [
                base_part_size + (1 if i < size_remainder else 0) for i in range(part_count)
            ]

        copy_source = copy_intent.outbound_source
        relative_offset = start_offset - copy_intent.destination_offset
        for part_size in part_sizes:
            yield EmergePart(CopyEmergePartDefinition(copy_source, relative_offset, part_size))
            relative_offset += part_size

    def _buff_split(self, upload_buffer):
        """ Split upload buffer to parts candidates - smaller upload buffers.

        :rtype iterator[b2sdk._internal.transfer.emerge.planner.planner.UploadBuffer]:
        """
        if upload_buffer.intent_count() == 0:
            return
        tail_buffer = upload_buffer
        while True:
            if tail_buffer.length < self.recommended_upload_part_size + self.min_part_size:
                # `EmergePlanner_buff_partition` can split in such way that tail part
                # can be smaller than `min_part_size` - to avoid unnecessary download of possible
                # incoming copy intent, we don't split further
                yield tail_buffer
                return
            head_buff, tail_buffer = self._buff_partition(tail_buffer)
            yield head_buff

    def _buff_partition(self, upload_buffer):
        """ Split upload buffer to two parts (smaller upload buffers).

        In result left part cannot be split more, and nothing can be assumed about right part.

        :rtype tuple(b2sdk._internal.transfer.emerge.planner.planner.UploadBuffer,
                     b2sdk._internal.transfer.emerge.planner.planner.UploadBuffer):
        """
        left_buff = UploadBuffer(upload_buffer.start_offset)
        buff_start = upload_buffer.start_offset
        for idx, (intent, fragment_end) in enumerate(upload_buffer.iter_items()):
            candidate_size = fragment_end - buff_start
            if candidate_size > self.recommended_upload_part_size:
                right_fragment_size = candidate_size - self.recommended_upload_part_size
                left_buff.append(intent, fragment_end - right_fragment_size)
                return left_buff, upload_buffer.get_slice(
                    start_idx=idx, start_offset=left_buff.end_offset
                )
            else:
                left_buff.append(intent, fragment_end)
                if candidate_size == self.recommended_upload_part_size:
                    return left_buff, upload_buffer.get_slice(start_idx=idx + 1)

        return left_buff, UploadBuffer(left_buff.end_offset)

    def _select_intent_fragments(self, write_intent_iterator):
        """ Select overlapping write intent fragments to use.

        To solve overlapping intents selection, intents can be split to smaller fragments.
        Those fragments are yielded as soon as decision can be made to use them,
        so there is possibility that one intent is yielded in multiple fragments. Those
        would be merged again by higher level iterator that produces emerge parts, but
        in principle this merging can happen here. Not merging it is a code design decision
        to make this function easier to implement and also it would allow yielding emerge parts
        a bit quicker.
        """

        # `protected_intent_length` for upload state is 0, so it would generate at most single intent fragment
        # every loop iteration, but algorithm is not assuming that - one may one day choose to
        # protect upload fragments length too - eg. to avoid situation when file is opened to
        # read just small number of bytes and then switch to another overlapping upload source
        upload_intents_state = IntentsState()
        copy_intents_state = IntentsState(protected_intent_length=self.min_part_size)

        last_sent_offset = 0
        incoming_offset = None
        while True:
            incoming_intent = next(write_intent_iterator, None)
            if incoming_intent is None:
                incoming_offset = None
            else:
                incoming_offset = incoming_intent.destination_offset

            upload_intents = list(
                upload_intents_state.state_update(last_sent_offset, incoming_offset)
            )
            copy_intents = list(copy_intents_state.state_update(last_sent_offset, incoming_offset))

            intent_fragments = self._merge_intent_fragments(
                last_sent_offset,
                upload_intents,
                copy_intents,
            )

            for intent, intent_fragment_end in intent_fragments:
                yield intent, intent_fragment_end
                last_sent_offset = intent_fragment_end

            if incoming_offset is not None and last_sent_offset < incoming_offset:
                raise ValueError(
                    'Cannot emerge file with holes. '
                    f'Found hole range: ({last_sent_offset}, {incoming_offset})'
                )

            if incoming_intent is None:
                yield None, None  # lets yield sentinel for cleaner `_get_emerge_parts` implementation
                return
            if incoming_intent.is_upload():
                upload_intents_state.add(incoming_intent)
            elif incoming_intent.is_copy():
                copy_intents_state.add(incoming_intent)
            else:
                raise RuntimeError('This should not happen at all!')

    def _merge_intent_fragments(self, start_offset, upload_intents, copy_intents):
        """ Select "competing" upload and copy fragments.

        Upload and copy fragments may overlap so we need to choose right one
        to use - copy fragments are prioritized unless this fragment is unprotected
        (we use "protection" as an abstract for "short copy" fragments - meaning upload
        fragments have higher priority than "short copy")
        """
        upload_intents = deque(upload_intents)
        copy_intents = deque(copy_intents)
        while True:
            upload_intent = copy_intent = None
            if upload_intents:
                upload_intent, upload_end, _ = upload_intents[0]
            if copy_intents:
                copy_intent, copy_end, copy_protected = copy_intents[0]

            if upload_intent is not None and copy_intent is not None:
                if not copy_protected:
                    yield_intent = upload_intent
                else:
                    yield_intent = copy_intent
                start_offset = min(upload_end, copy_end)
                yield yield_intent, start_offset
                if start_offset >= upload_end:
                    upload_intents.popleft()
                if start_offset >= copy_end:
                    copy_intents.popleft()
            elif upload_intent is not None:
                yield upload_intent, upload_end
                upload_intents.popleft()
            elif copy_intent is not None:
                yield copy_intent, copy_end
                copy_intents.popleft()
            else:
                return

    def _validatation_iterator(self, write_intents):
        """ Iterate over write intents and validate length and order. """
        last_offset = 0
        for write_intent in write_intents:
            if write_intent.length is None:
                raise ValueError('Planner cannot support write intents of unknown length')
            if write_intent.destination_offset < last_offset:
                raise ValueError('Write intent stream have to be sorted by destination offset')
            last_offset = write_intent.destination_offset
            yield write_intent


class IntentsState:
    """ Store and process state of incoming write intents to solve
    overlapping intents selection in streaming manner.

    It does not check if intents are of the same kind (upload/copy), but the intention
    was to use it to split incoming intents by kind (two intents state are required then).
    If there would be no need for differentiating incoming intents, this would
    still work - so intent kind is ignored at this level. To address "short copy"
    prioritization problem (and avoidance) - ``protected_intent_length`` param was introduced
    to prevent logic from allowing too small fragments (if it is possible)
    """

    def __init__(self, protected_intent_length=0):
        self.protected_intent_length = protected_intent_length
        self._current_intent = None
        self._next_intent = None
        self._last_sent_offset = 0
        self._incoming_offset = None
        self._current_intent_start = None
        self._current_intent_end = None
        self._next_intent_end = None

    def add(self, incoming_intent):
        """ Add incoming intent to state.

        It has to called *after* ``IntentsState.state_update`` but it is not verified.
        """
        if self._next_intent is None:
            self._set_next_intent(incoming_intent)
        elif incoming_intent.destination_end_offset > self._next_intent_end:
            # here either incoming intent starts at the same position as next intent
            # (and current intent is None in such case - it was just cleared in `state_update`
            # or it was cleared some time ago - in previous iteratios) or we are in situation
            # when current and next intent overlaps, and `last_sent_offset` is now set to
            # incoming intent `destination_offset` - in both cases we want to choose
            # intent which has larger `destination_end_offset`
            self._set_next_intent(incoming_intent)

    def state_update(self, last_sent_offset, incoming_offset):
        """ Update the state using incoming intent offset.

        It has to be called *before* ``IntentsState.add`` and even if incoming intent
        would not be added to this intents state. It would yield a state of this stream
        of intents (like copy or upload) from ``last_sent_offset`` to ``incoming_offset``.
        So here happens the first stage of solving overlapping intents selection - but
        write intent iterator can be split to multiple substreams (like copy and upload)
        so additional stage is required to cover this.
        """
        if self._current_intent is not None:
            if last_sent_offset >= self._current_intent_end:
                self._set_current_intent(None, None)

        # `effective_incoming_offset` is a safeguard after intent iterator is drained
        if incoming_offset is not None:
            effective_incoming_offset = incoming_offset
        elif self._next_intent is not None:
            effective_incoming_offset = self._next_intent_end
        elif self._current_intent is not None:
            effective_incoming_offset = self._current_intent_end
        else:
            # intent iterator is drained and this state is empty
            return

        if (
            self._current_intent is None and self._next_intent is not None and (
                self._next_intent.destination_offset != effective_incoming_offset or
                incoming_offset is None
            )
        ):
            self._set_current_intent(self._next_intent, last_sent_offset)
            self._set_next_intent(None)

        # current and next can be both not None at this point only if they overlap
        if (
            self._current_intent is not None and self._next_intent is not None and
            effective_incoming_offset > self._current_intent_end
        ):
            # incoming intent does not overlap with current intent
            # so we switch to next because we are sure that we will have to use it anyway
            # (of course other overriding (eg. "copy" over "upload") state can have
            # overlapping intent but we have no information about it here)
            # but we also need to protect current intent length
            if not self._is_current_intent_protected():
                # we were unable to protect current intent, so we can safely rotate
                self._set_current_intent(self._next_intent, last_sent_offset)
                self._set_next_intent(None)
            else:
                remaining_len = self.protected_intent_length - (
                    last_sent_offset - self._current_intent_start
                )
                if remaining_len > 0:
                    last_sent_offset += remaining_len
                    if not self._can_be_protected(last_sent_offset, self._next_intent_end):
                        last_sent_offset = self._current_intent_end
                    yield self._current_intent, last_sent_offset, True
                self._set_current_intent(self._next_intent, last_sent_offset)
                self._set_next_intent(None)

        if self._current_intent is not None:
            yield (
                self._current_intent,
                min(effective_incoming_offset, self._current_intent_end),
                self._is_current_intent_protected(),
            )

    def _set_current_intent(self, intent, start_offset):
        self._current_intent = intent
        if self._current_intent is not None:
            self._current_intent_end = self._current_intent.destination_end_offset
        else:
            self._current_intent_end = None
            assert start_offset is None
        self._current_intent_start = start_offset

    def _set_next_intent(self, intent):
        self._next_intent = intent
        if self._next_intent is not None:
            self._next_intent_end = self._next_intent.destination_end_offset
        else:
            self._next_intent_end = None

    def _is_current_intent_protected(self):
        """ States if current intent is protected.

        Intent can be split to smaller fragments, but to choose upload over "small copy"
        we need to know for fragment if it is a "small copy" or not. In result of solving
        overlapping intents selection there might be a situation when original intent was not
        a small copy, but in effect it will be used only partially and in effect it may be a "small copy".
        Algorithm attempts to avoid using smaller fragments than ``protected_intent_length`` but
        sometimes it may be impossible. So if this function returns ``False`` it means
        that used length of this intent is smaller than ``protected_intent_length`` and the algorithm
        was unable to avoid this.
        """
        return self._can_be_protected(self._current_intent_start, self._current_intent_end)

    def _can_be_protected(self, start, end):
        return end - start >= self.protected_intent_length


class BaseEmergePlan(metaclass=ABCMeta):
    def __init__(self, emerge_parts):
        self.emerge_parts = emerge_parts

    @abstractmethod
    def is_large_file(self):
        pass

    @abstractmethod
    def get_total_length(self):
        pass

    @abstractmethod
    def get_plan_id(self):
        pass

    def enumerate_emerge_parts(self):
        return enumerate(self.emerge_parts, 1)


class EmergePlan(BaseEmergePlan):
    def __init__(self, emerge_parts):
        super().__init__(list(emerge_parts))
        self._is_large_file = len(self.emerge_parts) > 1

    def is_large_file(self):
        return self._is_large_file

    def get_total_length(self):
        return sum(emerge_part.get_length() for emerge_part in self.emerge_parts)

    def get_plan_id(self):
        if all(part.is_hashable() for part in self.emerge_parts):
            return None

        json_id = json.dumps([emerge_part.get_part_id() for emerge_part in self.emerge_parts])
        return hashlib.sha1(json_id.encode()).hexdigest()


class StreamingEmergePlan(BaseEmergePlan):
    def __init__(self, emerge_parts_iterator):
        emerge_parts_iterator, self._is_large_file = self._peek_for_large_file(
            emerge_parts_iterator
        )
        super().__init__(emerge_parts_iterator)

    def is_large_file(self):
        return self._is_large_file

    def get_total_length(self):
        return None

    def get_plan_id(self):
        return None

    def _peek_for_large_file(self, emerge_parts_iterator):
        peeked, emerge_parts_iterator = iterator_peek(emerge_parts_iterator, 2)

        if not peeked:
            raise ValueError('Empty emerge parts iterator')

        return emerge_parts_iterator, len(peeked) > 1


class EmergePart:
    def __init__(self, part_definition, verification_ranges=None):
        self.part_definition = part_definition
        self.verification_ranges = verification_ranges

    def __repr__(self):
        return f'<{self.__class__.__name__} part_definition={repr(self.part_definition)}>'

    def get_length(self):
        return self.part_definition.get_length()

    def get_execution_step(self, execution_step_factory):
        return self.part_definition.get_execution_step(execution_step_factory)

    def get_part_id(self):
        return self.part_definition.get_part_id()

    def is_hashable(self):
        return self.part_definition.is_hashable()

    def get_sha1(self):
        return self.part_definition.get_sha1()
