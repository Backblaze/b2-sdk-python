import hashlib
import json

from collections import deque
from itertools import chain

from b2sdk.transfer.emerge.planner.part_definition import (
    CopyEmergePartDefinition,
    UploadEmergePartDefinition,
    UploadSubpartsEmergePartDefinition,
)
from b2sdk.transfer.emerge.planner.upload_subpart import (
    LocalSourceUploadSubpart,
    RemoteSourceUploadSubpart,
)

MEGABYTE = 1000 * 1000
GIGABYTE = 1000 * MEGABYTE


class EmergePlanner(object):
    def __init__(self, min_part_size=5 * MEGABYTE, recommended_part_size=100 * MEGABYTE, max_part_size=5 * GIGABYTE):
        self.min_part_size = min(min_part_size, recommended_part_size, max_part_size)
        self.recommended_part_size = min(recommended_part_size, max_part_size)
        self.max_part_size = max_part_size

    @classmethod
    def from_account_info(cls, account_info, min_part_size=None, recommended_part_size=None, max_part_size=None):
        # TODO: add support for getting `min_part_size` and `max_part_size` from account info
        if recommended_part_size is None:
            # TODO: change `get_minimum_part_size` to correct name
            recommended_part_size = account_info.get_minimum_part_size()
        kwargs = {
            'min_part_size': min_part_size,
            'recommended_part_size': recommended_part_size,
            'max_part_size': max_part_size,
        }
        return cls(**{key: value for key, value in kwargs.items() if value is not None})

    def get_emerge_plan(self, write_intents):
        write_intents = sorted(write_intents, key=lambda intent: intent.destination_offset)
        return self._get_emerge_plan(write_intents, EmergePlan)

    def get_streaming_emerge_plan(self, write_intent_iterator):
        return self._get_emerge_plan(write_intent_iterator, StreamingEmergePlan)

    def _get_emerge_plan(self, write_intent_iterator, plan_class):
        return plan_class(
            self._get_emerge_parts(
                self._select_intent_fragments(
                    self._validatation_iterator(write_intent_iterator)
                )
            )
        )

    def _get_emerge_parts(self, intent_fragments_iterator):
        min_part_size = self.min_part_size
        buff_start = 0
        buff_end = 0
        buff = []
        current_end = 0
        current_intent = None
        for intent, fragment_end in intent_fragments_iterator:
            if current_intent is None:
                current_intent = intent
                current_end = fragment_end
                continue

            if intent is current_intent:
                current_end = fragment_end
            else:
                current_len = current_end - buff_end
                buff_len = buff_end - buff_start
                if current_intent.is_copy() and current_len >= min_part_size:
                    if buff and buff_len < min_part_size:
                        missing_length = min_part_size - buff_len
                    else:
                        missing_length = 0
                    if missing_length > 0 and current_len - missing_length < min_part_size:
                        # current intent is *not* a "small copy", but upload buffer is small
                        # and current intent is too short to build two parts at least the minimum part size
                        # so we append current intent to upload buffer
                        buff.append((current_intent, current_end))
                    else:
                        if missing_length > 0:
                            # we "borrow" a fragment of current intent to upload buffer
                            # to fill it to minimum part size
                            buff_end += missing_length
                            buff.append((current_intent, buff_end))
                        for sub_buff_start, sub_buff in self._buff_split(buff, buff_start):
                            yield self._get_upload_part(sub_buff, sub_buff_start)
                        for part in self._get_copy_parts(current_intent, buff_end, current_end):
                            yield part
                        buff = []
                        buff_start = current_end
                        buff_end = buff_start
                else:
                    buff.append((current_intent, current_end))
                    buff_end = current_end
                    buff_parts = list(self._buff_split(buff, buff_start))
                    for sub_buff_start, sub_buff in buff_parts[:-1]:
                        yield self._get_upload_part(sub_buff, sub_buff_start)
                    buff_start, buff = buff_parts[-1]
                current_intent = intent
                current_end = fragment_end
                if current_intent is None:
                    for sub_buff_start, sub_buff in self._buff_split(buff, buff_start):
                        yield self._get_upload_part(sub_buff, sub_buff_start)

    def _get_upload_part(self, buff, buff_start):
        if len(buff) == 1 and buff[0][0].is_upload():
            intent, buff_end = buff[0]
            relative_offset = buff_start - intent.destination_offset
            length = buff_end - buff_start
            definition = UploadEmergePartDefinition(intent.outbound_source, relative_offset, length)
        else:
            subparts = []
            fragment_start = buff_start
            for intent, fragment_end in buff:
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
            size_reminder = fragment_length % part_count
            part_sizes = [base_part_size + (1 if i < size_reminder else 0) for i in range(part_count)]

        copy_source = copy_intent.outbound_source
        relative_offset = start_offset - copy_intent.destination_offset
        for part_size in part_sizes:
            yield EmergePart(CopyEmergePartDefinition(copy_source, relative_offset, part_size))
            relative_offset += part_size

    def _buff_split(self, buff, buff_start):
        if not buff:
            return
        buff_end = buff[-1][1]
        while True:
            if buff_end - buff_start < self.recommended_part_size + self.min_part_size:
                yield buff_start, buff
                return
            left_buff, split_offset, right_buff = self._buff_partition(buff, buff_start)
            yield buff_start, left_buff
            buff_start = split_offset
            buff = right_buff

    def _buff_partition(self, buff, buff_start):
        left_buff = []
        split_offset = buff_start
        for idx, (intent, fragment_end) in enumerate(buff):
            candidate_size = fragment_end - buff_start
            if candidate_size > self.recommended_part_size:
                right_fragment_size = candidate_size - self.recommended_part_size
                split_offset = fragment_end - right_fragment_size
                left_buff.append((intent, split_offset))
                return left_buff, split_offset, buff[idx:]
            else:
                left_buff.append((intent, fragment_end))
                split_offset = fragment_end
                if candidate_size == self.recommended_part_size:
                    return left_buff, split_offset, buff[idx + 1:]

        return left_buff, split_offset, []

    def _select_intent_fragments(self, write_intent_iterator):
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

            upload_intents = list(upload_intents_state.state_update(last_sent_offset, incoming_offset))
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
                raise ValueError('Cannot emerge file with holes')

            if incoming_intent is None:
                yield None, None  # lets yield sentinel for cleaner `_get_emerge_parts` implementation
                return
            else:
                if incoming_intent.is_upload():
                    upload_intents_state.add(incoming_intent)
                elif incoming_intent.is_copy():
                    copy_intents_state.add(incoming_intent)
                else:
                    raise RuntimeError('This should not happen at all!')

    def _merge_intent_fragments(self, start_offset, upload_intents, copy_intents):
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
        last_offset = 0
        for write_intent in write_intents:
            if write_intent.length is None:
                raise ValueError('Planner cannot support write intents of unknown length')
            if write_intent.destination_offset < last_offset:
                raise ValueError('Write intent stream have to be sorted by destination offset')
            last_offset = write_intent.destination_offset
            yield write_intent


class IntentsState(object):
    def __init__(self, protected_intent_length=0):
        self.protected_intent_length = protected_intent_length
        self._current_intent = None
        self._next_intent = None
        self._last_sent_offset = 0
        self._incoming_offset = None
        self._current_intent_end = None
        self._next_intent_end = None

    def add(self, incoming_intent):
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

        if (self._current_intent is None and
                self._next_intent is not None and
                (self._next_intent.destination_offset != effective_incoming_offset or
                 incoming_offset is None)):
            self._set_current_intent(self._next_intent, last_sent_offset)
            self._set_next_intent(None)

        # current and next can be both not None at this point only if they overlap
        if (self._current_intent is not None
                and self._next_intent is not None
                and effective_incoming_offset > self._current_intent_end):
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
                remaining_len = self.protected_intent_length - (last_sent_offset - self._current_intent_start)
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
            self._current_intent_start = start_offset
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
        return self._can_be_protected(self._current_intent_start, self._current_intent_end)

    def _can_be_protected(self, start, end):
        return end - start >= self.protected_intent_length


class BaseEmergePlan(object):
    def __init__(self, emerge_parts):
        self.emerge_parts = emerge_parts

    def is_large_file(self):
        raise NotImplementedError()

    def get_total_length(self):
        raise NotImplementedError()

    def get_plan_id(self):
        raise NotImplementedError()

    def enumerate_emerge_parts(self):
        return enumerate(self.emerge_parts, 1)


class EmergePlan(BaseEmergePlan):
    def __init__(self, emerge_parts):
        super(EmergePlan, self).__init__(list(emerge_parts))
        self._is_large_file = len(self.emerge_parts) > 1

    def is_large_file(self):
        return self._is_large_file

    def get_total_length(self):
        return sum(emerge_part.get_length() for emerge_part in self.emerge_parts)

    def get_plan_id(self):
        if all(part.is_hashable() for part in self.emerge_parts):
            return None

        json_id = json.dumps([emerge_part.get_part_id() for emerge_part in self.emerge_parts])
        return hashlib.sha1(json_id).hexdigest()


class StreamingEmergePlan(BaseEmergePlan):
    def __init__(self, emerge_parts_iterator):
        emerge_parts, self._is_large_file = self._peek_for_large_file(emerge_parts_iterator)
        super(StreamingEmergePlan, self).__init__(emerge_parts)

    def is_large_file(self):
        return self._is_large_file

    def get_total_length(self):
        return None

    def get_plan_id(self):
        return None

    def _peek_for_large_file(self, emerge_parts_iterator):
        first_part = next(emerge_parts_iterator, None)
        if first_part is None:
            raise ValueError('Empty emerge parts iterator')

        second_part = next(emerge_parts_iterator, None)
        if second_part is None:
            return iter([first_part]), False
        else:
            return chain([first_part, second_part], emerge_parts_iterator), True


class EmergePart(object):
    def __init__(self, part_definition, verification_ranges=None):
        self.part_definition = part_definition
        self.verification_ranges = verification_ranges

    def __repr__(self):
        return '<{classname} part_definition={part_definition}>'.format(
            classname=self.__class__.__name__,
            part_definition=repr(self.part_definition),
        )

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
