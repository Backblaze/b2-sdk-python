######################################################################
#
# File: b2sdk/_internal/transfer/emerge/executor.py
#
# Copyright 2020 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import logging
import threading
from abc import ABCMeta, abstractmethod
from typing import TYPE_CHECKING

from b2sdk._internal.encryption.setting import EncryptionSetting
from b2sdk._internal.exception import MaxFileSizeExceeded
from b2sdk._internal.file_lock import NO_RETENTION_FILE_SETTING, FileRetentionSetting, LegalHold
from b2sdk._internal.http_constants import LARGE_FILE_SHA1
from b2sdk._internal.transfer.outbound.large_file_upload_state import LargeFileUploadState
from b2sdk._internal.transfer.outbound.upload_source import UploadSourceStream

AUTO_CONTENT_TYPE = 'b2/x-auto'

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from b2sdk._internal.transfer.emerge.planner.part_definition import UploadEmergePartDefinition
    from b2sdk._internal.transfer.emerge.planner.planner import StreamingEmergePlan


class EmergeExecutor:
    def __init__(self, services):
        self.services = services

    def execute_emerge_plan(
        self,
        emerge_plan,
        bucket_id,
        file_name,
        content_type,
        file_info,
        progress_listener,
        continue_large_file_id=None,
        max_queue_size=None,
        encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        custom_upload_timestamp: int | None = None,
    ):
        if emerge_plan.is_large_file():
            execution = LargeFileEmergeExecution(
                self.services,
                bucket_id,
                file_name,
                content_type,
                file_info,
                progress_listener,
                encryption=encryption,
                file_retention=file_retention,
                legal_hold=legal_hold,
                continue_large_file_id=continue_large_file_id,
                max_queue_size=max_queue_size,
                custom_upload_timestamp=custom_upload_timestamp,
            )
        else:
            if continue_large_file_id is not None:
                raise ValueError('Cannot resume emerging single part plan.')
            execution = SmallFileEmergeExecution(
                self.services,
                bucket_id,
                file_name,
                content_type,
                file_info,
                progress_listener,
                encryption=encryption,
                file_retention=file_retention,
                legal_hold=legal_hold,
                custom_upload_timestamp=custom_upload_timestamp,
            )
        return execution.execute_plan(emerge_plan)


class BaseEmergeExecution(metaclass=ABCMeta):
    DEFAULT_CONTENT_TYPE = AUTO_CONTENT_TYPE

    def __init__(
        self,
        services,
        bucket_id,
        file_name,
        content_type,
        file_info,
        progress_listener,
        encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        custom_upload_timestamp: int | None = None,
    ):
        self.services = services
        self.bucket_id = bucket_id
        self.file_name = file_name
        self.content_type = content_type
        self.file_info = file_info
        self.progress_listener = progress_listener
        self.encryption = encryption
        self.file_retention = file_retention
        self.legal_hold = legal_hold
        self.custom_upload_timestamp = custom_upload_timestamp

    @abstractmethod
    def execute_plan(self, emerge_plan):
        pass


class SmallFileEmergeExecution(BaseEmergeExecution):
    def execute_plan(self, emerge_plan):
        emerge_parts = list(emerge_plan.emerge_parts)
        assert len(emerge_parts) == 1
        emerge_part = emerge_parts[0]
        execution_step_factory = SmallFileEmergeExecutionStepFactory(self, emerge_part)
        execution_step = execution_step_factory.get_execution_step()
        future = execution_step.execute()
        return future.result()


class LargeFileEmergeExecution(BaseEmergeExecution):
    MAX_LARGE_FILE_SIZE = 10 * 1000 * 1000 * 1000 * 1000  # 10 TB

    def __init__(
        self,
        services,
        bucket_id,
        file_name,
        content_type,
        file_info,
        progress_listener,
        encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        continue_large_file_id=None,
        max_queue_size=None,
        custom_upload_timestamp: int | None = None,
    ):
        super().__init__(
            services,
            bucket_id,
            file_name,
            content_type,
            file_info,
            progress_listener,
            encryption=encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
            custom_upload_timestamp=custom_upload_timestamp,
        )
        self.continue_large_file_id = continue_large_file_id
        self.max_queue_size = max_queue_size
        self._semaphore = None
        if self.max_queue_size is not None:
            self._semaphore = threading.Semaphore(self.max_queue_size)

    def execute_plan(self, emerge_plan: StreamingEmergePlan):
        total_length = emerge_plan.get_total_length()
        encryption = self.encryption

        if total_length is not None and total_length > self.MAX_LARGE_FILE_SIZE:
            raise MaxFileSizeExceeded(total_length, self.MAX_LARGE_FILE_SIZE)

        plan_id = emerge_plan.get_plan_id()

        file_info = dict(self.file_info) if self.file_info is not None else {}
        if plan_id is not None:
            file_info['plan_id'] = plan_id

        self.progress_listener.set_total_bytes(total_length or 0)

        emerge_parts_dict = None
        if total_length is not None:
            emerge_parts_dict = dict(emerge_plan.enumerate_emerge_parts())

        unfinished_file, finished_parts = self._get_unfinished_file_and_parts(
            self.bucket_id,
            self.file_name,
            file_info,
            self.continue_large_file_id,
            encryption=encryption,
            file_retention=self.file_retention,
            legal_hold=self.legal_hold,
            emerge_parts_dict=emerge_parts_dict,
            custom_upload_timestamp=self.custom_upload_timestamp,
        )

        if unfinished_file is None:
            if self.content_type is None:
                content_type = self.DEFAULT_CONTENT_TYPE
            else:
                content_type = self.content_type
            unfinished_file = self.services.large_file.start_large_file(
                self.bucket_id,
                self.file_name,
                content_type,
                file_info,
                encryption=encryption,
                file_retention=self.file_retention,
                legal_hold=self.legal_hold,
            )
        file_id = unfinished_file.file_id

        large_file_upload_state = LargeFileUploadState(self.progress_listener)

        part_futures = []
        for part_number, emerge_part in emerge_plan.enumerate_emerge_parts():
            execution_step_factory = LargeFileEmergeExecutionStepFactory(
                self,
                emerge_part,
                part_number,
                file_id,
                large_file_upload_state,
                finished_parts=finished_parts,
                # it already knows encryption from BaseMergeExecution being passed as self
            )
            execution_step = execution_step_factory.get_execution_step()
            future = self._execute_step(execution_step)
            part_futures.append(future)

        # Collect the sha1 checksums of the parts as the uploads finish.
        # If any of them raised an exception, that same exception will
        # be raised here by result()
        part_sha1_array = [f.result()['contentSha1'] for f in part_futures]

        # Finish the large file
        response = self.services.session.finish_large_file(file_id, part_sha1_array)
        return self.services.api.file_version_factory.from_api_response(response)

    def _execute_step(self, execution_step: UploadPartExecutionStep):
        semaphore = self._semaphore
        if semaphore is None:
            return execution_step.execute()
        else:
            semaphore.acquire()
            try:
                future = execution_step.execute()
            except:
                semaphore.release()
                raise
            else:
                future.add_done_callback(lambda f: semaphore.release())
            return future

    def _get_unfinished_file_and_parts(
        self,
        bucket_id,
        file_name,
        file_info,
        continue_large_file_id,
        encryption: EncryptionSetting,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        emerge_parts_dict=None,
        custom_upload_timestamp: int | None = None,
    ):
        if 'listFiles' not in self.services.session.account_info.get_allowed()['capabilities']:
            return None, {}

        unfinished_file = None
        finished_parts = {}

        if continue_large_file_id is not None:
            unfinished_file = self.services.large_file.get_unfinished_large_file(
                bucket_id,
                continue_large_file_id,
                prefix=file_name,
            )
            if unfinished_file.file_info != file_info:
                raise ValueError(
                    'Cannot manually resume unfinished large file with different file_info'
                )
            finished_parts = {
                part.part_number: part
                for part in self.services.large_file.list_parts(continue_large_file_id)
            }
        elif 'plan_id' in file_info:
            assert emerge_parts_dict is not None
            unfinished_file, finished_parts = self._find_unfinished_file_by_plan_id(
                bucket_id,
                file_name,
                file_info,
                emerge_parts_dict,
                encryption,
                file_retention,
                legal_hold,
                custom_upload_timestamp=custom_upload_timestamp,
            )
        elif emerge_parts_dict is not None:
            unfinished_file, finished_parts = self._match_unfinished_file_if_possible(
                bucket_id,
                file_name,
                file_info,
                emerge_parts_dict,
                encryption,
                file_retention,
                legal_hold,
                custom_upload_timestamp=custom_upload_timestamp,
            )
        return unfinished_file, finished_parts

    def _find_matching_unfinished_file(
        self,
        bucket_id,
        file_name,
        file_info,
        emerge_parts_dict,
        encryption: EncryptionSetting,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        custom_upload_timestamp: int | None = None,
        check_file_info_without_large_file_sha1: bool | None = False,
        eager_mode: bool | None = False,
    ):
        """
        Search for a matching unfinished large file in the specified bucket.

        In case a matching file is found but has inconsistencies (for example, mismatching file info or encryption settings),
        mismatches are logged.

        :param bucket_id: The identifier of the bucket where the unfinished file resides.
        :param file_name: The name of the file to be matched.
        :param file_info: Information about the file to be uploaded.
        :param emerge_parts_dict: A dictionary containing the parts of the file to be emerged.
        :param encryption: The encryption settings for the file.
        :param file_retention: The retention settings for the file, if any.
        :param legal_hold: The legal hold status of the file, if any.
        :param custom_upload_timestamp: The custom timestamp for the upload, if any.
        :param check_file_info_without_large_file_sha1: A flag indicating whether the file information should be checked without the `large_file_sha1`.
        :param eager_mode: A flag indicating whether the first matching file should be returned.
        
        :return: A tuple of the best matching unfinished file and its finished parts. If no match is found, returns `None`.
        """

        file_retention = file_retention or NO_RETENTION_FILE_SETTING
        best_match_file = None
        best_match_parts = {}
        best_match_parts_len = 0

        for file_ in self.services.large_file.list_unfinished_large_files(
            bucket_id, prefix=file_name
        ):
            if file_.file_name != file_name:
                logger.debug('Rejecting %s: file name mismatch', file_.file_id)
                continue

            if file_.file_info != file_info:
                if check_file_info_without_large_file_sha1:
                    file_info_without_large_file_sha1 = self._get_file_info_without_large_file_sha1(
                        file_info
                    )
                    if file_info_without_large_file_sha1 != self._get_file_info_without_large_file_sha1(
                        file_.file_info
                    ):
                        logger.debug(
                            'Rejecting %s: file info mismatch after dropping `large_file_sha1`',
                            file_.file_id
                        )
                        continue
                else:
                    logger.debug('Rejecting %s: file info mismatch', file_.file_id)
                    continue

            if encryption is not None and encryption != file_.encryption:
                logger.debug('Rejecting %s: encryption mismatch', file_.file_id)
                continue

            if legal_hold is None:
                if LegalHold.UNSET != file_.legal_hold:
                    logger.debug('Rejecting %s: legal hold mismatch (not unset)', file_.file_id)
                    continue
            elif legal_hold != file_.legal_hold:
                logger.debug('Rejecting %s: legal hold mismatch', file_.file_id)
                continue

            if file_retention != file_.file_retention:
                logger.debug('Rejecting %s: retention mismatch', file_.file_id)
                continue

            if custom_upload_timestamp is not None and file_.upload_timestamp != custom_upload_timestamp:
                logger.debug('Rejecting %s: custom_upload_timestamp mismatch', file_.file_id)
                continue

            finished_parts = {}

            for part in self.services.large_file.list_parts(file_.file_id):

                emerge_part = emerge_parts_dict.get(part.part_number)

                if emerge_part is None:
                    # something is wrong - we have a part that we don't know about
                    # so we can't resume this upload
                    logger.debug(
                        'Rejecting %s: part %s not found in emerge parts, giving up.',
                        file_.file_id, part.part_number
                    )
                    finished_parts = None
                    break

                # Compare part sizes
                if emerge_part.get_length() != part.content_length:
                    logger.debug(
                        'Rejecting %s: part %s size mismatch', file_.file_id, part.part_number
                    )
                    continue  # part size doesn't match - so we reupload

                # Compare part hashes
                if emerge_part.is_hashable() and emerge_part.get_sha1() != part.content_sha1:
                    logger.debug(
                        'Rejecting %s: part %s sha1 mismatch', file_.file_id, part.part_number
                    )
                    continue  # part.sha1 doesn't match - so we reupload

                finished_parts[part.part_number] = part

            if finished_parts is None:
                continue

            finished_parts_len = len(finished_parts)

            if finished_parts and (
                best_match_file is None or finished_parts_len > best_match_parts_len
            ):
                best_match_file = file_
                best_match_parts = finished_parts
                best_match_parts_len = finished_parts_len

            if eager_mode and best_match_file is not None:
                break

        return best_match_file, best_match_parts

    def _find_unfinished_file_by_plan_id(
        self,
        bucket_id,
        file_name,
        file_info,
        emerge_parts_dict,
        encryption: EncryptionSetting,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        custom_upload_timestamp: int | None = None,
    ):
        """
        Search for a matching unfinished large file by plan_id in the specified bucket.

        This function aims to locate a matching unfinished large file using the plan_id and the supplied parameters. 
        It's used to resume an interrupted upload, centralizing the shared logic between `_find_unfinished_file_by_plan_id` 
        and `_match_unfinished_file_if_possible`.

        In case a matching file is found but has inconsistencies (for example, mismatching file info or encryption settings), 
        the function checks if 'plan_id' is in file_info, as this is a prerequisite.

        :param bucket_id: The identifier of the bucket where the unfinished file resides.
        :param file_name: The name of the file to be matched.
        :param file_info: Information about the file to be uploaded.
        :param emerge_parts_dict: A dictionary containing the parts of the file to be emerged.
        :param encryption: The encryption settings for the file.
        :param file_retention: The retention settings for the file, if any.
        :param legal_hold: The legal hold status of the file, if any.
        :param custom_upload_timestamp: The custom timestamp for the upload, if any.
        
        :return: A tuple of the best matching unfinished file and its finished parts. If no match is found, it returns `None`.
        """
        if 'plan_id' not in file_info:
            raise ValueError("The 'plan_id' key must be in file_info dictionary.")

        return self._find_matching_unfinished_file(
            bucket_id=bucket_id,
            file_name=file_name,
            file_info=file_info,
            emerge_parts_dict=emerge_parts_dict,
            encryption=encryption,
            file_retention=file_retention or NO_RETENTION_FILE_SETTING,
            legal_hold=legal_hold,
            custom_upload_timestamp=custom_upload_timestamp,
            check_file_info_without_large_file_sha1=False,
        )

    @classmethod
    def _get_file_info_without_large_file_sha1(
        cls,
        file_info: dict[str, str] | None,
    ) -> dict[str, str] | None:
        if not file_info or LARGE_FILE_SHA1 not in file_info:
            return file_info
        out_file_info = dict(file_info)
        del out_file_info[LARGE_FILE_SHA1]
        return out_file_info

    def _match_unfinished_file_if_possible(
        self,
        bucket_id,
        file_name,
        file_info,
        emerge_parts_dict,
        encryption: EncryptionSetting,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        custom_upload_timestamp: int | None = None,
    ):
        """
        Scan for a suitable unfinished large file in the specified bucket to resume upload.

        This function examines each unfinished large file for a possible match with the provided
        parameters. This enables resumption of an interrupted upload by reusing the unfinished file,
        provided that file's info and additional parameters match.

        Along with the filename and file info, additional parameters like encryption, file retention,
        legal hold, custom upload timestamp, and cache control are compared for a match. The
        'emerge_parts_dict' is also cross-checked for matching file parts.

        Function is eager to find a match, and will return the first match it finds. If no match is
        found, it returns `None`.

        :param bucket_id: The identifier of the bucket containing the unfinished file.
        :param file_name: The name of the file to find.
        :param file_info: Information about the file to be uploaded.
        :param emerge_parts_dict: A dictionary of the parts of the file to be emerged.
        :param encryption: The encryption settings for the file.
        :param file_retention: The retention settings for the file, if applicable.
        :param legal_hold: The legal hold status of the file, if applicable.
        :param custom_upload_timestamp: The custom timestamp for the upload, if set.
        
        :return: A tuple of the best matching unfinished file and its finished parts. If no match is found, returns `None`.
        """
        logger.debug('Checking for matching unfinished large files for %s...', file_name)

        file_, finished_parts = self._find_matching_unfinished_file(
            bucket_id,
            file_name,
            file_info,
            emerge_parts_dict,
            encryption,
            file_retention,
            legal_hold,
            custom_upload_timestamp,
            check_file_info_without_large_file_sha1=True,
            eager_mode=True,
        )

        if file_ is None:
            logger.debug('No matching unfinished files found.')
            return None, {}

        logger.debug(
            'Unfinished file %s matches with %i finished parts', file_.file_id, len(finished_parts)
        )
        return file_, finished_parts


class BaseExecutionStepFactory(metaclass=ABCMeta):
    def __init__(self, emerge_execution, emerge_part):
        self.emerge_execution = emerge_execution
        self.emerge_part = emerge_part

    @abstractmethod
    def create_copy_execution_step(self, copy_range):
        pass

    @abstractmethod
    def create_upload_execution_step(self, stream_opener, stream_length=None, stream_sha1=None):
        pass

    def get_execution_step(self):
        return self.emerge_part.get_execution_step(self)


class SmallFileEmergeExecutionStepFactory(BaseExecutionStepFactory):
    def create_copy_execution_step(self, copy_range):
        return CopyFileExecutionStep(self.emerge_execution, copy_range)

    def create_upload_execution_step(self, stream_opener, stream_length=None, stream_sha1=None):
        return UploadFileExecutionStep(
            self.emerge_execution,
            stream_opener,
            stream_length=stream_length,
            stream_sha1=stream_sha1
        )


class LargeFileEmergeExecutionStepFactory(BaseExecutionStepFactory):
    def __init__(
        self,
        emerge_execution,
        emerge_part: UploadEmergePartDefinition,
        part_number,
        large_file_id,
        large_file_upload_state,
        finished_parts=None,
    ):
        super().__init__(emerge_execution, emerge_part)
        self.part_number = part_number
        self.large_file_id = large_file_id
        self.large_file_upload_state = large_file_upload_state
        self.finished_parts = finished_parts or {}

    def create_copy_execution_step(self, copy_range):
        return CopyPartExecutionStep(
            self.emerge_execution,
            copy_range,
            self.part_number,
            self.large_file_id,
            self.large_file_upload_state,
        )

    def create_upload_execution_step(self, stream_opener, stream_length=None, stream_sha1=None):
        return UploadPartExecutionStep(
            self.emerge_execution,
            stream_opener,
            self.part_number,
            self.large_file_id,
            self.large_file_upload_state,
            stream_length=stream_length,
            stream_sha1=stream_sha1,
            finished_parts=self.finished_parts,
        )


class BaseExecutionStep(metaclass=ABCMeta):
    def __init__(self, emerge_execution: BaseEmergeExecution):
        self.emerge_execution = emerge_execution

    @abstractmethod
    def execute(self):
        pass


class CopyFileExecutionStep(BaseExecutionStep):
    def __init__(self, emerge_execution, copy_source_range):
        super().__init__(emerge_execution)
        self.copy_source_range = copy_source_range

    def execute(self):
        execution = self.emerge_execution
        # if content type is not None then we support empty dict as default file info
        # but if content type is None, then setting empty dict as file info
        # would result with an error, because default in such case is: copy from source
        if execution.content_type is not None:
            file_info = execution.file_info or {}
        else:
            file_info = None
        return execution.services.copy_manager.copy_file(
            self.copy_source_range,
            execution.file_name,
            content_type=execution.content_type,
            file_info=file_info,
            destination_bucket_id=execution.bucket_id,
            progress_listener=execution.progress_listener,
            destination_encryption=execution.encryption,
            source_encryption=self.copy_source_range.encryption,
            file_retention=execution.file_retention,
            legal_hold=execution.legal_hold,
        )


class CopyPartExecutionStep(BaseExecutionStep):
    def __init__(
        self,
        emerge_execution,
        copy_source_range,
        part_number,
        large_file_id,
        large_file_upload_state,
        finished_parts=None
    ):
        super().__init__(emerge_execution)
        self.copy_source_range = copy_source_range
        self.part_number = part_number
        self.large_file_id = large_file_id
        self.large_file_upload_state = large_file_upload_state
        self.finished_parts = finished_parts or {}

    def execute(self):
        return self.emerge_execution.services.copy_manager.copy_part(
            self.large_file_id,
            self.copy_source_range,
            self.part_number,
            self.large_file_upload_state,
            finished_parts=self.finished_parts,
            destination_encryption=self.emerge_execution.encryption,
            source_encryption=self.copy_source_range.encryption,
        )


class UploadFileExecutionStep(BaseExecutionStep):
    def __init__(self, emerge_execution, stream_opener, stream_length=None, stream_sha1=None):
        super().__init__(emerge_execution)
        self.stream_opener = stream_opener
        self.stream_length = stream_length
        self.stream_sha1 = stream_sha1

    def execute(self):
        upload_source = UploadSourceStream(
            self.stream_opener,
            stream_length=self.stream_length,
            stream_sha1=self.stream_sha1,
        )
        execution = self.emerge_execution
        return execution.services.upload_manager.upload_file(
            execution.bucket_id,
            upload_source,
            execution.file_name,
            execution.content_type or execution.DEFAULT_CONTENT_TYPE,
            execution.file_info or {},
            execution.progress_listener,
            encryption=execution.encryption,
            file_retention=execution.file_retention,
            legal_hold=execution.legal_hold,
            custom_upload_timestamp=execution.custom_upload_timestamp,
        )


class UploadPartExecutionStep(BaseExecutionStep):
    def __init__(
        self,
        emerge_execution,
        stream_opener,
        part_number,
        large_file_id,
        large_file_upload_state,
        stream_length=None,
        stream_sha1=None,
        finished_parts=None
    ):
        super().__init__(emerge_execution)
        self.stream_opener = stream_opener
        self.stream_length = stream_length
        self.stream_sha1 = stream_sha1
        self.part_number = part_number
        self.large_file_id = large_file_id
        self.large_file_upload_state = large_file_upload_state
        self.finished_parts = finished_parts or {}

    def execute(self):
        execution = self.emerge_execution
        upload_source = UploadSourceStream(
            self.stream_opener,
            stream_length=self.stream_length,
            stream_sha1=self.stream_sha1,
        )
        return execution.services.upload_manager.upload_part(
            execution.bucket_id,
            self.large_file_id,
            upload_source,
            self.part_number,
            self.large_file_upload_state,
            finished_parts=self.finished_parts,
            encryption=execution.encryption,
        )
