import logging
import six

from b2sdk.exception import (
    AlreadyFailed,
    B2Error,
    MaxRetriesExceeded,
)
from b2sdk.file_version import FileVersionInfoFactory
from b2sdk.stream.progress import ReadingStreamWithProgress
from b2sdk.stream.hashing import StreamWithHash
from b2sdk.raw_api import HEX_DIGITS_AT_END
from b2sdk.utils import B2TraceMetaAbstract

from .progress_reporter import PartProgressReporter

try:
    import concurrent.futures as futures
except ImportError:
    import futures

logger = logging.getLogger(__name__)


@six.add_metaclass(B2TraceMetaAbstract)
class UploadManager(object):
    """
    Handle complex actions around uploads to free raw_api from that responsibility.
    """

    MAX_UPLOAD_ATTEMPTS = 5

    def __init__(self, session, services, max_upload_workers=10):
        """
        Initialize the CopyManager using the given session.

        :param session: an instance of :class:`~b2sdk.v1.B2Session`,
                      or any custom class derived from
                      :class:`~b2sdk.v1.B2Session`
        :param services: an instace of :class:`~b2sdk.v1.Services`
        :param int max_upload_workers: a number of upload threads, default is 10
        """
        self.session = session
        self.services = services

        self.upload_executor = None
        self.max_workers = max_upload_workers

    @property
    def account_info(self):
        return self.session.account_info

    def set_thread_pool_size(self, max_workers):
        """
        Set the size of the thread pool to use for uploads and downloads.

        Must be called before any work starts, or the thread pool will get
        the default size of 1.

        :param int max_workers: maximum allowed number of workers in a pool
        """
        if self.upload_executor is not None:
            raise Exception('thread pool already created')
        self.max_workers = max_workers

    def get_thread_pool(self):
        """
        Return the thread pool executor to use for uploads and downloads.
        """
        if self.upload_executor is None:
            self.upload_executor = futures.ThreadPoolExecutor(max_workers=self.max_workers)
        return self.upload_executor

    def upload_file(
        self, bucket_id, upload_source, file_name, content_type, file_info, progress_listener
    ):
        f = self.get_thread_pool().submit(
            self._upload_small_file,
            bucket_id,
            upload_source,
            file_name,
            content_type,
            file_info,
            progress_listener,
        )
        return f

    def upload_part(
        self,
        bucket_id,
        file_id,
        part_upload_source,
        part_number,
        large_file_upload_state,
        finished_parts=None
    ):
        f = self.get_thread_pool().submit(
            self._upload_part,
            bucket_id,
            file_id,
            part_upload_source,
            part_number,
            large_file_upload_state,
            finished_parts,
        )
        return f

    def _upload_part(
        self,
        bucket_id,
        file_id,
        part_upload_source,
        part_number,
        large_file_upload_state,
        finished_parts=None
    ):
        """
        Upload a file part to started large file.

        :param :param str bucket_id: a bucket ID
        :param file_id: a large file ID
        :param b2sdk.v1.UploadSourcePart upload_source_part: wrapper for upload source that reads only required range
        :param b2sdk.v1.LargeFileUploadState large_file_upload_state: state object for progress reporting
                                                                      on large file upload
        :param dict,None finished_parts: dictionary of known finished parts, keys are part numbers,
                                         values are instances of :class:`~b2sdk.v1.Part`
        """
        # Check if this part was uploaded before
        if finished_parts is not None and part_number in finished_parts:
            # Report this part finished
            part = finished_parts[part_number]
            large_file_upload_state.update_part_bytes(part_upload_source.get_content_length())

            # Return SHA1 hash
            return {'contentSha1': part.content_sha1}

        # Set up a progress listener
        part_progress_listener = PartProgressReporter(large_file_upload_state)

        # Retry the upload as needed
        exception_list = []
        for _ in six.moves.xrange(self.MAX_UPLOAD_ATTEMPTS):
            # if another part has already had an error there's no point in
            # uploading this part
            if large_file_upload_state.has_error():
                raise AlreadyFailed(large_file_upload_state.get_error_message())

            try:
                with part_upload_source.open() as part_stream:
                    input_stream = ReadingStreamWithProgress(part_stream, part_progress_listener)
                    hashing_stream = StreamWithHash(
                        input_stream, part_upload_source.get_content_length()
                    )
                    # it is important that `len()` works on `hashing_stream`
                    response = self.session.upload_part(
                        file_id, part_number, hashing_stream.length, HEX_DIGITS_AT_END,
                        hashing_stream
                    )
                    assert hashing_stream.hash == response['contentSha1']
                    return response

            except B2Error as e:
                if not e.should_retry_upload():
                    raise
                exception_list.append(e)
                self.account_info.clear_bucket_upload_data(bucket_id)

        large_file_upload_state.set_error(str(exception_list[-1]))
        raise MaxRetriesExceeded(self.MAX_UPLOAD_ATTEMPTS, exception_list)

    def _upload_small_file(
        self, bucket_id, upload_source, file_name, content_type, file_info, progress_listener
    ):
        content_length = upload_source.get_content_length()
        exception_info_list = []
        progress_listener.set_total_bytes(content_length)
        with progress_listener:
            for _ in six.moves.xrange(self.MAX_UPLOAD_ATTEMPTS):
                try:
                    with upload_source.open() as file:
                        input_stream = ReadingStreamWithProgress(file, progress_listener)
                        hashing_stream = StreamWithHash(input_stream, content_length)
                        # it is important that `len()` works on `hashing_stream`
                        response = self.session.upload_file(
                            bucket_id, file_name, hashing_stream.length, content_type,
                            HEX_DIGITS_AT_END, file_info, hashing_stream
                        )
                        assert hashing_stream.hash == response['contentSha1']
                        return FileVersionInfoFactory.from_api_response(response)

                except B2Error as e:
                    if not e.should_retry_upload():
                        raise
                    exception_info_list.append(e)
                    self.account_info.clear_bucket_upload_data(bucket_id)

        raise MaxRetriesExceeded(self.MAX_UPLOAD_ATTEMPTS, exception_info_list)
