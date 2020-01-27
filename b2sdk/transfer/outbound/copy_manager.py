import logging
import six

from b2sdk.exception import AlreadyFailed

from b2sdk.file_version import FileVersionInfoFactory
from b2sdk.raw_api import MetadataDirectiveMode
from b2sdk.utils import B2TraceMetaAbstract


try:
    import concurrent.futures as futures
except ImportError:
    import futures


logger = logging.getLogger(__name__)


@six.add_metaclass(B2TraceMetaAbstract)
class CopyManager(object):
    """
    Handle complex actions around server side copy to free raw_api from that responsibility.
    """

    MAX_LARGE_FILE_SIZE = 10 * 1000 * 1000 * 1000 * 1000  # 10 TB

    def __init__(self, session, services, max_copy_workers=10):
        """
        Initialize the CopyManager using the given session.

        :param session: an instance of :class:`~b2sdk.v1.B2Session`,
                      or any custom class derived from
                      :class:`~b2sdk.v1.B2Session`
        :param services: an instace of :class:`~b2sdk.v1.Services`
        :param int max_copy_workers: a number of copy threads, default is 10
        """
        self.session = session
        self.services = services

        self.copy_executor = None
        self.max_workers = max_copy_workers

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
        if self.copy_executor is not None:
            raise Exception('thread pool already created')
        self.max_workers = max_workers

    def get_thread_pool(self):
        """
        Return the thread pool executor to use for uploads and downloads.
        """
        if self.copy_executor is None:
            self.copy_executor = futures.ThreadPoolExecutor(max_workers=self.max_workers)
        return self.copy_executor

    def copy_file(
        self,
        copy_source,
        file_name,
        content_type=None,
        file_info=None,
        destination_bucket_id=None
    ):
        # Run small copies in the same thread pool as large file copies,
        # so that they share resources during a sync.
        return self.get_thread_pool().submit(
            self._copy_small_file,
            copy_source,
            file_name,
            content_type=content_type,
            file_info=file_info,
            destination_bucket_id=destination_bucket_id,
        )

    def copy_part(
        self, large_file_id, part_copy_source, part_number, large_file_upload_state, finished_parts=None
    ):
        return self.get_thread_pool().submit(
            self._copy_part,
            large_file_id,
            part_copy_source,
            part_number,
            large_file_upload_state,
            finished_parts=finished_parts,
        )

    def _copy_part(
        self, large_file_id, part_copy_source, part_number, large_file_upload_state, finished_parts=None
    ):
        """
        Copy a file part to started large file.

        :param :param str bucket_id: a bucket ID
        :param file_id: a large file ID
        :param b2sdk.v1.CopySourcePart copy_source_part: wrapper for copy source that represnts part range
        :param b2sdk.v1.LargeFileUploadState large_file_upload_state: state object for progress reporting
                                                                      on large file upload
        :param dict,None finished_parts: dictionary of known finished parts, keys are part numbers,
                                         values are instances of :class:`~b2sdk.v1.Part`
        """

        # Check if this part was uploaded before
        if finished_parts is not None and part_number in finished_parts:
            # Report this part finished
            part = finished_parts[part_number]
            large_file_upload_state.update_part_bytes(part.content_length)

            # Return SHA1 hash
            return {'contentSha1': part.content_sha1}

        # if another part has already had an error there's no point in
        # uploading this part
        if large_file_upload_state.has_error():
            raise AlreadyFailed(large_file_upload_state.get_error_message())

        response = self.session.copy_part(
            part_copy_source.file_id,
            large_file_id,
            part_number,
            bytes_range=part_copy_source.get_bytes_range(),
        )
        large_file_upload_state.update_part_bytes(response['contentLength'])
        return response

    def _copy_small_file(
        self, copy_source, file_name, content_type=None, file_info=None, destination_bucket_id=None
    ):
        # no progress report - because there is nothing to report
        if copy_source.content_length is None and copy_source.offset is not None:
            raise NotImplementedError('copy offset of unknown length is not supported yet')

        bytes_range = copy_source.get_bytes_range()

        if content_type is None:
            if file_info is not None:
                raise ValueError('File info can be set only when content type is set')
            metadata_directive = MetadataDirectiveMode.COPY
        else:
            if file_info is None:
                raise ValueError('File info can be not set only when content type is not set')
            metadata_directive = MetadataDirectiveMode.REPLACE

        response = self.session.copy_file(
            copy_source.file_id,
            file_name,
            bytes_range=bytes_range,
            metadata_directive=metadata_directive,
            content_type=content_type,
            file_info=file_info,
            destination_bucket_id=destination_bucket_id
        )
        return FileVersionInfoFactory.from_api_response(response)
