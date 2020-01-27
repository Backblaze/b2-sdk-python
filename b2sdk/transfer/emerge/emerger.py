import logging

import six

from b2sdk.utils import B2TraceMetaAbstract

from b2sdk.transfer.emerge.executor import EmergeExecutor
from b2sdk.transfer.emerge.planner.planner import EmergePlanner

logger = logging.getLogger(__name__)


@six.add_metaclass(B2TraceMetaAbstract)
class Emerger(object):
    """
    Handle complex actions around multi source copy/uploads.

    This class can be used to build advanced copy workflows like incremental upload.
    """

    def __init__(self, session, services):
        """
        Initialize the Emerger using the given session and transfer managers.

        :param session: an instance of :class:`~b2sdk.v1.B2Session`,
                      or any custom class derived from
                      :class:`~b2sdk.v1.B2Session`
        :param services: an instace of :class:`~b2sdk.v1.Services`
        """
        self.session = session
        self.emerge_executor = EmergeExecutor(session, services)

    def emerge(
        self, bucket_id, write_intents, file_name, content_type, file_info,
        progress_listener, planner=None, continue_large_file_id=None,
    ):
        """
        Emerge (store multiple sources) of write intents list.

        :param  str bucket_id: a bucket ID
        :param write_intents: list of :class:`~b2sdk.v1.WriteIntent`
        :param str file_name: the file name of the new B2 file
        :param str,None content_type: the MIME type or ``None`` to determine automatically
        :param dict,None file_info: a file info to store with the file or ``None`` to not store anything
        :param b2sdk.v1.AbstractProgressListener progress_listener: a progress listener object to use

        """
        planner = planner or self.get_default_emerge_planner()
        emerge_plan = planner.get_emerge_plan(write_intents)
        return self.emerge_executor.execute_emerge_plan(
            emerge_plan, bucket_id, file_name, content_type, file_info, progress_listener,
            continue_large_file_id=continue_large_file_id,
        )

    def emerge_stream(
        self, bucket_id, write_intent_iterator, file_name, content_type, file_info,
        progress_listener, planner=None, continue_large_file_id=None,
    ):
        """
        Emerge (store multiple sources) of write intents iterator.

        :param  str bucket_id: a bucket ID
        :param write_intents: iterator of :class:`~b2sdk.v1.WriteIntent`
        :param str file_name: the file name of the new B2 file
        :param str,None content_type: the MIME type or ``None`` to determine automatically
        :param dict,None file_info: a file info to store with the file or ``None`` to not store anything
        :param b2sdk.v1.AbstractProgressListener progress_listener: a progress listener object to use

        """
        planner = planner or self.get_default_emerge_planner()
        emerge_plan = planner.get_streaming_emerge_plan(write_intent_iterator)
        return self.emerge_executor.execute_emerge_plan(
            emerge_plan, bucket_id, file_name, content_type, file_info, progress_listener,
            continue_large_file_id=continue_large_file_id,
        )

    def get_default_emerge_planner(self):
        return EmergePlanner.from_account_info(self.session.account_info)
