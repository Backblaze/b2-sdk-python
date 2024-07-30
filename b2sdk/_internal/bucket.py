######################################################################
#
# File: b2sdk/_internal/bucket.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import datetime as dt
import fnmatch
import itertools
import logging
import pathlib
from contextlib import suppress
from typing import Iterable, Sequence

from .encryption.setting import EncryptionSetting, EncryptionSettingFactory
from .encryption.types import EncryptionMode
from .exception import (
    BucketIdNotFound,
    CopySourceTooBig,
    FileDeleted,
    FileNotHidden,
    FileNotPresent,
    FileOrBucketNotFound,
    UnexpectedCloudBehaviour,
    UnexpectedFileVersionAction,
    UnrecognizedBucketType,
)
from .file_lock import (
    UNKNOWN_BUCKET_RETENTION,
    BucketRetentionSetting,
    FileLockConfiguration,
    FileRetentionSetting,
    LegalHold,
)
from .file_version import DownloadVersion, FileIdAndName, FileVersion
from .filter import Filter, FilterMatcher
from .http_constants import LIST_FILE_NAMES_MAX_LIMIT
from .progress import AbstractProgressListener, DoNothingProgressListener
from .raw_api import LifecycleRule, NotificationRule, NotificationRuleResponse
from .replication.setting import ReplicationConfiguration, ReplicationConfigurationFactory
from .transfer.emerge.executor import AUTO_CONTENT_TYPE
from .transfer.emerge.unbound_write_intent import UnboundWriteIntentGenerator
from .transfer.emerge.write_intent import WriteIntent
from .transfer.inbound.downloaded_file import DownloadedFile
from .transfer.outbound.copy_source import CopySource
from .transfer.outbound.upload_source import UploadMode, UploadSourceBytes, UploadSourceLocalFile
from .utils import (
    B2TraceMeta,
    Sha1HexDigest,
    b2_url_encode,
    disable_trace,
    limit_trace_arguments,
    validate_b2_file_name,
)

logger = logging.getLogger(__name__)


class Bucket(metaclass=B2TraceMeta):
    """
    Provide access to a bucket in B2: listing files, uploading and downloading.
    """

    DEFAULT_CONTENT_TYPE = AUTO_CONTENT_TYPE

    def __init__(
        self,
        api,
        id_,
        name=None,
        type_=None,
        bucket_info=None,
        cors_rules=None,
        lifecycle_rules: list[LifecycleRule] | None = None,
        revision=None,
        bucket_dict=None,
        options_set=None,
        default_server_side_encryption: EncryptionSetting = EncryptionSetting(
            EncryptionMode.UNKNOWN
        ),
        default_retention: BucketRetentionSetting = UNKNOWN_BUCKET_RETENTION,
        is_file_lock_enabled: bool | None = None,
        replication: ReplicationConfiguration | None = None,
    ):
        """
        :param b2sdk.v2.B2Api api: an API object
        :param str id_: a bucket id
        :param str name: a bucket name
        :param str type_: a bucket type
        :param dict bucket_info: an info to store with a bucket
        :param dict cors_rules: CORS rules to store with a bucket
        :param lifecycle_rules: lifecycle rules of the bucket
        :param int revision: a bucket revision number
        :param dict bucket_dict: a dictionary which contains bucket parameters
        :param set options_set: set of bucket options strings
        :param b2sdk.v2.EncryptionSetting default_server_side_encryption: default server side encryption settings
        :param b2sdk.v2.BucketRetentionSetting default_retention: default retention setting
        :param bool is_file_lock_enabled: whether file locking is enabled or not
        :param b2sdk.v2.ReplicationConfiguration replication: replication rules for the bucket
        """
        self.api = api
        self.id_ = id_
        self.name = name
        self.type_ = type_
        self.bucket_info = bucket_info or {}
        self.cors_rules = cors_rules or []
        self.lifecycle_rules = lifecycle_rules or []
        self.revision = revision
        self.bucket_dict = bucket_dict or {}
        self.options_set = options_set or set()
        self.default_server_side_encryption = default_server_side_encryption
        self.default_retention = default_retention
        self.is_file_lock_enabled = is_file_lock_enabled
        self.replication = replication

    def _add_file_info_item(self, file_info: dict[str, str], name: str, value: str | None):
        if value is not None:
            if name in file_info and file_info[name] != value:
                logger.warning(
                    'Overwriting file info key %s with value %s (previous value %s)', name, value,
                    file_info[name]
                )
            file_info[name] = value

    def _merge_file_info_and_headers_params(
        self,
        file_info: dict | None,
        cache_control: str | None,
        expires: str | dt.datetime | None,
        content_disposition: str | None,
        content_encoding: str | None,
        content_language: str | None,
    ) -> dict | None:
        updated_file_info = {**(file_info or {})}

        if isinstance(expires, dt.datetime):
            expires = expires.astimezone(dt.timezone.utc)
            expires = dt.datetime.strftime(expires, '%a, %d %b %Y %H:%M:%S GMT')

        self._add_file_info_item(updated_file_info, 'b2-expires', expires)
        self._add_file_info_item(updated_file_info, 'b2-cache-control', cache_control)
        self._add_file_info_item(updated_file_info, 'b2-content-disposition', content_disposition)
        self._add_file_info_item(updated_file_info, 'b2-content-encoding', content_encoding)
        self._add_file_info_item(updated_file_info, 'b2-content-language', content_language)

        # If file_info was None and we didn't add anything, we want to return None
        if not updated_file_info:
            return file_info
        return updated_file_info

    def get_fresh_state(self) -> Bucket:
        """
        Fetch all the information about this bucket and return a new bucket object.
        This method does NOT change the object it is called on.
        """
        buckets_found = self.api.list_buckets(bucket_id=self.id_)
        if not buckets_found:
            raise BucketIdNotFound(self.id_)
        return buckets_found[0]

    def get_id(self) -> str:
        """
        Return bucket ID.

        :rtype: str
        """
        return self.id_

    def set_info(self, new_bucket_info, if_revision_is=None) -> Bucket:
        """
        Update bucket info.

        :param dict new_bucket_info: new bucket info dictionary
        :param int if_revision_is: revision number, update the info **only if** *revision* equals to *if_revision_is*
        """
        return self.update(bucket_info=new_bucket_info, if_revision_is=if_revision_is)

    def set_type(self, bucket_type) -> Bucket:
        """
        Update bucket type.

        :param str bucket_type: a bucket type ("allPublic" or "allPrivate")
        """
        return self.update(bucket_type=bucket_type)

    def update(
        self,
        bucket_type: str | None = None,
        bucket_info: dict | None = None,
        cors_rules: dict | None = None,
        lifecycle_rules: list[LifecycleRule] | None = None,
        if_revision_is: int | None = None,
        default_server_side_encryption: EncryptionSetting | None = None,
        default_retention: BucketRetentionSetting | None = None,
        replication: ReplicationConfiguration | None = None,
        is_file_lock_enabled: bool | None = None,
    ) -> Bucket:
        """
        Update various bucket parameters.

        :param bucket_type: a bucket type, e.g. ``allPrivate`` or ``allPublic``
        :param bucket_info: an info to store with a bucket
        :param cors_rules: CORS rules to store with a bucket
        :param lifecycle_rules: lifecycle rules to store with a bucket
        :param if_revision_is: revision number, update the info **only if** *revision* equals to *if_revision_is*
        :param default_server_side_encryption: default server side encryption settings (``None`` if unknown)
        :param default_retention: bucket default retention setting
        :param replication: replication rules for the bucket
        :param bool is_file_lock_enabled: specifies whether bucket should get File Lock-enabled
        """
        account_id = self.api.account_info.get_account_id()
        return self.api.BUCKET_FACTORY_CLASS.from_api_bucket_dict(
            self.api,
            self.api.session.update_bucket(
                account_id,
                self.id_,
                bucket_type=bucket_type,
                bucket_info=bucket_info,
                cors_rules=cors_rules,
                lifecycle_rules=lifecycle_rules,
                if_revision_is=if_revision_is,
                default_server_side_encryption=default_server_side_encryption,
                default_retention=default_retention,
                replication=replication,
                is_file_lock_enabled=is_file_lock_enabled,
            )
        )

    def cancel_large_file(self, file_id):
        """
        Cancel a large file transfer.

        :param str file_id: a file ID
        """
        return self.api.cancel_large_file(file_id)

    def download_file_by_id(
        self,
        file_id: str,
        progress_listener: AbstractProgressListener | None = None,
        range_: tuple[int, int] | None = None,
        encryption: EncryptionSetting | None = None,
    ) -> DownloadedFile:
        """
        Download a file by ID.

        .. note::
          download_file_by_id actually belongs in :py:class:`b2sdk.v2.B2Api`, not in :py:class:`b2sdk.v2.Bucket`; we just provide a convenient redirect here

        :param file_id: a file ID
        :param progress_listener: a progress listener object to use, or ``None`` to not track progress
        :param range_: two integer values, start and end offsets
        :param encryption: encryption settings (``None`` if unknown)
        """
        return self.api.download_file_by_id(
            file_id,
            progress_listener,
            range_=range_,
            encryption=encryption,
        )

    def download_file_by_name(
        self,
        file_name: str,
        progress_listener: AbstractProgressListener | None = None,
        range_: tuple[int, int] | None = None,
        encryption: EncryptionSetting | None = None,
    ) -> DownloadedFile:
        """
        Download a file by name.

        .. seealso::

            :ref:`Synchronizer <sync>`, a *high-performance* utility that synchronizes a local folder with a Bucket.

        :param file_name: a file name
        :param progress_listener: a progress listener object to use, or ``None`` to not track progress
        :param range_: two integer values, start and end offsets
        :param encryption: encryption settings (``None`` if unknown)
        """
        url = self.api.session.get_download_url_by_name(self.name, file_name)
        return self.api.services.download_manager.download_file_from_url(
            url,
            progress_listener,
            range_,
            encryption=encryption,
        )

    def get_file_info_by_id(self, file_id: str) -> FileVersion:
        """
        Gets a file version's by ID.

        :param str file_id: the id of the file who's info will be retrieved.
        :rtype: generator[b2sdk.v2.FileVersion]
        """
        return self.api.get_file_info(file_id)

    def get_file_info_by_name(self, file_name: str) -> DownloadVersion:
        """
        Gets a file's DownloadVersion by name.

        :param str file_name: the name of the file who's info will be retrieved.
        """
        try:
            return self.api.download_version_factory.from_response_headers(
                self.api.session.get_file_info_by_name(self.name, file_name)
            )
        except FileOrBucketNotFound:
            raise FileNotPresent(bucket_name=self.name, file_id_or_name=file_name)

    def get_download_authorization(self, file_name_prefix, valid_duration_in_seconds):
        """
        Return an authorization token that is valid only for downloading
        files from the given bucket.

        :param str file_name_prefix: a file name prefix, only files that match it could be downloaded
        :param int valid_duration_in_seconds: a token is valid only during this amount of seconds
        """
        response = self.api.session.get_download_authorization(
            self.id_, file_name_prefix, valid_duration_in_seconds
        )
        return response['authorizationToken']

    def list_parts(self, file_id, start_part_number=None, batch_size=None):
        """
        Get a list of all parts that have been uploaded for a given file.

        :param str file_id: a file ID
        :param int start_part_number: the first part number to return.  defaults to the first part.
        :param int batch_size: the number of parts to fetch at a time from the server
        """
        return self.api.list_parts(file_id, start_part_number, batch_size)

    def list_file_versions(
        self, file_name: str, fetch_count: int | None = LIST_FILE_NAMES_MAX_LIMIT
    ) -> Iterable[FileVersion]:
        """
        Lists all of the versions for a single file.

        :param file_name: the name of the file to list.
        :param fetch_count: how many entries to list per API call or ``None`` to use the default. Acceptable values: 1 - 10000
        :rtype: generator[b2sdk.v2.FileVersion]
        """
        if fetch_count is not None and fetch_count <= 0:
            # fetch_count equal to 0 means "use API default", which we don't want to support here
            raise ValueError("unsupported fetch_count value")
        start_file_name = file_name
        start_file_id = None
        session = self.api.session
        while 1:
            response = session.list_file_versions(
                self.id_, start_file_name, start_file_id, fetch_count, file_name
            )

            for entry in response['files']:
                file_version = self.api.file_version_factory.from_api_response(entry)
                if file_version.file_name != file_name:
                    # All versions for the requested file name have been listed.
                    return
                yield file_version
            start_file_name = response['nextFileName']
            start_file_id = response['nextFileId']
            if start_file_name is None:
                return

    def ls(
        self,
        path: str = '',
        latest_only: bool = True,
        recursive: bool = False,
        fetch_count: int | None = LIST_FILE_NAMES_MAX_LIMIT,
        with_wildcard: bool = False,
        filters: Sequence[Filter] = (),
    ) -> Iterable[tuple[FileVersion, str]]:
        """
        Pretend that folders exist and yields the information about the files in a folder.

        B2 has a flat namespace for the files in a bucket, but there is a convention
        of using "/" as if there were folders.  This method searches through the
        flat namespace to find the files and "folders" that live within a given
        folder.

        When the `recursive` flag is set, lists all of the files in the given
        folder, and all of its sub-folders.

        :param path: Path to list.
                     To reduce the number of API calls, if path points to a folder, it should end with "/".
                     Must not start with "/".
                     Empty string means top-level folder.
        :param latest_only: when ``False`` returns info about all versions of a file,
                            when ``True``, just returns info about the most recent versions
        :param recursive: if ``True``, list folders recursively
        :param fetch_count: how many entries to list per API call or ``None`` to use the default. Acceptable values: 1 - 10000
        :param with_wildcard: Accepts "*", "?", "[]" and "[!]" in folder_to_list, similarly to what shell does.
                              As of 1.19.0 it can only be enabled when recursive is also enabled.
                              Also, in this mode, folder_to_list is considered to be a filename or a pattern.
        :param filters: list of filters to apply to the files returned by the server.
        :rtype: generator[tuple[b2sdk.v2.FileVersion, str]]
        :returns: generator of (file_version, folder_name) tuples

        .. note::
            In case of `recursive=True`, folder_name is not returned.
        """
        # Ensure that recursive is enabled when with_wildcard is enabled.
        if with_wildcard and not recursive:
            raise ValueError('with_wildcard requires recursive to be turned on as well')

        # check if path points to an object instead of a folder
        if path and not with_wildcard and not path.endswith('/'):
            file_versions = self.list_file_versions(path, 1 if latest_only else fetch_count)
            if latest_only:
                file_versions = itertools.islice(file_versions, 1)
            path_pointed_to_file = False
            for file_version in file_versions:
                path_pointed_to_file = True
                if not latest_only or file_version.action == 'upload':
                    yield file_version, None
            if path_pointed_to_file:
                return

        folder_to_list = path
        # Every file returned must have a name that starts with the
        # folder name and a "/".
        prefix = folder_to_list
        # In case of wildcards, we don't assume that this is folder that we're searching through.
        # It could be an exact file, e.g. 'a/b.txt' that we're trying to locate.
        if prefix != '' and not prefix.endswith('/') and not with_wildcard:
            prefix += '/'

        # If we're running with wildcard-matching, we could get
        # a different prefix from it.  We search for the first
        # occurrence of the special characters and fetch
        # parent path from that place.
        # Examples:
        #   'b/c/*.txt' –> 'b/c/'
        #   '*.txt' –> ''
        #   'a/*/result.[ct]sv' –> 'a/'
        if with_wildcard:
            for wildcard_character in '*?[':
                try:
                    starter_index = folder_to_list.index(wildcard_character)
                except ValueError:
                    continue

                # +1 to include the starter character.  Using posix path to
                # ensure consistent behaviour on Windows (e.g. case sensitivity).
                path = pathlib.PurePosixPath(folder_to_list[:starter_index + 1])
                parent_path = str(path.parent)
                # Path considers dot to be the empty path.
                # There's no shorter path than that.
                if parent_path == '.':
                    prefix = ''
                    break
                # We could receive paths in different stage, e.g. 'a/*/result.[ct]sv' has two
                # possible parent paths: 'a/' and 'a/*/', with the first one being the correct one
                if len(parent_path) < len(prefix):
                    prefix = parent_path

        # Loop until all files in the named directory have been listed.
        # The starting point of the first list_file_names request is the
        # prefix we're looking for.  The prefix ends with '/', which is
        # now allowed for file names, so no file name will match exactly,
        # but the first one after that point is the first file in that
        # "folder".   If the first search doesn't produce enough results,
        # then we keep calling list_file_names until we get all of the
        # names in this "folder".
        filter_matcher = FilterMatcher(filters)
        current_dir = None
        start_file_name = prefix
        start_file_id = None
        session = self.api.session
        while True:
            if latest_only:
                response = session.list_file_names(self.id_, start_file_name, fetch_count, prefix)
            else:
                response = session.list_file_versions(
                    self.id_, start_file_name, start_file_id, fetch_count, prefix
                )
            for entry in response['files']:
                file_version = self.api.file_version_factory.from_api_response(entry)
                if not file_version.file_name.startswith(prefix):
                    # We're past the files we care about
                    return
                if with_wildcard and not fnmatch.fnmatchcase(
                    file_version.file_name, folder_to_list
                ):
                    # File doesn't match our wildcard rules
                    continue

                if not filter_matcher.match(file_version.file_name):
                    continue

                after_prefix = file_version.file_name[len(prefix):]
                # In case of wildcards, we don't care about folders at all, and it's recursive by default.
                if '/' not in after_prefix or recursive:
                    # This is not a folder, so we'll print it out and
                    # continue on.
                    yield file_version, None
                    current_dir = None
                else:
                    # This is a folder.  If it's different than the folder
                    # we're already in, then we can print it.  This check
                    # is needed, because all of the files in the folder
                    # will be in the list.
                    folder_with_slash = after_prefix.split('/')[0] + '/'
                    if folder_with_slash != current_dir:
                        folder_name = prefix + folder_with_slash
                        yield file_version, folder_name
                        current_dir = folder_with_slash
            if response['nextFileName'] is None:
                # The response says there are no more files in the bucket,
                # so we can stop.
                return

            # Now we need to set up the next search.  The response from
            # B2 has the starting point to continue with the next file,
            # but if we're in the middle of a "folder", we can skip ahead
            # to the end of the folder.  The character after '/' is '0',
            # so we'll replace the '/' with a '0' and start there.
            #
            # When recursive is True, current_dir is always None.
            if current_dir is None:
                start_file_name = response.get('nextFileName')
                start_file_id = response.get('nextFileId')
            else:
                start_file_name = max(
                    response['nextFileName'],
                    prefix + current_dir[:-1] + '0',
                )

    def list_unfinished_large_files(self, start_file_id=None, batch_size=None, prefix=None):
        """
        A generator that yields an :py:class:`b2sdk.v2.UnfinishedLargeFile` for each
        unfinished large file in the bucket, starting at the given file, filtering by prefix.

        :param str,None start_file_id: a file ID to start from or None to start from the beginning
        :param int,None batch_size: max file count
        :param str,None prefix: file name prefix filter
        :rtype: generator[b2sdk.v2.UnfinishedLargeFile]
        """
        return self.api.services.large_file.list_unfinished_large_files(
            self.id_,
            start_file_id=start_file_id,
            batch_size=batch_size,
            prefix=prefix,
        )

    @limit_trace_arguments(skip=('data_bytes',))
    def upload_bytes(
        self,
        data_bytes,
        file_name,
        content_type: str | None = None,
        file_info: dict | None = None,
        progress_listener=None,
        encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        large_file_sha1: Sha1HexDigest | None = None,
        custom_upload_timestamp: int | None = None,
        cache_control: str | None = None,
        expires: str | dt.datetime | None = None,
        content_disposition: str | None = None,
        content_encoding: str | None = None,
        content_language: str | None = None,
    ) -> FileVersion:
        """
        Upload bytes in memory to a B2 file.

        .. note:
            ``custom_upload_timestamp`` is disabled by default - please talk to customer support to enable it on your account (if you really need it)

        :param bytes data_bytes: a byte array to upload
        :param str file_name: a file name to upload bytes to
        :param str,None content_type: the MIME type, or ``None`` to accept the default based on file extension of the B2 file name
        :param dict,None file_info: a file info to store with the file or ``None`` to not store anything
        :param b2sdk.v2.AbstractProgressListener,None progress_listener: a progress listener object to use, or ``None`` to not track progress
        :param b2sdk.v2.EncryptionSetting encryption: encryption settings (``None`` if unknown)
        :param b2sdk.v2.FileRetentionSetting file_retention: file retention setting
        :param bool legal_hold: legal hold setting
        :param Sha1HexDigest,None large_file_sha1: SHA-1 hash of the result file or ``None`` if unknown
        :param int,None custom_upload_timestamp: override object creation date, expressed as a number of milliseconds since epoch
        :param str,None cache_control: an optional cache control setting. Syntax based on the section 14.9 of RFC 2616.
            Example string value: 'public, max-age=86400, s-maxage=3600, no-transform'.
        :param str,datetime.datetime,None expires: an optional cache expiration setting.
            If this argument is a string, its syntax must be based on the section 14.21 of RFC 2616.
            Example string value: 'Thu, 01 Dec 2050 16:00:00 GMT'. If this argument is a datetime,
            it will be converted to a string in the same format.
        :param str,None content_disposition: an optional content disposition setting. Syntax based on the section 19.5.1 of RFC 2616.
            Example string value: 'attachment; filename="fname.ext"'.
        :param str,None content_encoding: an optional content encoding setting.Syntax based on the section 14.11 of RFC 2616.
            Example string value: 'gzip'.
        :param str,None content_language: an optional content language setting. Syntax based on the section 14.12 of RFC 2616.
            Example string value: 'mi, en_US'.
        :rtype: b2sdk.v2.FileVersion
        """
        upload_source = UploadSourceBytes(data_bytes)
        return self.upload(
            upload_source,
            file_name,
            content_type=content_type,
            file_info=file_info,
            progress_listener=progress_listener,
            encryption=encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
            large_file_sha1=large_file_sha1,
            custom_upload_timestamp=custom_upload_timestamp,
            cache_control=cache_control,
            expires=expires,
            content_disposition=content_disposition,
            content_encoding=content_encoding,
            content_language=content_language,
        )

    def upload_local_file(
        self,
        local_file,
        file_name,
        content_type: str | None = None,
        file_info: dict | None = None,
        sha1_sum: str | None = None,
        min_part_size: int | None = None,
        progress_listener=None,
        encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        upload_mode: UploadMode = UploadMode.FULL,
        custom_upload_timestamp: int | None = None,
        cache_control: str | None = None,
        expires: str | dt.datetime | None = None,
        content_disposition: str | None = None,
        content_encoding: str | None = None,
        content_language: str | None = None,
    ):
        """
        Upload a file on local disk to a B2 file.

        .. note:
            ``custom_upload_timestamp`` is disabled by default - please talk to customer support to enable it on your account (if you really need it)

        .. seealso::

            :ref:`Synchronizer <sync>`, a *high-performance* utility that synchronizes a local folder with a :term:`bucket`.

        :param str local_file: a path to a file on local disk
        :param str file_name: a file name of the new B2 file
        :param content_type: the MIME type, or ``None`` to accept the default based on file extension of the B2 file name
        :param file_info: a file info to store with the file or ``None`` to not store anything
        :param sha1_sum: file SHA1 hash or ``None`` to compute it automatically
        :param min_part_size: lower limit of part size for the transfer planner, in bytes
        :param b2sdk.v2.AbstractProgressListener,None progress_listener: a progress listener object to use, or ``None`` to not report progress
        :param b2sdk.v2.EncryptionSetting encryption: encryption settings (``None`` if unknown)
        :param b2sdk.v2.FileRetentionSetting file_retention: file retention setting
        :param bool legal_hold: legal hold setting
        :param b2sdk.v2.UploadMode upload_mode: desired upload mode
        :param int,None custom_upload_timestamp: override object creation date, expressed as a number of milliseconds since epoch
        :param str,None cache_control: an optional cache control setting. Syntax based on the section 14.9 of RFC 2616.
            Example string value: 'public, max-age=86400, s-maxage=3600, no-transform'.
        :param str,datetime.datetime,None expires: an optional cache expiration setting.
            If this argument is a string, its syntax must be based on the section 14.21 of RFC 2616.
            Example string value: 'Thu, 01 Dec 2050 16:00:00 GMT'. If this argument is a datetime,
            it will be converted to a string in the same format.
        :param str,None content_disposition: an optional content disposition setting. Syntax based on the section 19.5.1 of RFC 2616.
            Example string value: 'attachment; filename="fname.ext"'.
        :param str,None content_encoding: an optional content encoding setting.Syntax based on the section 14.11 of RFC 2616.
            Example string value: 'gzip'.
        :param str,None content_language: an optional content language setting. Syntax based on the section 14.12 of RFC 2616.
            Example string value: 'mi, en_US'.
        :rtype: b2sdk.v2.FileVersion
        """
        upload_source = UploadSourceLocalFile(local_path=local_file, content_sha1=sha1_sum)
        sources = [upload_source]
        large_file_sha1 = sha1_sum

        if upload_mode == UploadMode.INCREMENTAL:
            with suppress(FileNotPresent):
                existing_file_info = self.get_file_info_by_name(file_name)

                sources = upload_source.get_incremental_sources(
                    existing_file_info,
                    self.api.session.account_info.get_absolute_minimum_part_size()
                )

                if len(sources) > 1 and not large_file_sha1:
                    # the upload will be incremental, but the SHA1 sum is unknown, calculate it now
                    large_file_sha1 = upload_source.get_content_sha1()

        file_info = self._merge_file_info_and_headers_params(
            file_info=file_info,
            cache_control=cache_control,
            expires=expires,
            content_disposition=content_disposition,
            content_encoding=content_encoding,
            content_language=content_language,
        )
        return self.concatenate(
            sources,
            file_name,
            content_type=content_type,
            file_info=file_info,
            min_part_size=min_part_size,
            progress_listener=progress_listener,
            encryption=encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
            large_file_sha1=large_file_sha1,
            custom_upload_timestamp=custom_upload_timestamp,
        )

    def upload_unbound_stream(
        self,
        read_only_object,
        file_name: str,
        content_type: str = None,
        file_info: dict[str, str] | None = None,
        progress_listener: AbstractProgressListener | None = None,
        recommended_upload_part_size: int | None = None,
        encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        min_part_size: int | None = None,
        max_part_size: int | None = None,
        large_file_sha1: Sha1HexDigest | None = None,
        buffers_count: int = 2,
        buffer_size: int | None = None,
        read_size: int = 8192,
        unused_buffer_timeout_seconds: float = 3600.0,
        custom_upload_timestamp: int | None = None,
        cache_control: str | None = None,
        expires: str | dt.datetime | None = None,
        content_disposition: str | None = None,
        content_encoding: str | None = None,
        content_language: str | None = None,
    ):
        """
        Upload an unbound file-like read-only object to a B2 file.

        It is assumed that this object is streamed like stdin or socket, and the size is not known up front.
        It is up to caller to ensure that this object is open and available through the whole streaming process.

        If stdin is to be passed, consider opening it in binary mode, if possible on the platform:

        .. code-block:: python

            with open(sys.stdin.fileno(), mode='rb', buffering=min_part_size, closefd=False) as source:
                bucket.upload_unbound_stream(source, 'target-file')

        For platforms without file descriptors, one can use the following:

        .. code-block:: python

            bucket.upload_unbound_stream(sys.stdin.buffer, 'target-file')

        but note that buffering in this case depends on the interpreter mode.

        ``min_part_size``, ``recommended_upload_part_size`` and ``max_part_size`` should
        all be greater than ``account_info.get_absolute_minimum_part_size()``.

        ``buffers_count`` describes a desired number of buffers that are to be used.
        Minimal amount is 2.
        to determine the method of uploading this stream (if there's only a single buffer we send it as a normal file,
        if there are at least two – as a large file).
        Number of buffers determines the amount of memory used by the streaming process and
        the amount of data that can be pulled from ``read_only_object`` while also uploading it.
        Providing more buffers allows for higher upload parallelization.
        While only one buffer can be filled with data at once,
        all others are used to send the data in parallel (limited only by the number of parallel threads).

        Buffer size can be controlled by ``buffer_size`` parameter.
        If left unset, it will default to a value of ``recommended_upload_part_size``.
        Note that in the current implementation buffers are (almost) directly sent to B2, thus whatever is picked
        as the ``buffer_size`` will also become the size of the part when uploading a large file in this manner.
        In rare cases, namely when the whole buffer was sent, but there was an error during sending of last bytes
        and a retry was issued, additional buffer (above the aforementioned limit) will be temporarily allocated.

        .. note:
            ``custom_upload_timestamp`` is disabled by default - please talk to customer support to enable it on your account (if you really need it)

        :param read_only_object: any object containing a ``read`` method accepting size of the read
        :param file_name: a file name of the new B2 file
        :param content_type: the MIME type, or ``None`` to accept the default based on file extension of the B2 file name
        :param file_info: a file info to store with the file or ``None`` to not store anything
        :param progress_listener: a progress listener object to use, or ``None`` to not report progress
        :param encryption: encryption settings (``None`` if unknown)
        :param file_retention: file retention setting
        :param legal_hold: legal hold setting
        :param recommended_upload_part_size: the recommended part size to use for uploading local sources
                        or ``None`` to determine automatically
        :param min_part_size: lower limit of part size for the transfer planner, in bytes
        :param max_part_size: upper limit of part size for the transfer planner, in bytes
        :param large_file_sha1: SHA-1 hash of the result file or ``None`` if unknown
        :param buffers_count: desired number of buffers allocated, cannot be smaller than 2
        :param buffer_size: size of a single buffer that we pull data to or upload data to B2. If ``None``,
                        value of ``recommended_upload_part_size`` is used. If that also is ``None``,
                        it will be determined automatically as "recommended upload size".
        :param read_size: size of a single read operation performed on the ``read_only_object``
        :param unused_buffer_timeout_seconds: amount of time that a buffer can be idle before returning error
        :param int,None custom_upload_timestamp: override object creation date, expressed as a number of milliseconds since epoch
        :param str,None cache_control: an optional cache control setting. Syntax based on the section 14.9 of RFC 2616.
            Example string value: 'public, max-age=86400, s-maxage=3600, no-transform'.
        :param str,datetime.datetime,None expires: an optional cache expiration setting.
            If this argument is a string, its syntax must be based on the section 14.21 of RFC 2616.
            Example string value: 'Thu, 01 Dec 2050 16:00:00 GMT'. If this argument is a datetime,
            it will be converted to a string in the same format.
        :param str,None content_disposition: an optional content disposition setting. Syntax based on the section 19.5.1 of RFC 2616.
            Example string value: 'attachment; filename="fname.ext"'.
        :param str,None content_encoding: an optional content encoding setting.Syntax based on the section 14.11 of RFC 2616.
            Example string value: 'gzip'.
        :param str,None content_language: an optional content language setting. Syntax based on the section 14.12 of RFC 2616.
            Example string value: 'mi, en_US'.
        :rtype: b2sdk.v2.FileVersion
        """
        if buffers_count <= 1:
            raise ValueError('buffers_count has to be at least 2')
        if read_size <= 0:
            raise ValueError('read_size has to be a positive integer')
        if unused_buffer_timeout_seconds <= 0.0:
            raise ValueError('unused_buffer_timeout_seconds has to be a positive float')

        buffer_size = buffer_size or recommended_upload_part_size
        if buffer_size is None:
            planner = self.api.services.emerger.get_emerge_planner()
            buffer_size = planner.recommended_upload_part_size

        file_info = self._merge_file_info_and_headers_params(
            file_info=file_info,
            cache_control=cache_control,
            expires=expires,
            content_disposition=content_disposition,
            content_encoding=content_encoding,
            content_language=content_language,
        )
        return self._create_file(
            self.api.services.emerger.emerge_unbound,
            UnboundWriteIntentGenerator(
                read_only_object,
                buffer_size,
                read_size=read_size,
                queue_size=buffers_count,
                queue_timeout_seconds=unused_buffer_timeout_seconds,
            ).iterator(),
            file_name,
            content_type=content_type,
            file_info=file_info,
            progress_listener=progress_listener,
            encryption=encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
            min_part_size=min_part_size,
            recommended_upload_part_size=recommended_upload_part_size,
            max_part_size=max_part_size,
            # This is a parameter for EmergeExecutor.execute_emerge_plan telling
            # how many buffers in parallel can be handled at once. We ensure that one buffer
            # is always downloading data from the stream while others are being uploaded.
            max_queue_size=buffers_count - 1,
            large_file_sha1=large_file_sha1,
            custom_upload_timestamp=custom_upload_timestamp,
        )

    def upload(
        self,
        upload_source,
        file_name,
        content_type: str | None = None,
        file_info=None,
        min_part_size: int | None = None,
        progress_listener=None,
        encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        large_file_sha1: Sha1HexDigest | None = None,
        custom_upload_timestamp: int | None = None,
        cache_control: str | None = None,
        expires: str | dt.datetime | None = None,
        content_disposition: str | None = None,
        content_encoding: str | None = None,
        content_language: str | None = None,
    ):
        """
        Upload a file to B2, retrying as needed.

        The source of the upload is an UploadSource object that can be used to
        open (and re-open) the file.  The result of opening should be a binary
        file whose read() method returns bytes.

        The function `opener` should return a file-like object, and it
        must be possible to call it more than once in case the upload
        is retried.

        .. note:
            ``custom_upload_timestamp`` is disabled by default - please talk to customer support to enable it on your account (if you really need it)

        :param b2sdk.v2.AbstractUploadSource upload_source: an object that opens the source of the upload
        :param str file_name: the file name of the new B2 file
        :param str,None content_type: the MIME type, or ``None`` to accept the default based on file extension of the B2 file name
        :param dict,None file_info: a file info to store with the file or ``None`` to not store anything
        :param min_part_size: lower limit of part size for the transfer planner, in bytes
        :param b2sdk.v2.AbstractProgressListener,None progress_listener: a progress listener object to use, or ``None`` to not report progress
        :param b2sdk.v2.EncryptionSetting encryption: encryption settings (``None`` if unknown)
        :param b2sdk.v2.FileRetentionSetting file_retention: file retention setting
        :param bool legal_hold: legal hold setting
        :param Sha1HexDigest,None large_file_sha1: SHA-1 hash of the result file or ``None`` if unknown
        :param int,None custom_upload_timestamp: override object creation date, expressed as a number of milliseconds since epoch
        :param str,None cache_control: an optional cache control setting. Syntax based on the section 14.9 of RFC 2616.
            Example string value: 'public, max-age=86400, s-maxage=3600, no-transform'.
        :param str,datetime.datetime,None expires: an optional cache expiration setting.
            If this argument is a string, its syntax must be based on the section 14.21 of RFC 2616.
            Example string value: 'Thu, 01 Dec 2050 16:00:00 GMT'. If this argument is a datetime,
            it will be converted to a string in the same format.
        :param str,None content_disposition: an optional content disposition setting. Syntax based on the section 19.5.1 of RFC 2616.
            Example string value: 'attachment; filename="fname.ext"'.
        :param str,None content_encoding: an optional content encoding setting.Syntax based on the section 14.11 of RFC 2616.
            Example string value: 'gzip'.
        :param str,None content_language: an optional content language setting. Syntax based on the section 14.12 of RFC 2616.
            Example string value: 'mi, en_US'.
        :rtype: b2sdk.v2.FileVersion
        """
        return self.create_file(
            [WriteIntent(upload_source)],
            file_name,
            content_type=content_type,
            file_info=file_info,
            progress_listener=progress_listener,
            # FIXME: Bucket.upload documents wrong logic
            recommended_upload_part_size=min_part_size,
            encryption=encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
            large_file_sha1=large_file_sha1,
            custom_upload_timestamp=custom_upload_timestamp,
            cache_control=cache_control,
            expires=expires,
            content_disposition=content_disposition,
            content_encoding=content_encoding,
            content_language=content_language,
        )

    def create_file(
        self,
        write_intents,
        file_name,
        content_type: str | None = None,
        file_info=None,
        progress_listener=None,
        recommended_upload_part_size=None,
        continue_large_file_id=None,
        encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        min_part_size: int | None = None,
        max_part_size: int | None = None,
        large_file_sha1=None,
        custom_upload_timestamp: int | None = None,
        cache_control: str | None = None,
        expires: str | dt.datetime | None = None,
        content_disposition: str | None = None,
        content_encoding: str | None = None,
        content_language: str | None = None,
    ):
        """
        Creates a new file in this bucket using an iterable (list, tuple etc) of remote or local sources.

        Source ranges can overlap and remote sources will be prioritized over local sources (when possible).
        For more information and usage examples please see :ref:`Advanced usage patterns <AdvancedUsagePatterns>`.

        .. note:
            ``custom_upload_timestamp`` is disabled by default - please talk to customer support to enable it on your account (if you really need it)

        :param list[b2sdk.v2.WriteIntent] write_intents: list of write intents (remote or local sources)
        :param str file_name: file name of the new file
        :param str,None content_type: content_type for the new file, if ``None`` content_type would be
                        automatically determined or it may be copied if it resolves
                        as single part remote source copy
        :param dict,None file_info: file_info for the new file, if ``None`` it will be set to empty dict
                        or it may be copied if it resolves as single part remote source copy
        :param b2sdk.v2.AbstractProgressListener,None progress_listener: a progress listener object to use,
                        or ``None`` to not report progress
        :param int,None recommended_upload_part_size: the recommended part size to use for uploading local sources
                        or ``None`` to determine automatically, but remote sources would be copied with
                        maximum possible part size
        :param str,None continue_large_file_id: large file id that should be selected to resume file creation
                        for multipart upload/copy, ``None`` for automatic search for this id
        :param b2sdk.v2.EncryptionSetting encryption: encryption settings (``None`` if unknown)
        :param b2sdk.v2.FileRetentionSetting file_retention: file retention setting
        :param bool legal_hold: legal hold setting
        :param min_part_size: lower limit of part size for the transfer planner, in bytes
        :param max_part_size: upper limit of part size for the transfer planner, in bytes
        :param Sha1HexDigest,None large_file_sha1: SHA-1 hash of the result file or ``None`` if unknown
        :param int,None custom_upload_timestamp: override object creation date, expressed as a number of milliseconds since epoch
        :param str,None cache_control: an optional cache control setting. Syntax based on the section 14.9 of RFC 2616.
            Example string value: 'public, max-age=86400, s-maxage=3600, no-transform'.
        :param str,datetime.datetime,None expires: an optional cache expiration setting.
            If this argument is a string, its syntax must be based on the section 14.21 of RFC 2616.
            Example string value: 'Thu, 01 Dec 2050 16:00:00 GMT'. If this argument is a datetime,
            it will be converted to a string in the same format.
        :param str,None content_disposition: an optional content disposition setting. Syntax based on the section 19.5.1 of RFC 2616.
            Example string value: 'attachment; filename="fname.ext"'.
        :param str,None content_encoding: an optional content encoding setting.Syntax based on the section 14.11 of RFC 2616.
            Example string value: 'gzip'.
        :param str,None content_language: an optional content language setting. Syntax based on the section 14.12 of RFC 2616.
            Example string value: 'mi, en_US'.
        """
        return self._create_file(
            self.api.services.emerger.emerge,
            write_intents,
            file_name,
            content_type=content_type,
            file_info=file_info,
            progress_listener=progress_listener,
            continue_large_file_id=continue_large_file_id,
            recommended_upload_part_size=recommended_upload_part_size,
            encryption=encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
            min_part_size=min_part_size,
            max_part_size=max_part_size,
            large_file_sha1=large_file_sha1,
            custom_upload_timestamp=custom_upload_timestamp,
            cache_control=cache_control,
            expires=expires,
            content_disposition=content_disposition,
            content_encoding=content_encoding,
            content_language=content_language,
        )

    def create_file_stream(
        self,
        write_intents_iterator,
        file_name,
        content_type=None,
        file_info=None,
        progress_listener=None,
        recommended_upload_part_size=None,
        continue_large_file_id=None,
        encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        min_part_size: int | None = None,
        max_part_size: int | None = None,
        large_file_sha1=None,
        custom_upload_timestamp: int | None = None,
        cache_control: str | None = None,
        expires: str | dt.datetime | None = None,
        content_disposition: str | None = None,
        content_encoding: str | None = None,
        content_language: str | None = None,
    ):
        """
        Creates a new file in this bucket using a stream of multiple remote or local sources.

        Source ranges can overlap and remote sources will be prioritized over local sources (when possible).
        For more information and usage examples please see :ref:`Advanced usage patterns <AdvancedUsagePatterns>`.

        .. note:
            ``custom_upload_timestamp`` is disabled by default - please talk to customer support to enable it on your account (if you really need it)

        :param iterator[b2sdk.v2.WriteIntent] write_intents_iterator: iterator of write intents which
                        are sorted ascending by ``destination_offset``
        :param str file_name: file name of the new file
        :param str,None content_type: content_type for the new file, if ``None`` content_type would be
                        automatically determined or it may be copied if it resolves
                        as single part remote source copy
        :param dict,None file_info: file_info for the new file, if ``None`` it will be set to empty dict
                        or it may be copied if it resolves as single part remote source copy
        :param b2sdk.v2.AbstractProgressListener,None progress_listener: a progress listener object to use,
                        or ``None`` to not report progress
        :param int,None recommended_upload_part_size: the recommended part size to use for uploading local sources
                        or ``None`` to determine automatically, but remote sources would be copied with
                        maximum possible part size
        :param str,None continue_large_file_id: large file id that should be selected to resume file creation
                        for multipart upload/copy, if ``None`` in multipart case it would always start a new
                        large file
        :param b2sdk.v2.EncryptionSetting encryption: encryption settings (``None`` if unknown)
        :param b2sdk.v2.FileRetentionSetting file_retention: file retention setting
        :param bool legal_hold: legal hold setting
        :param min_part_size: lower limit of part size for the transfer planner, in bytes
        :param max_part_size: upper limit of part size for the transfer planner, in bytes
        :param Sha1HexDigest,None large_file_sha1: SHA-1 hash of the result file or ``None`` if unknown
        :param int,None custom_upload_timestamp: override object creation date, expressed as a number of milliseconds since epoch
        :param str,None cache_control: an optional cache control setting. Syntax based on the section 14.9 of RFC 2616.
            Example string value: 'public, max-age=86400, s-maxage=3600, no-transform'.
        :param str,datetime.datetime,None expires: an optional cache expiration setting.
            If this argument is a string, its syntax must be based on the section 14.21 of RFC 2616.
            Example string value: 'Thu, 01 Dec 2050 16:00:00 GMT'. If this argument is a datetime,
            it will be converted to a string in the same format.
        :param str,None content_disposition: an optional content disposition setting. Syntax based on the section 19.5.1 of RFC 2616.
            Example string value: 'attachment; filename="fname.ext"'.
        :param str,None content_encoding: an optional content encoding setting.Syntax based on the section 14.11 of RFC 2616.
            Example string value: 'gzip'.
        :param str,None content_language: an optional content language setting. Syntax based on the section 14.12 of RFC 2616.
            Example string value: 'mi, en_US'.
        """
        return self._create_file(
            self.api.services.emerger.emerge_stream,
            write_intents_iterator,
            file_name,
            content_type=content_type,
            file_info=file_info,
            progress_listener=progress_listener,
            continue_large_file_id=continue_large_file_id,
            recommended_upload_part_size=recommended_upload_part_size,
            encryption=encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
            min_part_size=min_part_size,
            max_part_size=max_part_size,
            large_file_sha1=large_file_sha1,
            custom_upload_timestamp=custom_upload_timestamp,
            cache_control=cache_control,
            expires=expires,
            content_disposition=content_disposition,
            content_encoding=content_encoding,
            content_language=content_language,
        )

    def _create_file(
        self,
        emerger_method,
        write_intents_iterable,
        file_name,
        content_type: str | None = None,
        file_info=None,
        progress_listener=None,
        recommended_upload_part_size=None,
        continue_large_file_id=None,
        encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        min_part_size: int | None = None,
        max_part_size: int | None = None,
        large_file_sha1=None,
        cache_control: str | None = None,
        expires: str | dt.datetime | None = None,
        content_disposition: str | None = None,
        content_encoding: str | None = None,
        content_language: str | None = None,
        **kwargs
    ):
        validate_b2_file_name(file_name)
        progress_listener = progress_listener or DoNothingProgressListener()

        file_info = self._merge_file_info_and_headers_params(
            file_info=file_info,
            cache_control=cache_control,
            expires=expires,
            content_disposition=content_disposition,
            content_encoding=content_encoding,
            content_language=content_language,
        )
        return emerger_method(
            self.id_,
            write_intents_iterable,
            file_name,
            content_type,
            file_info,
            progress_listener,
            recommended_upload_part_size=recommended_upload_part_size,
            continue_large_file_id=continue_large_file_id,
            encryption=encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
            min_part_size=min_part_size,
            max_part_size=max_part_size,
            large_file_sha1=large_file_sha1,
            **kwargs
        )

    def concatenate(
        self,
        outbound_sources,
        file_name,
        content_type=None,
        file_info=None,
        progress_listener=None,
        recommended_upload_part_size=None,
        continue_large_file_id=None,
        encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        min_part_size: int | None = None,
        max_part_size: int | None = None,
        large_file_sha1=None,
        custom_upload_timestamp: int | None = None,
        cache_control: str | None = None,
        expires: str | dt.datetime | None = None,
        content_disposition: str | None = None,
        content_encoding: str | None = None,
        content_language: str | None = None,
    ):
        """
        Creates a new file in this bucket by concatenating multiple remote or local sources.

        .. note:
            ``custom_upload_timestamp`` is disabled by default - please talk to customer support to enable it on your account (if you really need it)

        :param list[b2sdk.v2.OutboundTransferSource] outbound_sources: list of outbound sources (remote or local)
        :param str file_name: file name of the new file
        :param str,None content_type: content_type for the new file, if ``None`` content_type would be
                        automatically determined from file name or it may be copied if it resolves
                        as single part remote source copy
        :param dict,None file_info: file_info for the new file, if ``None`` it will be set to empty dict
                        or it may be copied if it resolves as single part remote source copy
        :param b2sdk.v2.AbstractProgressListener,None progress_listener: a progress listener object to use,
                        or ``None`` to not report progress
        :param int,None recommended_upload_part_size: the recommended part size to use for uploading local sources
                        or ``None`` to determine automatically, but remote sources would be copied with
                        maximum possible part size
        :param str,None continue_large_file_id: large file id that should be selected to resume file creation
                        for multipart upload/copy, ``None`` for automatic search for this id
        :param b2sdk.v2.EncryptionSetting encryption: encryption settings (``None`` if unknown)
        :param b2sdk.v2.FileRetentionSetting file_retention: file retention setting
        :param bool legal_hold: legal hold setting
        :param min_part_size: lower limit of part size for the transfer planner, in bytes
        :param max_part_size: upper limit of part size for the transfer planner, in bytes
        :param Sha1HexDigest,None large_file_sha1: SHA-1 hash of the result file or ``None`` if unknown
        :param int,None custom_upload_timestamp: override object creation date, expressed as a number of milliseconds since epoch
        :param str,None cache_control: an optional cache control setting. Syntax based on the section 14.9 of RFC 2616.
            Example string value: 'public, max-age=86400, s-maxage=3600, no-transform'.
        :param str,datetime.datetime,None expires: an optional cache expiration setting.
            If this argument is a string, its syntax must be based on the section 14.21 of RFC 2616.
            Example string value: 'Thu, 01 Dec 2050 16:00:00 GMT'. If this argument is a datetime,
            it will be converted to a string in the same format.
        :param str,None content_disposition: an optional content disposition setting. Syntax based on the section 19.5.1 of RFC 2616.
            Example string value: 'attachment; filename="fname.ext"'.
        :param str,None content_encoding: an optional content encoding setting.Syntax based on the section 14.11 of RFC 2616.
            Example string value: 'gzip'.
        :param str,None content_language: an optional content language setting. Syntax based on the section 14.12 of RFC 2616.
            Example string value: 'mi, en_US'.
        """
        return self.create_file(
            list(WriteIntent.wrap_sources_iterator(outbound_sources)),
            file_name,
            content_type=content_type,
            file_info=file_info,
            progress_listener=progress_listener,
            recommended_upload_part_size=recommended_upload_part_size,
            continue_large_file_id=continue_large_file_id,
            encryption=encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
            min_part_size=min_part_size,
            max_part_size=max_part_size,
            large_file_sha1=large_file_sha1,
            custom_upload_timestamp=custom_upload_timestamp,
            cache_control=cache_control,
            expires=expires,
            content_disposition=content_disposition,
            content_encoding=content_encoding,
            content_language=content_language,
        )

    def concatenate_stream(
        self,
        outbound_sources_iterator,
        file_name,
        content_type=None,
        file_info=None,
        progress_listener=None,
        recommended_upload_part_size=None,
        continue_large_file_id=None,
        encryption: EncryptionSetting | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        large_file_sha1: Sha1HexDigest | None = None,
        custom_upload_timestamp: int | None = None,
        cache_control: str | None = None,
        expires: str | dt.datetime | None = None,
        content_disposition: str | None = None,
        content_encoding: str | None = None,
        content_language: str | None = None,
    ):
        """
        Creates a new file in this bucket by concatenating stream of multiple remote or local sources.

        :param iterator[b2sdk.v2.OutboundTransferSource] outbound_sources_iterator: iterator of outbound sources
        :param str file_name: file name of the new file
        :param str,None content_type: content_type for the new file, if ``None`` content_type would be
                        automatically determined or it may be copied if it resolves
                        as single part remote source copy
        :param dict,None file_info: file_info for the new file, if ``None`` it will be set to empty dict
                        or it may be copied if it resolves as single part remote source copy
        :param b2sdk.v2.AbstractProgressListener,None progress_listener: a progress listener object to use,
                        or ``None`` to not report progress
        :param int,None recommended_upload_part_size: the recommended part size to use for uploading local sources
                        or ``None`` to determine automatically, but remote sources would be copied with
                        maximum possible part size
        :param str,None continue_large_file_id: large file id that should be selected to resume file creation
                        for multipart upload/copy, if ``None`` in multipart case it would always start a new
                        large file
        :param b2sdk.v2.EncryptionSetting encryption: encryption setting (``None`` if unknown)
        :param b2sdk.v2.FileRetentionSetting file_retention: file retention setting
        :param bool legal_hold: legal hold setting
        :param Sha1HexDigest,None large_file_sha1: SHA-1 hash of the result file or ``None`` if unknown
        :param int,None custom_upload_timestamp: override object creation date, expressed as a number of milliseconds since epoch
        :param str,None cache_control: an optional cache control setting. Syntax based on the section 14.9 of RFC 2616.
            Example string value: 'public, max-age=86400, s-maxage=3600, no-transform'.
        :param str,datetime.datetime,None expires: an optional cache expiration setting.
            If this argument is a string, its syntax must be based on the section 14.21 of RFC 2616.
            Example string value: 'Thu, 01 Dec 2050 16:00:00 GMT'. If this argument is a datetime,
            it will be converted to a string in the same format.
        :param str,None content_disposition: an optional content disposition setting. Syntax based on the section 19.5.1 of RFC 2616.
            Example string value: 'attachment; filename="fname.ext"'.
        :param str,None content_encoding: an optional content encoding setting.Syntax based on the section 14.11 of RFC 2616.
            Example string value: 'gzip'.
        :param str,None content_language: an optional content language setting. Syntax based on the section 14.12 of RFC 2616.
            Example string value: 'mi, en_US'.
        """
        return self.create_file_stream(
            WriteIntent.wrap_sources_iterator(outbound_sources_iterator),
            file_name,
            content_type=content_type,
            file_info=file_info,
            progress_listener=progress_listener,
            recommended_upload_part_size=recommended_upload_part_size,
            continue_large_file_id=continue_large_file_id,
            encryption=encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
            large_file_sha1=large_file_sha1,
            custom_upload_timestamp=custom_upload_timestamp,
            cache_control=cache_control,
            expires=expires,
            content_disposition=content_disposition,
            content_encoding=content_encoding,
            content_language=content_language,
        )

    def get_download_url(self, filename):
        """
        Get file download URL.

        :param str filename: a file name
        :rtype: str
        """
        return "{}/file/{}/{}".format(
            self.api.account_info.get_download_url(),
            b2_url_encode(self.name),
            b2_url_encode(filename),
        )

    def hide_file(self, file_name):
        """
        Hide a file.

        :param str file_name: a file name
        :rtype: b2sdk.v2.FileVersion
        """
        response = self.api.session.hide_file(self.id_, file_name)
        return self.api.file_version_factory.from_api_response(response)

    def unhide_file(self, file_name: str, bypass_governance: bool = False) -> FileIdAndName:
        """
        Unhide a file by deleting the "hide marker".
        """

        # get the latest file version
        file_versions = self.list_file_versions(file_name=file_name, fetch_count=1)
        latest_file_version = next(file_versions, None)
        if latest_file_version is None:
            raise FileNotPresent(bucket_name=self.name, file_id_or_name=file_name)

        action = latest_file_version.action
        if action == "upload":
            raise FileNotHidden(file_name)
        elif action == "delete":
            raise FileDeleted(file_name)
        elif action != "hide":
            raise UnexpectedFileVersionAction(action)

        return self.delete_file_version(latest_file_version.id_, file_name, bypass_governance)

    def copy(
        self,
        file_id,
        new_file_name,
        content_type=None,
        file_info=None,
        offset=0,
        length=None,
        progress_listener=None,
        destination_encryption: EncryptionSetting | None = None,
        source_encryption: EncryptionSetting | None = None,
        source_file_info: dict | None = None,
        source_content_type: str | None = None,
        file_retention: FileRetentionSetting | None = None,
        legal_hold: LegalHold | None = None,
        cache_control: str | None = None,
        min_part_size: int | None = None,
        max_part_size: int | None = None,
        expires: str | dt.datetime | None = None,
        content_disposition: str | None = None,
        content_encoding: str | None = None,
        content_language: str | None = None,
    ) -> FileVersion:
        """
        Creates a new file in this bucket by (server-side) copying from an existing file.

        :param str file_id: file ID of existing file to copy from
        :param str new_file_name: file name of the new file
        :param str,None content_type: content_type for the new file, if ``None`` and ``b2_copy_file`` will be used
                        content_type will be copied from source file - otherwise content_type would be
                        automatically determined
        :param dict,None file_info: file_info for the new file, if ``None`` will and ``b2_copy_file`` will be used
                        file_info will be copied from source file - otherwise it will be set to empty dict
        :param int offset: offset of existing file that copy should start from
        :param int,None length: number of bytes to copy, if ``None`` then ``offset`` have to be ``0`` and it will
                        use ``b2_copy_file`` without ``range`` parameter so it may fail if file is too large.
                        For large files length have to be specified to use ``b2_copy_part`` instead.
        :param b2sdk.v2.AbstractProgressListener,None progress_listener: a progress listener object to use
                        for multipart copy, or ``None`` to not report progress
        :param b2sdk.v2.EncryptionSetting destination_encryption: encryption settings for the destination
                        (``None`` if unknown)
        :param b2sdk.v2.EncryptionSetting source_encryption: encryption settings for the source
                        (``None`` if unknown)
        :param dict,None source_file_info: source file's file_info dict, useful when copying files with SSE-C
        :param str,None source_content_type: source file's content type, useful when copying files with SSE-C
        :param b2sdk.v2.FileRetentionSetting file_retention: file retention setting for the new file.
        :param bool legal_hold: legal hold setting for the new file.
        :param str,None cache_control: an optional cache control setting. Syntax based on the section 14.9 of RFC 2616.
            Example string value: 'public, max-age=86400, s-maxage=3600, no-transform'.
        :param min_part_size: lower limit of part size for the transfer planner, in bytes
        :param max_part_size: upper limit of part size for the transfer planner, in bytes
        :param str,datetime.datetime,None expires: an optional cache expiration setting.
            If this argument is a string, its syntax must be based on the section 14.21 of RFC 2616.
            Example string value: 'Thu, 01 Dec 2050 16:00:00 GMT'. If this argument is a datetime,
            it will be converted to a string in the same format.
        :param str,None content_disposition: an optional content disposition setting. Syntax based on the section 19.5.1 of RFC 2616.
            Example string value: 'attachment; filename="fname.ext"'.
        :param str,None content_encoding: an optional content encoding setting.Syntax based on the section 14.11 of RFC 2616.
            Example string value: 'gzip'.
        :param str,None content_language: an optional content language setting. Syntax based on the section 14.12 of RFC 2616.
            Example string value: 'mi, en_US'.
        """

        copy_source = CopySource(
            file_id,
            offset=offset,
            length=length,
            encryption=source_encryption,
            source_file_info=source_file_info,
            source_content_type=source_content_type,
        )
        if not length:
            # TODO: it feels like this should be checked on lower level - eg. RawApi
            validate_b2_file_name(new_file_name)
            try:
                progress_listener = progress_listener or DoNothingProgressListener()
                file_info = self._merge_file_info_and_headers_params(
                    file_info=file_info,
                    cache_control=cache_control,
                    expires=expires,
                    content_disposition=content_disposition,
                    content_encoding=content_encoding,
                    content_language=content_language,
                )
                return self.api.services.copy_manager.copy_file(
                    copy_source,
                    new_file_name,
                    content_type=content_type,
                    file_info=file_info,
                    destination_bucket_id=self.id_,
                    progress_listener=progress_listener,
                    destination_encryption=destination_encryption,
                    source_encryption=source_encryption,
                    file_retention=file_retention,
                    legal_hold=legal_hold,
                ).result()
            except CopySourceTooBig as e:
                copy_source.length = e.size
                progress_listener = DoNothingProgressListener()
                logger.warning(
                    'a copy of large object of unknown size is upgraded to the large file interface. No progress report will be provided.'
                )
        return self.create_file(
            [WriteIntent(copy_source)],
            new_file_name,
            content_type=content_type,
            file_info=file_info,
            progress_listener=progress_listener,
            encryption=destination_encryption,
            file_retention=file_retention,
            legal_hold=legal_hold,
            cache_control=cache_control,
            min_part_size=min_part_size,
            max_part_size=max_part_size,
        )

    def delete_file_version(self, file_id: str, file_name: str, bypass_governance: bool = False):
        """
        Delete a file version.

        :param file_id: a file ID
        :param file_name: a file name
        :param bypass_governance: Must be set to true if deleting a file version protected by Object Lock governance
                                  mode retention settings (unless its retention period expired)
        """
        # filename argument is not first, because one day it may become optional
        return self.api.delete_file_version(file_id, file_name, bypass_governance)

    @disable_trace
    def as_dict(self):
        """
        Return bucket representation as a dictionary.

        :rtype: dict
        """
        result = {
            'accountId': self.api.account_info.get_account_id(),
            'bucketId': self.id_,
        }
        if self.name is not None:
            result['bucketName'] = self.name
        if self.type_ is not None:
            result['bucketType'] = self.type_
        result['bucketInfo'] = self.bucket_info
        result['corsRules'] = self.cors_rules
        result['lifecycleRules'] = self.lifecycle_rules
        result['revision'] = self.revision
        result['options'] = self.options_set
        result['defaultServerSideEncryption'] = self.default_server_side_encryption.as_dict()
        result['isFileLockEnabled'] = self.is_file_lock_enabled
        result['defaultRetention'] = self.default_retention.as_dict()
        result['replication'] = self.replication and self.replication.as_dict()

        return result

    def __repr__(self):
        return f'Bucket<{self.id_},{self.name},{self.type_}>'

    def get_notification_rules(self) -> list[NotificationRuleResponse]:
        """
        Get all notification rules for this bucket.
        """
        return self.api.session.get_bucket_notification_rules(self.id_)

    def set_notification_rules(self,
                               rules: Iterable[NotificationRule]) -> list[NotificationRuleResponse]:
        """
        Set notification rules for this bucket.
        """
        return self.api.session.set_bucket_notification_rules(self.id_, rules)


class BucketFactory:
    """
    This is a factory for creating bucket objects from different kind of objects.
    """
    BUCKET_CLASS = staticmethod(Bucket)

    @classmethod
    def from_api_response(cls, api, response):
        """
        Create a Bucket object from API response.

        :param b2sdk.v2.B2Api api: API object
        :param requests.Response response: response object
        :rtype: b2sdk.v2.Bucket
        """
        return [cls.from_api_bucket_dict(api, bucket_dict) for bucket_dict in response['buckets']]

    @classmethod
    def from_api_bucket_dict(cls, api, bucket_dict):
        """
        Turn a dictionary, like this:

        .. code-block:: python

            {
                "bucketType": "allPrivate",
                "bucketId": "a4ba6a39d8b6b5fd561f0010",
                "bucketName": "zsdfrtsazsdfafr",
                "accountId": "4aa9865d6f00",
                "bucketInfo": {},
                "options": [],
                "revision": 1,
                "defaultServerSideEncryption": {
                    "isClientAuthorizedToRead" : true,
                    "value": {
                        "algorithm" : "AES256",
                        "mode" : "SSE-B2"
                    }
                },
                "fileLockConfiguration": {
                    "isClientAuthorizedToRead": true,
                    "value": {
                        "defaultRetention": {
                            "mode": null,
                            "period": null
                            },
                            "isFileLockEnabled": false
                        }
                },
                "replicationConfiguration": {
                    "clientIsAllowedToRead": true,
                    "value": {
                        "asReplicationSource": {
                            "replicationRules": [
                                {
                                    "destinationBucketId": "c5f35d53a90a7ea284fb0719",
                                    "fileNamePrefix": "",
                                    "includeExistingFiles": True,
                                    "isEnabled": true,
                                    "priority": 1,
                                    "replicationRuleName": "replication-us-west"
                                },
                                {
                                    "destinationBucketId": "55f34d53a96a7ea284fb0719",
                                    "fileNamePrefix": "",
                                    "includeExistingFiles": True,
                                    "isEnabled": true,
                                    "priority": 2,
                                    "replicationRuleName": "replication-us-west-2"
                                }
                            ],
                            "sourceApplicationKeyId": "10053d55ae26b790000000006"
                        },
                        "asReplicationDestination": {
                            "sourceToDestinationKeyMapping": {
                                "10053d55ae26b790000000045": "10053d55ae26b790000000004",
                                "10053d55ae26b790000000046": "10053d55ae26b790030000004"
                            }
                        }
                    }
                }
            }

        into a Bucket object.

        :param b2sdk.v2.B2Api api: API client
        :param dict bucket_dict: a dictionary with bucket properties
        :rtype: b2sdk.v2.Bucket

        """
        type_ = bucket_dict['bucketType']
        if type_ is None:
            raise UnrecognizedBucketType(bucket_dict['bucketType'])
        bucket_name = bucket_dict['bucketName']
        bucket_id = bucket_dict['bucketId']
        bucket_info = bucket_dict['bucketInfo']
        cors_rules = bucket_dict['corsRules']
        lifecycle_rules = bucket_dict['lifecycleRules']
        revision = bucket_dict['revision']
        options = set(bucket_dict['options'])

        if 'defaultServerSideEncryption' not in bucket_dict:
            raise UnexpectedCloudBehaviour('server did not provide `defaultServerSideEncryption`')
        default_server_side_encryption = EncryptionSettingFactory.from_bucket_dict(bucket_dict)
        file_lock_configuration = FileLockConfiguration.from_bucket_dict(bucket_dict)
        replication = ReplicationConfigurationFactory.from_bucket_dict(bucket_dict).value
        return cls.BUCKET_CLASS(
            api,
            bucket_id,
            bucket_name,
            type_,
            bucket_info,
            cors_rules,
            lifecycle_rules,
            revision,
            bucket_dict,
            options,
            default_server_side_encryption,
            file_lock_configuration.default_retention,
            file_lock_configuration.is_file_lock_enabled,
            replication,
        )
