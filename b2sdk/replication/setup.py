######################################################################
#
# File: b2sdk/replication/setup.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

# b2 replication-setup [--create-keys (both|source|destination|auto)] [--widen-source-key-mode (fail|auto)] [--profile profileName] [--priority int|auto]--destination-profile destinationProfileName sourceBucketPath destinationBucketPath [rrName]
# b2 replication-debug [--profile profileName] [--destination-profile destinationProfileName] bucketPath
# b2 replication-status [--profile profileName] [--destination-profile destinationProfileName] [sourceBucketPath] [destinationBucketPath]
# b2 replication-pause [--profile profileName] [--rrName replicationRuleName] [sourceBucketPath] [destinationBucketPath]
# b2 replication-unpause [--profile profileName] [--rrName replicationRuleName] [sourceBucketPath] [destinationBucketPath]
# b2 replication-scan [--profile profileName] [--destination-profile destinationProfileName] [--rrName replicationRuleName] [sourceBucketPath] [destinationBucketPath]

from typing import ClassVar, List, Tuple
from enum import Enum, auto, unique
import logging

from b2sdk.api import B2Api
from b2sdk.application_key import ApplicationKey
from b2sdk.bucket import Bucket
from b2sdk.replication.setting import ReplicationConfiguration, ReplicationDestinationConfiguration, ReplicationRule, ReplicationSourceConfiguration

logger = logging.getLogger(__name__)


@unique
class ReplicationSetupHelperKeyMode(Enum):
    NONE = auto()
    AUTO = auto()
    SOURCE = auto()
    DESTINATION = auto()
    BOTH = auto()


class ReplicationSetupHelper:
    """ class with various methods that help with repliction management """
    DEFAULT_KEY_MODE: ClassVar[
        ReplicationSetupHelperKeyMode
    ] = ReplicationSetupHelperKeyMode.AUTO  #: default key mode for setup signatures
    PRIORITY_OFFSET: ClassVar[int] = 10  #: how far to to put the new rule from the existing rules
    DEFAULT_PRIORITY: ClassVar[int] = 128  #: what priority to set if there are no preexisting rules
    MAX_PRIORITY: ClassVar[int] = 255  #: maximum allowed priority of a replication rule
    DEFAULT_SOURCE_CAPABILITIES = 'readFiles'

    def __init__(self, source_b2api: B2Api = None, destination_b2api: B2Api = None):
        assert source_b2api is not None or destination_b2api is not None
        self.source_b2api = source_b2api
        self.destination_b2api = destination_b2api

    def setup_both(
        self,
        source_bucket_path: str,
        destination_bucket: Bucket,
        name: str = None,  #: name for the new replication rule
        priority: int = None,  #: priority for the new replication rule
        key_mode: ReplicationSetupHelperKeyMode = DEFAULT_KEY_MODE,
        widen_source_key: bool = True,
    ):
        source_key: ApplicationKey = self.setup_source(
            source_bucket_path,
            destination_bucket.id_,
            name,
            priority,
            key_mode,
            widen_source_key,
        )
        assert source_key
        # TODO
        #self.setup_destination(source_key.id_, destination_bucket, key_mode)

    def setup_destination(
        self,
        api: B2Api,
        source_key_id: str,
        destination_bucket: Bucket,
        key_mode: ReplicationSetupHelperKeyMode = DEFAULT_KEY_MODE,
    ):
        try:
            source_configuration = destination_bucket.replication.as_replication_source
        except (NameError, AttributeError):
            source_configuration = None
        try:
            destination_configuration = destination_bucket.replication.as_replication_destination
        except (NameError, AttributeError):
            destination_configuration = ReplicationDestinationConfiguration({})

        current_destination_key = destination_configuration.source_to_destination_key_mapping.get(
            source_key_id
        )

        destination_key = self._get_destination_key(current_destination_key)
        destination_configuration.source_to_destination_key_mapping[source_key_id] = destination_key
        new_replication_configuration = ReplicationConfiguration(
            source_configuration,
            destination_configuration,
        )
        return new_replication_configuration

    def _get_destination_key(self, current_destination_key) -> ApplicationKey:
        #    name=source_bucket.name[:91] + '-replisrc',
        do_create_key = False
        if current_destination_key is None:
            do_create_key = True
        if current_destination_key.prefix:
            pass
        # TODO
        if not do_create_key:
            return current_destination_key

    def setup_source(
        self, source_bucket_path, destination_bucket_id, name, priority, key_mode, widen_source_key
    ) -> ApplicationKey:
        source_bucket_name, prefix = self._partion_bucket_path(source_bucket_path)
        source_bucket: Bucket = self.source_b2api.list_buckets(source_bucket_name)[0]  # fresh

        try:
            current_source_rrs = source_bucket.replication.as_replication_source.rules
        except (NameError, AttributeError):
            current_source_rrs = []
        try:
            destination_configuration = source_bucket.replication.as_replication_destination
        except (NameError, AttributeError):
            destination_configuration = None

        source_key_id = self._get_source_key(
            source_bucket,
            prefix,
            key_mode,
            widen_source_key,
            source_bucket.replication,
            current_source_rrs,
        )
        priority = self._get_priority_for_new_rule(
            priority,
            current_source_rrs,
        )

        new_rr = ReplicationRule(
            name=name,
            priority=priority,
            destination_bucket_id=destination_bucket_id,
            file_name_prefix=prefix,
        )
        new_replication_configuration = ReplicationConfiguration(
            ReplicationSourceConfiguration(
                source_application_key_id=source_key_id,
                rules=current_source_rrs + [new_rr],
            ),
            destination_configuration,
        )
        source_bucket.update(
            if_revision_is=source_bucket.revision,
            replication=new_replication_configuration,
        )
        return source_key_id

    @classmethod
    def _get_source_key(
        cls,
        source_bucket,
        prefix,
        key_mode,
        widen_source_key,
        current_replication_configuration: ReplicationConfiguration,
        current_source_rrs,
    ) -> str:
        assert widen_source_key  # TODO
        api = source_bucket.api

        force_source_key_creation: bool = key_mode in (
            ReplicationSetupHelperKeyMode.SOURCE,
            ReplicationSetupHelperKeyMode.BOTH,
            #ReplicationSetupHelperKeyMode.AUTO,
        )
        #assert force_source_key_creation  # TODO
        #create_source_key_if_needed: bool = key_mode is ReplicationSetupHelperKeyMode.LAZY

        do_create_key = False
        new_prefix = cls._get_narrowest_common_prefix(
            [rr.path for rr in current_source_rrs] + [prefix]
        )
        if new_prefix != prefix:
            logger.debug(
                'forced key creation due to widened key prefix from %s to %s',
                prefix,
                new_prefix,
            )
            prefix = new_prefix
            do_create_key = True

        if force_source_key_creation:
            logger.debug(
                'forced key creation because key_mode is %s',
                ReplicationSetupHelperKeyMode(key_mode).name,
            )
            do_create_key = True

        if not do_create_key:
            if not current_replication_configuration or not current_replication_configuration.as_replication_source:
                do_create_key = True
            else:
                current_source_key = api.get_key(
                    current_replication_configuration.as_replication_source.
                    source_application_key_id
                )
                if current_source_key is None:
                    do_create_key = True
                    logger.debug(
                        'will create a new source key because current key %s has been deleted',
                        current_replication_configuration.as_replication_source.
                        source_application_key_id,
                    )
                else:
                    current_capabilities = current_source_key.get_capabilities()
                    if not prefix.startswith(current_source_key.prefix):
                        do_create_key = True
                        logger.debug(
                            'will create a new source key because %s installed so far does not encompass current needs with its prefix',
                            current_source_key.id_,
                        )
                    elif 'readFiles' not in current_capabilities:  # TODO: more permissions
                        do_create_key = True
                        logger.debug(
                            'will create a new source key because %s installed so far does not have enough permissions for replication source',
                            current_source_key.id_,
                        )
                    else:
                        return current_source_key

        #if not prefix.startswith(current_key.prefix):
        #    do_create_key = True
        #else:
        #    new_key = current_key

        new_key = cls._create_source_key(
            name=source_bucket.name[:91] + '-replisrc',
            api=api,
            bucket_id=source_bucket.id_,
            prefix=prefix,
        )
        return new_key

    @classmethod
    def _create_source_key(
        cls,
        name: str,
        api: B2Api,
        bucket_id: str,
        prefix: str,
    ) -> ApplicationKey:
        capabilities = cls.DEFAULT_SOURCE_CAPABILITIES
        return cls._create_key(name, api, bucket_id, prefix, capabilities)

    @classmethod
    def _create_destination_key(
        cls,
        name: str,
        api: B2Api,
        bucket_id: str,
        prefix: str,
    ) -> ApplicationKey:
        capabilities = cls.DEFAULT_DESTINATION_CAPABILITIES
        return cls._create_key(name, api, bucket_id, prefix, capabilities)

    @classmethod
    def _create_key(
        cls,
        name: str,
        api: B2Api,
        bucket_id: str,
        prefix: str,
        capabilities,
    ) -> ApplicationKey:
        return api.create_key(
            capabilities=capabilities,
            key_name=name,
            bucket_id=bucket_id,
            name_prefix=prefix,
        )

    @classmethod
    def _get_narrowest_common_prefix(cls, widen_to: List[str]) -> str:
        for path in widen_to:
            pass  # TODO
        return ''

    @classmethod
    def _get_priority_for_new_rule(cls, priority, current_source_rrs):
        # if there is no priority hint, look into current rules to determine the last priority and add a constant to it
        if priority is not None:
            return priority
        if current_source_rrs:
            # TODO: maybe handle a case where the existing rrs need to have their priorities decreased to make space
            existing_priority = max(rr.priority for rr in current_source_rrs)
            return min(existing_priority + cls.PRIORITY_OFFSET, cls.MAX_PRIORITY)
        return cls.DEFAULT_PRIORITY

    @classmethod
    def _partion_bucket_path(cls, bucket_path: str) -> Tuple[str, str]:
        bucket_name, _, path = bucket_path.partition('/')
        return bucket_name, path
