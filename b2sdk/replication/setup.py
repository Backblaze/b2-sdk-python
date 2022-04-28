######################################################################
#
# File: b2sdk/replication/setup.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

# b2 replication-setup [--profile profileName] --destination-profile destinationProfileName sourceBucketPath destinationBucketName [ruleName]
# b2 replication-debug [--profile profileName] [--destination-profile destinationProfileName] bucketPath
# b2 replication-status [--profile profileName] [--destination-profile destinationProfileName] [sourceBucketPath] [destinationBucketPath]

# b2 replication-pause [--profile profileName] (sourceBucketName|sourceBucketPath) [replicationRuleName]
# b2 replication-unpause [--profile profileName] (sourceBucketName|sourceBucketPath) [replicationRuleName]
# b2 replication-accept destinationBucketName sourceKeyId [destinationKeyId]
# b2 replication-deny destinationBucketName sourceKeyId

from typing import ClassVar, List, Optional, Tuple
import itertools
import logging

from b2sdk.api import B2Api
from b2sdk.application_key import ApplicationKey
from b2sdk.bucket import Bucket
from b2sdk.utils import B2TraceMeta
from b2sdk.replication.setting import ReplicationConfiguration, ReplicationDestinationConfiguration, ReplicationRule, ReplicationSourceConfiguration

logger = logging.getLogger(__name__)


class ReplicationSetupHelper(metaclass=B2TraceMeta):
    """ class with various methods that help with repliction management """
    PRIORITY_OFFSET: ClassVar[int] = 5  #: how far to to put the new rule from the existing rules
    DEFAULT_PRIORITY: ClassVar[
        int
    ] = ReplicationRule.DEFAULT_PRIORITY  #: what priority to set if there are no preexisting rules
    MAX_PRIORITY: ClassVar[
        int] = ReplicationRule.MAX_PRIORITY  #: maximum allowed priority of a replication rule
    DEFAULT_SOURCE_CAPABILITIES: ClassVar[Tuple[str, ...]] = (
        'readFiles',
        'readFileLegalHolds',
        'readFileRetentions',
    )
    DEFAULT_DESTINATION_CAPABILITIES: ClassVar[Tuple[str, ...]] = (
        'writeFiles',
        'writeFileLegalHolds',
        'writeFileRetentions',
        'deleteFiles',
    )

    def __init__(self, source_b2api: B2Api = None, destination_b2api: B2Api = None):
        assert source_b2api is not None or destination_b2api is not None
        self.source_b2api = source_b2api
        self.destination_b2api = destination_b2api

    def setup_both(
        self,
        source_bucket_name: str,
        destination_bucket: Bucket,
        name: Optional[str] = None,  #: name for the new replication rule
        priority: int = None,  #: priority for the new replication rule
        prefix: Optional[str] = None,
    ) -> Tuple[Bucket, Bucket]:
        source_bucket = self.setup_source(
            source_bucket_name,
            prefix,
            destination_bucket,
            name,
            priority,
        )
        destination_bucket = self.setup_destination(
            source_bucket.replication.as_replication_source.source_application_key_id,
            destination_bucket,
        )
        return source_bucket, destination_bucket

    def setup_destination(
        self,
        source_key_id: str,
        destination_bucket: Bucket,
    ) -> Bucket:
        api: B2Api = destination_bucket.api
        destination_bucket = api.list_buckets(destination_bucket.name)[0]  # fresh!
        if destination_bucket.replication is None or destination_bucket.replication.as_replication_source is None:
            source_configuration = None
        else:
            source_configuration = destination_bucket.replication.as_replication_source

        if destination_bucket.replication is None or destination_bucket.replication.as_replication_destination is None:
            destination_configuration = ReplicationDestinationConfiguration({})
        else:
            destination_configuration = destination_bucket.replication.as_replication_destination

        keys_to_purge, destination_key = self._get_destination_key(
            api,
            destination_bucket,
            destination_configuration,
        )

        destination_configuration.source_to_destination_key_mapping[source_key_id
                                                                   ] = destination_key.id_
        new_replication_configuration = ReplicationConfiguration(
            source_configuration,
            destination_configuration,
        )
        return destination_bucket.update(
            if_revision_is=destination_bucket.revision,
            replication=new_replication_configuration,
        )

    @classmethod
    def _get_destination_key(
        cls,
        api: B2Api,
        destination_bucket: Bucket,
        destination_configuration: ReplicationDestinationConfiguration,
    ):
        keys_to_purge = []
        current_destination_key_ids = destination_configuration.source_to_destination_key_mapping.values(
        )
        key = None
        for current_destination_key_id in current_destination_key_ids:
            # potential inefficiency here as we are fetching keys one by one, however
            # the number of keys on an account is limited to a 100 000 000 per account lifecycle
            # while the number of keys in the map can be expected to be very low
            current_destination_key = api.get_key(current_destination_key_id)
            if current_destination_key is None:
                logger.debug(
                    'zombie key found in replication destination_configuration.source_to_destination_key_mapping: %s',
                    current_destination_key_id
                )
                keys_to_purge.append(current_destination_key_id)
                continue
            if current_destination_key.has_capabilities(
                cls.DEFAULT_DESTINATION_CAPABILITIES
            ) and not current_destination_key.name_prefix:
                logger.debug('matching destination key found: %s', current_destination_key_id)
                key = current_destination_key
                # not breaking here since we want to fill the purge list
            else:
                logger.info('non-matching destination key found: %s', current_destination_key)
        if not key:
            logger.debug("no matching key found, making a new one")
            key = cls._create_destination_key(
                name=destination_bucket.name[:91] + '-replidst',
                api=api,
                bucket_id=destination_bucket.id_,
                prefix=None,
            )
        return keys_to_purge, key

    def setup_source(
        self,
        source_bucket_name,
        prefix,
        destination_bucket: Bucket,
        name,
        priority,
    ) -> Bucket:
        source_bucket: Bucket = self.source_b2api.list_buckets(source_bucket_name)[0]  # fresh!
        if prefix is None:
            prefix = ""

        try:
            current_source_rrs = source_bucket.replication.as_replication_source.rules
        except (NameError, AttributeError):
            current_source_rrs = []
        try:
            destination_configuration = source_bucket.replication.as_replication_destination
        except (NameError, AttributeError):
            destination_configuration = None

        source_key = self._get_source_key(
            source_bucket,
            prefix,
            source_bucket.replication,
            current_source_rrs,
        )
        priority = self._get_priority_for_new_rule(
            priority,
            current_source_rrs,
        )
        name = self._get_name_for_new_rule(
            name,
            current_source_rrs,
            destination_bucket.name,
        )
        new_rr = ReplicationRule(
            name=name,
            priority=priority,
            destination_bucket_id=destination_bucket.id_,
            file_name_prefix=prefix,
        )
        new_replication_configuration = ReplicationConfiguration(
            ReplicationSourceConfiguration(
                source_application_key_id=source_key.id_,
                rules=current_source_rrs + [new_rr],
            ),
            destination_configuration,
        )
        return source_bucket.update(
            if_revision_is=source_bucket.revision,
            replication=new_replication_configuration,
        )

    @classmethod
    def _get_source_key(
        cls,
        source_bucket,
        prefix,
        current_replication_configuration: ReplicationConfiguration,
        current_source_rrs,
    ) -> ApplicationKey:
        api = source_bucket.api

        current_source_key = api.get_key(
            current_replication_configuration.as_replication_source.source_application_key_id
        )
        do_create_key = cls._should_make_new_source_key(
            current_replication_configuration,
            current_source_key,
        )
        if not do_create_key:
            return current_source_key

        new_key = cls._create_source_key(
            name=source_bucket.name[:91] + '-replisrc',
            api=api,
            bucket_id=source_bucket.id_,
        )  # no prefix!
        return new_key

    @classmethod
    def _should_make_new_source_key(
        cls,
        current_replication_configuration: ReplicationConfiguration,
        current_source_key: Optional[ApplicationKey],
    ) -> bool:
        if current_replication_configuration.as_replication_source.source_application_key_id is None:
            logger.debug('will create a new source key because no key is set')
            return True

        if current_source_key is None:
            logger.debug(
                'will create a new source key because current key "%s" was deleted',
                current_replication_configuration.as_replication_source.source_application_key_id,
            )
            return True

        if current_source_key.name_prefix:
            logger.debug(
                'will create a new source key because current key %s had a prefix: "%s"',
                current_source_key.name_prefix,
            )
            return True

        if not current_source_key.has_capabilities(cls.DEFAULT_SOURCE_CAPABILITIES):
            logger.debug(
                'will create a new source key because %s installed so far does not have enough permissions for replication source: ',
                current_source_key.id_,
                current_source_key.capabilities,
            )
            return True
        return False  # current key is ok

    @classmethod
    def _create_source_key(
        cls,
        name: str,
        api: B2Api,
        bucket_id: str,
        prefix: Optional[str] = None,
    ) -> ApplicationKey:
        capabilities = cls.DEFAULT_SOURCE_CAPABILITIES
        return cls._create_key(name, api, bucket_id, prefix, capabilities)

    @classmethod
    def _create_destination_key(
        cls,
        name: str,
        api: B2Api,
        bucket_id: str,
        prefix: Optional[str] = None,
    ) -> ApplicationKey:
        capabilities = cls.DEFAULT_DESTINATION_CAPABILITIES
        return cls._create_key(name, api, bucket_id, prefix, capabilities)

    @classmethod
    def _create_key(
        cls,
        name: str,
        api: B2Api,
        bucket_id: str,
        prefix: Optional[str] = None,
        capabilities=tuple(),
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
    def _get_name_for_new_rule(
        cls, name: Optional[str], current_source_rrs, destination_bucket_name
    ):
        if name is not None:
            return name
        existing_names = set(rr.name for rr in current_source_rrs)
        suffixes = cls._get_rule_name_candidate_suffixes()
        while True:
            candidate = '%s%s' % (destination_bucket_name, next(suffixes))
            if candidate not in existing_names:
                return candidate

    @classmethod
    def _get_rule_name_candidate_suffixes(cls):
        """
        >>> a = ReplicationSetupHelper._get_rule_name_candidate_suffixes()
        >>> [next(a) for i in range(10)]
        ['', '2', '3', '4', '5', '6', '7', '8', '9', '10']
        """
        return map(str, itertools.chain([''], itertools.count(2)))

    @classmethod
    def _partion_bucket_path(cls, bucket_path: str) -> Tuple[str, str]:
        bucket_name, _, path = bucket_path.partition('/')
        return bucket_name, path
