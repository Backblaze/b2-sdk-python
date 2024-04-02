######################################################################
#
# File: b2sdk/_internal/replication/setup.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import itertools
import logging

# b2 replication-setup [--profile profileName] --destination-profile destinationProfileName sourceBucketPath destinationBucketName [ruleName]
# b2 replication-debug [--profile profileName] [--destination-profile destinationProfileName] bucketPath
# b2 replication-status [--profile profileName] [--destination-profile destinationProfileName] [sourceBucketPath] [destinationBucketPath]
# b2 replication-pause [--profile profileName] (sourceBucketName|sourceBucketPath) [replicationRuleName]
# b2 replication-unpause [--profile profileName] (sourceBucketName|sourceBucketPath) [replicationRuleName]
# b2 replication-accept destinationBucketName sourceKeyId [destinationKeyId]
# b2 replication-deny destinationBucketName sourceKeyId
from collections.abc import Iterable
from typing import ClassVar

from b2sdk._internal.api import B2Api
from b2sdk._internal.application_key import ApplicationKey
from b2sdk._internal.bucket import Bucket
from b2sdk._internal.replication.setting import ReplicationConfiguration, ReplicationRule
from b2sdk._internal.utils import B2TraceMeta

logger = logging.getLogger(__name__)


class ReplicationSetupHelper(metaclass=B2TraceMeta):
    """ class with various methods that help with setting up repliction """
    PRIORITY_OFFSET: ClassVar[int] = 5  #: how far to to put the new rule from the existing rules
    DEFAULT_PRIORITY: ClassVar[
        int
    ] = ReplicationRule.DEFAULT_PRIORITY  #: what priority to set if there are no preexisting rules
    MAX_PRIORITY: ClassVar[
        int] = ReplicationRule.MAX_PRIORITY  #: maximum allowed priority of a replication rule
    DEFAULT_SOURCE_CAPABILITIES: ClassVar[tuple[str, ...]] = (
        'readFiles',
        'readFileLegalHolds',
        'readFileRetentions',
    )
    DEFAULT_DESTINATION_CAPABILITIES: ClassVar[tuple[str, ...]] = (
        'writeFiles',
        'writeFileLegalHolds',
        'writeFileRetentions',
        'deleteFiles',
    )

    def setup_both(
        self,
        source_bucket: Bucket,
        destination_bucket: Bucket,
        name: str | None = None,  #: name for the new replication rule
        priority: int = None,  #: priority for the new replication rule
        prefix: str | None = None,
        include_existing_files: bool = False,
    ) -> tuple[Bucket, Bucket]:

        # setup source key
        source_key = self._get_source_key(
            source_bucket,
            prefix,
            source_bucket.replication,
        )

        # setup destination
        new_destination_bucket = self.setup_destination(
            source_key.id_,
            destination_bucket,
        )

        # setup source
        new_source_bucket = self.setup_source(
            source_bucket,
            source_key,
            destination_bucket,
            prefix,
            name,
            priority,
            include_existing_files,
        )

        return new_source_bucket, new_destination_bucket

    def setup_destination(
        self,
        source_key_id: str,
        destination_bucket: Bucket,
    ) -> Bucket:
        api: B2Api = destination_bucket.api

        # yapf: disable
        source_configuration = destination_bucket.replication.get_source_configuration_as_dict(
        ) if destination_bucket.replication else {}

        destination_configuration = destination_bucket.replication.get_destination_configuration_as_dict(
        ) if destination_bucket.replication else {'source_to_destination_key_mapping': {}}

        keys_to_purge, destination_key = self._get_destination_key(
            api,
            destination_bucket,
        )
        # note: no clean up of keys_to_purge is actually done

        destination_configuration['source_to_destination_key_mapping'][source_key_id] = destination_key.id_
        new_replication_configuration = ReplicationConfiguration(
            **source_configuration,
            **destination_configuration,
        )
        # yapf: enable
        return destination_bucket.update(
            if_revision_is=destination_bucket.revision,
            replication=new_replication_configuration,
        )

    @classmethod
    def _get_destination_key(
        cls,
        api: B2Api,
        destination_bucket: Bucket,
    ):
        keys_to_purge = []
        if destination_bucket.replication is not None:
            current_destination_key_ids = destination_bucket.replication.source_to_destination_key_mapping.values()  # yapf: disable
        else:
            current_destination_key_ids = []
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
                bucket=destination_bucket,
                prefix=None,
            )
        return keys_to_purge, key

    def setup_source(
        self,
        source_bucket: Bucket,
        source_key: ApplicationKey,
        destination_bucket: Bucket,
        prefix: str | None = None,
        name: str | None = None,  #: name for the new replication rule
        priority: int = None,  #: priority for the new replication rule
        include_existing_files: bool = False,
    ) -> Bucket:
        if prefix is None:
            prefix = ""

        if source_bucket.replication:
            current_source_rules = source_bucket.replication.rules
            destination_configuration = source_bucket.replication.get_destination_configuration_as_dict(
            )
        else:
            current_source_rules = []
            destination_configuration = {}

        priority = self._get_priority_for_new_rule(
            current_source_rules,
            priority,
        )
        name = self._get_new_rule_name(
            current_source_rules,
            destination_bucket,
            name,
        )
        new_rr = ReplicationRule(
            name=name,
            priority=priority,
            destination_bucket_id=destination_bucket.id_,
            file_name_prefix=prefix,
            include_existing_files=include_existing_files,
        )
        new_replication_configuration = ReplicationConfiguration(
            source_key_id=source_key.id_,
            rules=current_source_rules + [new_rr],
            **destination_configuration,
        )
        return source_bucket.update(
            if_revision_is=source_bucket.revision,
            replication=new_replication_configuration,
        )

    @classmethod
    def _get_source_key(
        cls,
        source_bucket: Bucket,
        prefix: str,
        current_replication_configuration: ReplicationConfiguration,
    ) -> ApplicationKey:
        api = source_bucket.api

        if current_replication_configuration is not None:
            current_source_key = api.get_key(current_replication_configuration.source_key_id)
            do_create_key = cls._should_make_new_source_key(
                current_replication_configuration,
                current_source_key,
            )
            if not do_create_key:
                return current_source_key

        new_key = cls._create_source_key(
            name=source_bucket.name[:91] + '-replisrc',
            bucket=source_bucket,
            prefix=prefix,
        )
        return new_key

    @classmethod
    def _should_make_new_source_key(
        cls,
        current_replication_configuration: ReplicationConfiguration,
        current_source_key: ApplicationKey | None,
    ) -> bool:
        if current_replication_configuration.source_key_id is None:
            logger.debug('will create a new source key because no key is set')
            return True

        if current_source_key is None:
            logger.debug(
                'will create a new source key because current key "%s" was deleted',
                current_replication_configuration.source_key_id,
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
        bucket: Bucket,
        prefix: str | None = None,
    ) -> ApplicationKey:
        # in this implementation we ignore the prefix and create a full key, because
        # if someone would need a different (wider) key later, all replication
        # destinations would have to start using new keys and it's not feasible
        # from organizational perspective, while the prefix of uploaded files can be
        # restricted on the rule level
        prefix = None
        capabilities = cls.DEFAULT_SOURCE_CAPABILITIES
        return cls._create_key(name, bucket, prefix, capabilities)

    @classmethod
    def _create_destination_key(
        cls,
        name: str,
        bucket: Bucket,
        prefix: str | None = None,
    ) -> ApplicationKey:
        capabilities = cls.DEFAULT_DESTINATION_CAPABILITIES
        return cls._create_key(name, bucket, prefix, capabilities)

    @classmethod
    def _create_key(
        cls,
        name: str,
        bucket: Bucket,
        prefix: str | None = None,
        capabilities=tuple(),
    ) -> ApplicationKey:
        api: B2Api = bucket.api
        return api.create_key(
            capabilities=capabilities,
            key_name=name,
            bucket_id=bucket.id_,
            name_prefix=prefix,
        )

    @classmethod
    def _get_priority_for_new_rule(
        cls,
        current_rules: Iterable[ReplicationRule],
        priority: int | None = None,
    ):
        if priority is not None:
            return priority
        if current_rules:
            # ignore a case where the existing rrs need to have their priorities decreased to make space (max is 2**31-1)
            existing_priority = max(rr.priority for rr in current_rules)
            return min(existing_priority + cls.PRIORITY_OFFSET, cls.MAX_PRIORITY)
        return cls.DEFAULT_PRIORITY

    @classmethod
    def _get_new_rule_name(
        cls,
        current_rules: Iterable[ReplicationRule],
        destination_bucket: Bucket,
        name: str | None = None,
    ):
        if name is not None:
            return name
        existing_names = set(rr.name for rr in current_rules)
        suffixes = cls._get_rule_name_candidate_suffixes()
        while True:
            candidate = f'{destination_bucket.name}{next(suffixes)}'  # use := after dropping 3.7
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
    def _partion_bucket_path(cls, bucket_path: str) -> tuple[str, str]:
        bucket_name, _, path = bucket_path.partition('/')
        return bucket_name, path
