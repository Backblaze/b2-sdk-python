######################################################################
#
# File: b2sdk/replication/setting.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import re

from builtins import classmethod
from dataclasses import dataclass, field
from typing import ClassVar, Dict, List, Optional


@dataclass
class ReplicationRule:
    """
    Hold information about replication rule: destination bucket, priority,
    prefix and rule name.
    """

    DEFAULT_PRIORITY: ClassVar[int] = 128

    destination_bucket_id: str
    name: str
    file_name_prefix: str = ''
    is_enabled: bool = True
    priority: int = DEFAULT_PRIORITY
    include_existing_files: bool = False

    REPLICATION_RULE_REGEX: ClassVar = re.compile(r'^[a-zA-Z0-9_\-]{1,64}$')
    MIN_PRIORITY: ClassVar[int] = 1
    MAX_PRIORITY: ClassVar[int] = 2147483647

    def __post_init__(self):
        if not self.destination_bucket_id:
            raise ValueError('destination_bucket_id is required')

        if not self.REPLICATION_RULE_REGEX.match(self.name):
            raise ValueError('replication rule name is invalid')

        if not (self.MIN_PRIORITY <= self.priority <= self.MAX_PRIORITY):
            raise ValueError(
                'priority should be within [%d, %d] interval' % (
                    self.MIN_PRIORITY,
                    self.MAX_PRIORITY,
                )
            )

    def as_dict(self) -> dict:
        return {
            'destinationBucketId': self.destination_bucket_id,
            'fileNamePrefix': self.file_name_prefix,
            'includeExistingFiles': self.include_existing_files,
            'isEnabled': self.is_enabled,
            'priority': self.priority,
            'replicationRuleName': self.name,
        }

    @classmethod
    def from_dict(cls, value_dict: dict) -> 'ReplicationRule':
        kwargs = {}
        for field_, protocolField in (
            ('destination_bucket_id', 'destinationBucketId'),
            ('name', 'replicationRuleName'),
            ('file_name_prefix', 'fileNamePrefix'),
            ('include_existing_files', 'includeExistingFiles'),
            ('is_enabled', 'isEnabled'),
            ('priority', 'priority'),
        ):
            value = value_dict.get(
                protocolField
            )  # refactor to := when dropping Python 3.7, maybe even dict expression
            if value is not None:
                kwargs[field_] = value
        return cls(**kwargs)


@dataclass
class ReplicationSourceConfiguration:
    """
    Hold information about bucket being a replication source
    """

    rules: List[ReplicationRule] = field(default_factory=list)
    source_application_key_id: Optional[str] = None

    def __post_init__(self):
        if self.rules and not self.source_application_key_id:
            raise ValueError("source_application_key_id must not be empty")

    def serialize_to_json_for_request(self) -> Optional[dict]:
        if self.rules or self.source_application_key_id:
            return self.as_dict()

    def as_dict(self) -> dict:
        return {
            "replicationRules": [rule.as_dict() for rule in self.rules],
            "sourceApplicationKeyId": self.source_application_key_id,
        }

    @classmethod
    def from_dict(cls, value_dict: dict) -> 'ReplicationSourceConfiguration':
        return cls(
            rules=[
                ReplicationRule.from_dict(rule_dict) for rule_dict in value_dict['replicationRules']
            ],
            source_application_key_id=value_dict['sourceApplicationKeyId'],
        )


@dataclass
class ReplicationDestinationConfiguration:
    source_to_destination_key_mapping: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        for source, destination in self.source_to_destination_key_mapping.items():
            if not source or not destination:
                raise ValueError(
                    "source_to_destination_key_mapping must not contain \
                     empty keys or values: ({}, {})".format(source, destination)
                )

    def serialize_to_json_for_request(self) -> Optional[dict]:
        if self.source_to_destination_key_mapping:
            return self.as_dict()

    def as_dict(self) -> dict:
        return {
            'sourceToDestinationKeyMapping': self.source_to_destination_key_mapping,
        }

    @classmethod
    def from_dict(cls, value_dict: dict) -> 'ReplicationDestinationConfiguration':
        return cls(source_to_destination_key_mapping=value_dict['sourceToDestinationKeyMapping'])


@dataclass
class ReplicationConfiguration:
    """
    Hold information about bucket replication configuration
    """

    as_replication_source: ReplicationSourceConfiguration = field(
        default_factory=ReplicationSourceConfiguration,
    )
    as_replication_destination: ReplicationDestinationConfiguration = field(
        default_factory=ReplicationDestinationConfiguration,
    )

    def serialize_to_json_for_request(self) -> dict:
        return self.as_dict()

    def as_dict(self) -> dict:
        """
        Represent the setting as a dict, for example:

        .. code-block:: python

            {
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

        """
        result = {}
        if self.as_replication_source is not None:
            result['asReplicationSource'
                  ] = self.as_replication_source.serialize_to_json_for_request()
        if self.as_replication_destination is not None:
            result['asReplicationDestination'
                  ] = self.as_replication_destination.serialize_to_json_for_request()
        return result

    @classmethod
    def from_dict(cls, value_dict: dict) -> 'ReplicationConfiguration':
        replication_source_dict = value_dict.get('asReplicationSource')
        as_replication_source = ReplicationSourceConfiguration.from_dict(
            replication_source_dict
        ) if replication_source_dict else ReplicationSourceConfiguration()

        replication_destination_dict = value_dict.get('asReplicationDestination')
        as_replication_destination = ReplicationDestinationConfiguration.from_dict(
            replication_destination_dict
        ) if replication_destination_dict else ReplicationDestinationConfiguration()

        return cls(
            as_replication_source=as_replication_source,
            as_replication_destination=as_replication_destination,
        )


@dataclass
class ReplicationConfigurationFactory:
    is_client_authorized_to_read: bool
    value: Optional[ReplicationConfiguration]

    @classmethod
    def from_bucket_dict(cls, bucket_dict: dict) -> Optional['ReplicationConfigurationFactory']:
        """
        Returns ReplicationConfigurationFactory for the given bucket dict
        retrieved from the api, or None if no replication configured.
        """
        replication_dict = bucket_dict.get('replicationConfiguration')
        if not replication_dict:
            return cls(
                is_client_authorized_to_read=True,
                value=ReplicationConfiguration(),
            )

        return cls.from_dict(replication_dict)

    @classmethod
    def from_dict(cls, value_dict: dict) -> 'ReplicationConfigurationFactory':
        if not value_dict['isClientAuthorizedToRead']:
            return cls(
                is_client_authorized_to_read=False,
                value=None,
            )

        replication_dict = value_dict['value']
        replication_configuration = ReplicationConfiguration.from_dict(replication_dict) \
            if replication_dict else ReplicationConfiguration()
        return cls(
            is_client_authorized_to_read=True,
            value=replication_configuration,
        )
