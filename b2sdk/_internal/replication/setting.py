######################################################################
#
# File: b2sdk/_internal/replication/setting.py
#
# Copyright 2022 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations

import re
from builtins import classmethod
from dataclasses import dataclass, field
from typing import ClassVar


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
    MAX_PRIORITY: ClassVar[int] = 2**31 - 1

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
    def from_dict(cls, value_dict: dict) -> ReplicationRule:
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
class ReplicationConfiguration:
    """
    Hold information about bucket replication configuration
    """
    # configuration as source:
    rules: list[ReplicationRule] = field(default_factory=list)
    source_key_id: str | None = None
    # configuration as destination:
    source_to_destination_key_mapping: dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        if self.rules and not self.source_key_id:
            raise ValueError("source_key_id must not be empty")

        for source, destination in self.source_to_destination_key_mapping.items():
            if not source or not destination:
                raise ValueError(
                    f"source_to_destination_key_mapping must not contain \
                     empty keys or values: ({source}, {destination})"
                )

    @property
    def is_source(self) -> bool:
        return bool(self.source_key_id)

    def get_source_configuration_as_dict(self) -> dict:
        return {
            'rules': self.rules,
            'source_key_id': self.source_key_id,
        }

    @property
    def is_destination(self) -> bool:
        return bool(self.source_to_destination_key_mapping)

    def get_destination_configuration_as_dict(self) -> dict:
        return {
            'source_to_destination_key_mapping': self.source_to_destination_key_mapping,
        }

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

        result = {
            'asReplicationSource':
                {
                    "replicationRules": [rule.as_dict() for rule in self.rules],
                    "sourceApplicationKeyId": self.source_key_id,
                } if self.is_source else None,
            'asReplicationDestination':
                {
                    'sourceToDestinationKeyMapping': self.source_to_destination_key_mapping,
                } if self.is_destination else None,
        }

        return result

    serialize_to_json_for_request = as_dict

    @classmethod
    def from_dict(cls, value_dict: dict) -> ReplicationConfiguration:
        source_dict = value_dict.get('asReplicationSource') or {}
        destination_dict = value_dict.get('asReplicationDestination') or {}

        return cls(
            rules=[
                ReplicationRule.from_dict(rule_dict)
                for rule_dict in source_dict.get('replicationRules', [])
            ],
            source_key_id=source_dict.get('sourceApplicationKeyId'),
            source_to_destination_key_mapping=destination_dict.get('sourceToDestinationKeyMapping')
            or {},
        )


@dataclass
class ReplicationConfigurationFactory:
    is_client_authorized_to_read: bool
    value: ReplicationConfiguration | None

    @classmethod
    def from_bucket_dict(cls, bucket_dict: dict) -> ReplicationConfigurationFactory:
        """
        Returns ReplicationConfigurationFactory for the given bucket dict
        retrieved from the api.
        """
        replication_dict = bucket_dict.get('replicationConfiguration') or {}
        value_dict = replication_dict.get('value') or {}

        return cls(
            is_client_authorized_to_read=replication_dict.get('isClientAuthorizedToRead', True),
            value=ReplicationConfiguration.from_dict(value_dict),
        )
