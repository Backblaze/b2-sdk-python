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
from dataclasses import dataclass
from typing import ClassVar, Dict, List, Optional


@dataclass
class ReplicationRule:
    """
    Hold information about replication rule: destination bucket, priority,
    prefix and rule name.
    """

    destination_bucket_id: str
    replication_rule_name: str
    file_name_prefix: str = ''
    is_enabled: bool = True
    priority: int = 1

    REPLICATION_RULE_REGEX: ClassVar = re.compile(r'^[a-zA-Z0-9_\-]{1,64}$')

    def __post_init__(self):
        if not self.destination_bucket_id:
            raise ValueError('destination_bucket_id is required')

        # TODO
        # if not (1 < self.priority < 255):
        # raise ValueError()

        # TODO: file_name_prefix validation

        if not self.REPLICATION_RULE_REGEX.match(self.replication_rule_name):
            raise ValueError('replication_rule_name is invalid')

    def as_dict(self) -> dict:
        return {
            'destinationBucketId': self.destination_bucket_id,
            'fileNamePrefix': self.file_name_prefix,
            'isEnabled': self.is_enabled,
            'priority': self.priority,
            'replicationRuleName': self.replication_rule_name,
        }

    @classmethod
    def from_dict(cls, value_dict: dict) -> 'ReplicationRule':
        return cls(
            destination_bucket_id=value_dict['destinationBucketId'],
            file_name_prefix=value_dict['fileNamePrefix'],
            is_enabled=value_dict['isEnabled'],
            priority=value_dict['priority'],
            replication_rule_name=value_dict['replicationRuleName'],
        )


@dataclass
class ReplicationSourceConfiguration:
    """
    Hold information about bucket as replication source
    """

    replication_rules: List[ReplicationRule]
    source_application_key_id: str

    def __post_init__(self):
        if not self.replication_rules:
            raise ValueError("replication_rules must not be empty")

        if not self.source_application_key_id:
            raise ValueError("source_application_key_id must not be empty")

    def as_dict(self) -> dict:
        return {
            "replicationRules": [rule.as_dict() for rule in self.replication_rules],
            "sourceApplicationKeyId": self.source_application_key_id,
        }

    @classmethod
    def from_dict(cls, value_dict: dict) -> 'ReplicationSourceConfiguration':
        return cls(
            replication_rules=[
                ReplicationRule.from_dict(rule_dict)
                for rule_dict in value_dict['replicationRules']
            ],
            source_application_key_id=value_dict['sourceApplicationKeyId'],
        )


@dataclass
class ReplicationDestinationConfiguration:
    source_to_destination_key_mapping: Dict[str, str]

    def __post_init__(self):
        if not self.source_to_destination_key_mapping:
            raise ValueError("source_to_destination_key_mapping must not be empty")

        for source, destination in self.source_to_destination_key_mapping.items():
            if not source or not destination:
                raise ValueError("source_to_destination_key_mapping must not contain \
                                  empty keys or values: ({}, {})".format(source, destination))

    def as_dict(self) -> dict:
        return {
            'sourceToDestinationKeyMapping': self.source_to_destination_key_mapping,
        }

    @classmethod
    def from_dict(cls, value_dict: dict) -> 'ReplicationDestinationConfiguration':
        return cls(
            source_to_destination_key_mapping=value_dict['sourceToDestinationKeyMapping'],
        )


@dataclass
class ReplicationConfiguration:
    """
    Hold information about bucket replication
    """

    as_replication_source: Optional[ReplicationSourceConfiguration] = None
    as_replication_destination: Optional[ReplicationDestinationConfiguration] = None

    def __post_init__(self):
        if not self.as_replication_source and not self.as_replication_destination:
            raise ValueError(
                "Must provide either as_replication_source or as_replication_destination"
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
                            "isEnabled": true,
                            "priority": 1,
                            "replicationRuleName": "replication-us-west"
                        },
                        {
                            "destinationBucketId": "55f34d53a96a7ea284fb0719",
                            "fileNamePrefix": "",
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

        if self.as_replication_source:
            result['asReplicationSource'] = self.as_replication_source.as_dict()

        if self.as_replication_destination:
            result['asReplicationDestination'] = self.as_replication_destination.as_dict()

        return result

    @classmethod
    def from_bucket_dict(cls, bucket_dict: dict) -> Optional['ReplicationConfiguration']:
        """
        Returns ReplicationConfiguration for the given bucket dict retrieved from the api, or None if no replication configured.
        """
        replication_data = bucket_dict.get('replicationConfiguration')
        if replication_data is None:
            return

        return cls.from_dict(bucket_dict['replicationConfiguration'])

    @classmethod
    def from_dict(cls, value_dict: dict) -> 'ReplicationConfiguration':
        replication_source_dict = value_dict.get('asReplicationSource')
        as_replication_source = replication_source_dict and ReplicationSourceConfiguration.from_dict(replication_source_dict)

        replication_destination_dict = value_dict.get('asReplicationDestination')
        as_replication_destination = replication_destination_dict and ReplicationDestinationConfiguration.from_dict(replication_destination_dict)

        return cls(
            as_replication_source=as_replication_source,
            as_replication_destination=as_replication_destination,
        )
