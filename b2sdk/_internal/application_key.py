######################################################################
#
# File: b2sdk/_internal/application_key.py
#
# Copyright 2021 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################
from __future__ import annotations


class BaseApplicationKey:
    """Common methods for ApplicationKey and FullApplicationKey."""

    def __init__(
        self,
        key_name: str,
        application_key_id: str,
        capabilities: list[str],
        account_id: str,
        expiration_timestamp_millis: int | None = None,
        bucket_id: str | None = None,
        name_prefix: str | None = None,
        options: list[str] | None = None,
    ):
        """
        :param key_name: name of the key, assigned by user
        :param application_key_id: key id, used to authenticate
        :param capabilities: list of capabilities assigned to this key
        :param account_id: account's id
        :param expiration_timestamp_millis: expiration time of the key
        :param bucket_id: if restricted to a bucket, this is the bucket's id
        :param name_prefix: if restricted to some files, this is their prefix
        :param options: reserved for future use
        """
        self.key_name = key_name
        self.id_ = application_key_id
        self.capabilities = capabilities
        self.account_id = account_id
        self.expiration_timestamp_millis = expiration_timestamp_millis
        self.bucket_id = bucket_id
        self.name_prefix = name_prefix
        self.options = options

    @classmethod
    def parse_response_dict(cls, response: dict):
        mandatory_args = {
            'key_name': response['keyName'],
            'application_key_id': response['applicationKeyId'],
            'capabilities': response['capabilities'],
            'account_id': response['accountId'],
        }

        optional_args = {
            'expiration_timestamp_millis': response.get('expirationTimestamp'),
            'bucket_id': response.get('bucketId'),
            'name_prefix': response.get('namePrefix'),
            'options': response.get('options'),
        }
        return {
            **mandatory_args,
            **{key: value
               for key, value in optional_args.items() if value is not None},
        }

    def has_capabilities(self, capabilities) -> bool:
        """ checks whether the key has ALL of the given capabilities """
        return len(set(capabilities) - set(self.capabilities)) == 0

    def as_dict(self):
        """Represent the key as a dict, like the one returned by B2 cloud"""
        mandatory_keys = {
            'keyName': self.key_name,
            'applicationKeyId': self.id_,
            'capabilities': self.capabilities,
            'accountId': self.account_id,
        }
        optional_keys = {
            'expirationTimestamp': self.expiration_timestamp_millis,
            'bucketId': self.bucket_id,
            'namePrefix': self.name_prefix,
            'options': self.options,
        }
        return {
            **mandatory_keys,
            **{key: value
               for key, value in optional_keys.items() if value is not None},
        }


class ApplicationKey(BaseApplicationKey):
    """Dataclass for storing info about an application key returned by delete-key or list-keys."""

    @classmethod
    def from_api_response(cls, response: dict) -> ApplicationKey:
        """Create an ApplicationKey object from a delete-key or list-key response (a parsed json object)."""
        return cls(**cls.parse_response_dict(response))


class FullApplicationKey(BaseApplicationKey):
    """Dataclass for storing info about an application key, including the actual key, as returned by create-key."""

    def __init__(
        self,
        key_name: str,
        application_key_id: str,
        application_key: str,
        capabilities: list[str],
        account_id: str,
        expiration_timestamp_millis: int | None = None,
        bucket_id: str | None = None,
        name_prefix: str | None = None,
        options: list[str] | None = None,
    ):
        """
        :param key_name: name of the key, assigned by user
        :param application_key_id: key id, used to authenticate
        :param application_key: the actual secret key
        :param capabilities: list of capabilities assigned to this key
        :param account_id: account's id
        :param expiration_timestamp_millis: expiration time of the key
        :param bucket_id: if restricted to a bucket, this is the bucket's id
        :param name_prefix: if restricted to some files, this is their prefix
        :param options: reserved for future use
        """
        self.application_key = application_key
        super().__init__(
            key_name=key_name,
            application_key_id=application_key_id,
            capabilities=capabilities,
            account_id=account_id,
            expiration_timestamp_millis=expiration_timestamp_millis,
            bucket_id=bucket_id,
            name_prefix=name_prefix,
            options=options,
        )

    @classmethod
    def from_create_response(cls, response: dict) -> FullApplicationKey:
        """Create a FullApplicationKey object from a create-key response (a parsed json object)."""
        return cls(**cls.parse_response_dict(response))

    @classmethod
    def parse_response_dict(cls, response: dict):
        result = super().parse_response_dict(response)
        result['application_key'] = response['applicationKey']
        return result

    def as_dict(self):
        """Represent the key as a dict, like the one returned by B2 cloud"""
        return {
            **super().as_dict(),
            'applicationKey': self.application_key,
        }
