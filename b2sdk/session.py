######################################################################
#
# File: b2sdk/session.py
#
# Copyright 2019 Backblaze Inc. All Rights Reserved.
#
# License https://www.backblaze.com/using_b2_code.html
#
######################################################################

import functools

from b2sdk.exception import (InvalidAuthToken, Unauthorized)
from b2sdk.raw_api import ALL_CAPABILITIES, TokenType


class B2Session(object):
    """
        A *magic* facade that supplies the correct api_url and account_auth_token
        to methods of underlying raw_api and reauthorizes if necessary.
    """

    def __init__(self, api, raw_api):
        self._api = api  # for reauthorization
        self.raw_api = raw_api

    def __getattr__(self, name):
        f = getattr(self.raw_api, name)

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            auth_failure_encountered = False
            # A *magic* that will identify and generate the correct type of Url and token based on the decorator on the B2RawApi method.
            token_type = getattr(f, 'token_type', TokenType.API)
            # download_by_name uses different URLs
            url_factory = kwargs.pop('url_factory', self._api.account_info.get_api_url)
            while 1:
                try:
                    if token_type == TokenType.API:
                        api_url = url_factory()
                        account_auth_token = self._api.account_info.get_account_auth_token()
                        return f(api_url, account_auth_token, *args, **kwargs)
                    elif token_type == TokenType.UPLOAD_SMALL:
                        return self._upload_small(f, *args, **kwargs)
                    elif token_type == TokenType.UPLOAD_PART:
                        return self._upload_part(f, *args, **kwargs)
                    else:
                        assert False, 'token type is not supported'
                except InvalidAuthToken:
                    if not auth_failure_encountered:
                        auth_failure_encountered = True
                        reauthorization_success = self._api.authorize_automatically()
                        if reauthorization_success:
                            continue
                        # TODO: exception chaining could be added here
                        #       to help debug reauthorization failures
                    raise
                except Unauthorized as e:
                    raise self._add_app_key_info_to_unauthorized(e)

        return wrapper

    def _add_app_key_info_to_unauthorized(self, unauthorized):
        """
        Take an Unauthorized error and adds information from the application key
        about why it might have failed.
        """
        # What's allowed?
        allowed = self._api.account_info.get_allowed()
        capabilities = allowed['capabilities']
        bucket_name = allowed['bucketName']
        name_prefix = allowed['namePrefix']

        # Make a list of messages about the application key restrictions
        key_messages = []
        if set(capabilities) != set(ALL_CAPABILITIES):
            key_messages.append("with capabilities '" + ','.join(capabilities) + "'")
        if bucket_name is not None:
            key_messages.append("restricted to bucket '" + bucket_name + "'")
        if name_prefix is not None:
            key_messages.append("restricted to files that start with '" + name_prefix + "'")
        if not key_messages:
            key_messages.append('with no restrictions')

        # Make a new message
        new_message = unauthorized.message
        if new_message == '':
            new_message = 'unauthorized'
        new_message += ' for application key ' + ', '.join(key_messages)

        return Unauthorized(new_message, unauthorized.code)

    def _get_upload_data(self, bucket_id):
        """
        Take ownership of an upload URL / auth token for the bucket and
        return it.
        """
        account_info = self._api.account_info
        upload_url, upload_auth_token = account_info.take_bucket_upload_url(bucket_id)
        if None not in (upload_url, upload_auth_token):
            return upload_url, upload_auth_token

        response = self.get_upload_url(bucket_id)
        return response['uploadUrl'], response['authorizationToken']

    def _get_upload_part_data(self, file_id):
        """
        Make sure that we have an upload URL and auth token for the given bucket and
        return it.
        """
        account_info = self._api.account_info
        upload_url, upload_auth_token = account_info.take_large_file_upload_url(file_id)
        if None not in (upload_url, upload_auth_token):
            return upload_url, upload_auth_token

        response = self.get_upload_part_url(file_id)
        return response['uploadUrl'], response['authorizationToken']

    def _upload_small(self, f, bucket_id, file_id, *args, **kwargs):
        upload_url, upload_auth_token = self._get_upload_data(bucket_id)
        response = f(upload_url, upload_auth_token, *args, **kwargs)
        self._api.account_info.put_bucket_upload_url(bucket_id, upload_url, upload_auth_token)
        return response

    def _upload_part(self, f, bucket_id, file_id, *args, **kwargs):
        upload_url, upload_auth_token = self._get_upload_part_data(file_id)
        response = f(upload_url, upload_auth_token, *args, **kwargs)
        self._api.account_info.put_large_file_upload_url(file_id, upload_url, upload_auth_token)
        return response
