Account info and authorization
##############################

This guide describes SDK behavior checked at ``v2.13.5-74-gb0f204c9``.
An installed release may behave differently.

Choose a store
==============

Account info holds authorization data for a :class:`b2sdk.v3.B2Api` session.
It includes the account ID, application key ID and key, authorization token,
API and download URLs, realm, part sizes, and allowed capabilities and restrictions.
When no account info is supplied, ``B2Api`` uses ``SqliteAccountInfo``.

* :class:`b2sdk.v3.AbstractAccountInfo` defines the account-info interface. Use a
  concrete implementation or implement the interface for a custom store.
* :class:`b2sdk.v3.InMemoryAccountInfo` keeps account data and bucket name-to-ID
  mappings in memory. Use it when persistence between processes is not needed.
* :class:`b2sdk.v3.SqliteAccountInfo` stores account data and bucket name-to-ID
  mappings in a SQLite database. Use it to reuse stored account information across
  processes. The database holds credentials; protect the file like a secret.
* :class:`b2sdk.v3.StubAccountInfo` is a test stub. It keeps account data in memory,
  but its bucket-name lookup and bucket upload URL methods do not provide a cache.
  It has a large-file upload URL pool.
* :class:`b2sdk.v3.UrlPoolAccountInfo` is an abstract base for stores that manage
  bucket and large-file upload URL pools. It does not implement a complete store.

The in-memory and SQLite implementations inherit upload URL pools from
``UrlPoolAccountInfo``. These pools are in memory, including for SQLite.
A separate ``cache`` object controls the bucket name cache used by ``B2Api``;
it need not be the account info store.

SQLite location
===============

For a store without a profile, a truthy ``file_name`` takes precedence over the
``B2_ACCOUNT_INFO`` environment variable. If neither is supplied, an existing
``~/.b2_account_info`` takes precedence. Otherwise, on Linux and BSD, the SDK uses
``$XDG_CONFIG_HOME/b2/account_info``; if ``XDG_CONFIG_HOME`` is empty, it uses
``~/.config/b2/account_info``. If ``XDG_CONFIG_HOME`` is set on another platform,
the SDK also uses that location. Otherwise it uses ``~/.b2_account_info``.
Read ``store.filename`` for the resolved path instead of assuming a location.

Authorize an account
====================

In v3, call ``B2Api.authorize_account(application_key_id, application_key, realm='production')``.
The call contacts the service and saves the response and key in account info.
Later SDK calls can use the saved authorization token and URLs without a separate
explicit authorization call. The session can reauthorize when needed.

The following example uses the simulator and does not contact B2:

.. code-block:: python

    from b2sdk.v3 import B2Api, B2HttpApiConfig, InMemoryAccountInfo, RawSimulator

    store = InMemoryAccountInfo()
    api = B2Api(account_info=store, api_config=B2HttpApiConfig(_raw_api_class=RawSimulator))
    key_id, key = api.session.raw_api.create_account()
    api.authorize_account(key_id, key)
    bucket = api.create_bucket('example-bucket', 'allPrivate')
    assert api.get_bucket_by_name('example-bucket').id_ == bucket.id_

Restricted application keys
===========================

An application key can limit capabilities, buckets, or a file name prefix.
The service enforces the key's allowed operations. For a bucket-restricted key,
``list_buckets`` requires a permitted bucket name or ID. Bucket lookups check the
allowed bucket names or IDs and can raise ``RestrictedBucket`` for a mismatch.
If an authorized key refers to a bucket that no longer has a name, authorization
raises ``RestrictedBucketMissing``.
