#########################################
Tutorial
#########################################

***************************
AccountInfo
***************************

``AccountInfo`` object holds information about access keys, tokens, upload urls, as well as a bucket id-name map.

It is the first object that you need to create to use **b2sdk**. Using ``AccountInfo``, we'll be able to create a ``B2Api`` object to manage a B2 account.

In the tutorial we will use :py:class:`b2sdk.v2.InMemoryAccountInfo`:

.. code-block:: python

    >>> from b2sdk.v2 import InMemoryAccountInfo
    >>> info = InMemoryAccountInfo()  # store credentials, tokens and cache in memory


With the ``info`` object in hand, we can now proceed to create a ``B2Api`` object.

.. note::
   :ref:`AccountInfo` section provides guidance for choosing the correct ``AccountInfo`` class for your application.

*********************
Account authorization
*********************

.. code-block:: python

    >>> from b2sdk.v2 import B2Api
    >>> b2_api = B2Api(info)
    >>> application_key_id = '4a5b6c7d8e9f'
    >>> application_key = '001b8e23c26ff6efb941e237deb182b9599a84bef7'
    >>> b2_api.authorize_account("production", application_key_id, application_key)

.. tip::
   Get credentials from B2 website

To find out more about account authorization, see :meth:`b2sdk.v2.B2Api.authorize_account`


***************************
B2Api
***************************

*B2Api* allows for account-level operations on a B2 account.

Typical B2Api operations
========================

.. currentmodule:: b2sdk.v2.B2Api

.. autosummary::
   :nosignatures:

   authorize_account
   create_bucket
   delete_bucket
   list_buckets
   get_bucket_by_name
   get_bucket_by_id
   create_key
   list_keys
   delete_key
   download_file_by_id
   list_parts
   cancel_large_file

.. code-block:: python

    >>> b2_api = B2Api(info)

to find out more, see :class:`b2sdk.v2.B2Api`.

The most practical operation on ``B2Api`` object is :meth:`b2sdk.v2.B2Api.get_bucket_by_name`.

*Bucket* allows for operations such as listing a remote bucket or transferring files.

***************************
Bucket
***************************

Initializing a Bucket
========================

Retrieve an existing Bucket
---------------------------

To get a ``Bucket`` object for an existing B2 Bucket:

.. code-block:: python

    >>> b2_api.get_bucket_by_name("example-mybucket-b2-1",)
    Bucket<346501784642eb3e60980d10,example-mybucket-b2-1,allPublic>

Create a new Bucket
------------------------

To create a bucket:

.. code-block:: python

    >>> bucket_name = 'example-mybucket-b2-1'
    >>> bucket_type = 'allPublic'  # or 'allPrivate'

    >>> b2_api.create_bucket(bucket_name, bucket_type)
    Bucket<346501784642eb3e60980d10,example-mybucket-b2-1,allPublic>

You can optionally store bucket info, CORS rules and lifecycle rules with the bucket. See :meth:`b2sdk.v2.B2Api.create_bucket` for more details.

.. note::
    Bucket name must be unique in B2 (across all accounts!). Your application should be able to cope with a bucket name collision with another B2 user.


Typical Bucket operations
=========================

.. currentmodule:: b2sdk.v2.Bucket

.. autosummary::
   :nosignatures:

   download_file_by_name
   upload_local_file
   upload_bytes
   ls
   hide_file
   delete_file_version
   get_download_authorization
   get_download_url
   update
   set_type
   set_info


To find out more, see :class:`b2sdk.v2.Bucket`.


***************************
Summary
***************************

You now know how to use ``AccountInfo``, ``B2Api`` and ``Bucket`` objects.

To see examples of some of the methods presented above, visit the :ref:`quick start guide <quick_start>` section.
