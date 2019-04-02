##########
User guide
##########

.. _semver:

***************************************
Version pinning
***************************************

b2sdk is divided into three parts. Please pay attention to which group you use, as the stability of your application depends on correct pinning of versions.

++++++++++
Interfaces
++++++++++

Public
======

Public interface consists of *public* members of the following modules:

.. autosummary::
  :nosignatures:

  b2sdk.api.B2Api
  b2sdk.bucket.Bucket
  b2sdk.exception
  b2sdk.sync
  b2sdk.sync.exception
  b2sdk.account_info.abstract
  b2sdk.account_info.exception
  b2sdk.account_info.sqlite_account_info
  b2sdk.account_info.upload_url_pool
  b2sdk.transferer
  b2sdk.utils

Those will not change in a backwards-incompatible way between non-major versions. In other words, if you pin your dependencies to `>=x.0.0;<x+1.0.0`, everything should be ok.
In other words, if you pin your dependencies to

.. hint:: If the current version of b2sdk is 4.5.6 and you only use the public interfaces, put this in your requirements.txt::
  
  >=4.5.6;<5.0.0

.. note:: b2sdk.*._something and b2sdk.*.*._something, having a name which begins with an underscore, are NOT considred public interface.


Protected
=========

Things which sometimes might be necssary to use that are NOT considered public interface (and may change in a non-major version):

.. autosummary::
  :nosignatures:

  b2sdk.session.B2Session
  b2sdk.raw_api.B2RawApi
  b2sdk.b2http.B2Http

.. note:: it is ok for you to use those (better that, than copying our sources), however if you do, please pin your dependencies to middle version.

.. hint:: If the current version of b2sdk is 4.5.6 and you use the public and protected interfaces, put this in your requirements.txt::
  
    >=4.5.6;<4.6.0


Private
=======

If you need to use some of our private interfaces, pin your dependencies strictly.

.. hint:: If the current version of b2sdk is 4.5.6 and you use the private interface, put this in your requirements.txt::
  
  ==4.5.6

*************
Authorization
*************

Before you can use b2sdk, you need to prove who you are to the server. For that you will need to pass `account id` and `api token` to one of the authorization classes.

In case you are storing that information in a database or something, you can implement your own class by inheriting from AbstractAuthorization. Otherwise, use one of the classes included in b2sdk package:


InMemoryAccountInfo:

This is probably what your application should be using and also what we use in our tests.


SqliteAccountInfo:

this is what B2 CLI uses to authorize the user. Stores information in a local file.


B2Api
~~~~~

The "main" object that abstracts the communication with B2 cloud is B2Api. It lets you manage buckets and download files by id.

example


Bucket
~~~~~~

Bucket class abstracts the B2 bucket, which is essentially a namespace for objects.

The best way to transfer your files into a bucket and back, is to use *sync*.

If for some reason you cannot use sync, it is also possible to upload and download files directly into/from the bucket, using Bucket.upload_file and Bucket.download_by_name.

The Bucket object also contains a few methods to list the contents of the bucket and the metadata associated with the objects contained in it.

