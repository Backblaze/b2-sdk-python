Test
~~~~

.. DANGER::
   Beware killer rabbits!

.. note:: This is a note admonition.
   This is the second line of the first paragraph.

   - The note contains all indented body elements
     following.
   - It includes this bullet list.

.. admonition:: And, by the way...

   You can make up your own admonition too.


.. title:: This is title

   block of text

.. admonition:: This is admonition

   block of text


.. warning:: This is warning

   block of text


.. tip:: This is tip

   block of text


.. note:: This is note

   block of text


.. important:: This is important

   block of text


.. hint:: This is hint

   block of text


.. error:: This is error

   block of text


.. danger:: This is danger

   block of text


.. caution:: This is caution

   block of text



.. attention:: This is attention

   block of text



.. _semver:
Types of interfaces and version pinning
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

b2sdk is divided into three parts. Please pay attention to which group you use, as the stability of your application depends on correct pinning of versions.

Public
~~~~~~

Public interface consists of *public* members of the following modules:
* b2sdk.api.B2Api
* b2sdk.bucket.Bucket
* b2sdk.exception
* b2sdk.sync
* b2sdk.sync.exception

and some of their dependencies:
* b2sdk.account_info.InMemoryAccountInfo
* b2sdk.account_info.SqliteAccountInfo
* b2sdk.transferer
* b2sdk.utils

Those will not change in a backwards-incompatible way between non-major versions. In other words, if you pin your dependencies to `>=x.0.0;<x+1.0.0`, everything should be ok.
In other words, if you pin your dependencies to
.. example:: If the current version of b2sdk is 4.5.6 and you only use the public interfaces, put this in your requirements.txt:
  
  >=4.5.6;<5.0.0

.. note:: b2sdk.*._something and b2sdk.*.*._something, having a name which begins with an underscore, are NOT considered public interface.


Protected
~~~~~~~~~

Things which sometimes might be necessary to use that are NOT considered public interface (and may change in a non-major version):
* B2Session
* B2RawHTTPApi
* B2Http

.. note:: it is ok for you to use those (better that, than copying our sources), however if you do, please pin your dependencies to middle version.

.. example:: If the current version of b2sdk is 4.5.6 and you use the public and protected interfaces, put this in your requirements.txt:
  
  >=4.5.6;<4.6.0

Private
~~~~~~~

If you need to use some of our private interfaces, pin your dependencies strictly.

.. example:: If the current version of b2sdk is 4.5.6 and you use the private interface, put this in your requirements.txt:
  
  ==4.5.6

Authorization
~~~~~~~~~~~~~

Before you can use b2sdk, you need to prove who you are to the server. For that you will need to pass `account id` and `api token` to one of the authorization classes.

In case you are storing that information in a database or something, you can implement your own class by inheriting from AbstractAuthorization. Otherwise, use one of the classes included in b2sdk package:


InMemoryAccountInfo:

This is probably what your application should be using and also what we use in our tests.


SqliteAccountInfo:

this is what B2 CLI uses to authorize the user. Stores information in a local file.


B2Api
~~~~

The "main" object that abstracts the communication with B2 cloud is B2Api. It lets you manage buckets and download files by id.

example


Bucket
~~~~~~

Bucket class abstracts the B2 bucket, which is essentially a namespace for objects.

The best way to transfer your files into a bucket and back, is to use *sync*.

If for some reason you cannot use sync, it is also possible to upload and download files directly into/from the bucket, using Bucket.upload_file and Bucket.download_by_name.

The Bucket object also contains a few methods to list the contents of the bucket and the metadata associated with the objects contained in it.

========
Tutorial
========

Account authorization
=====================

TODO

Bucket actions
==============

Create a bucket
---------------

TODO

Remove a bucket
---------------

TODO

List buckets
-------------

TODO

Update bucket info
------------------

TODO

File actions
============

Upload file
-----------

TODO

Download file
-------------

TODO

List files
----------

TODO

Get file meta information
-------------------------

TODO

Delete file
-----------

TODO

Cancel file operations
----------------------

TODO

Synchronization
===============

TODO

Account information
===================

TODO
