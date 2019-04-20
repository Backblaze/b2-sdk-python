#######################################
Semantic Versioning
#######################################

Excerpt (modified slightly) from `SemVer <https://semver.org/>`_, the version convention adopted by this project::

  Given a version number MAJOR.MINOR.PATCH, increment the:

   1. MAJOR version when you make incompatible changes [to the public API],
   2. MINOR version when you add functionality in a backwards-compatible manner, and
   3. PATCH version when you make backwards-compatible bug fixes.


#######################################
Version pinning
#######################################

b2sdk API is divided into two parts, *public* and *internal*. Please pay attention to which interface type you use

.. caution:: the stability of your application depends on correct pinning of versions


#######################################
Interfaces
#######################################

Public
======

Public interface consists of *public* members of the following modules:

.. autosummary::
  :nosignatures:

  b2sdk.api.B2Api
  b2sdk.bucket.Bucket
  b2sdk.exception
  b2sdk.file_version
  b2sdk.sync
  b2sdk.sync.exception
  b2sdk.account_info.abstract
  b2sdk.account_info.exception
  b2sdk.account_info.sqlite_account_info
  b2sdk.account_info.upload_url_pool

This should be enough for 99% of use cases, one can implement anything from a web application to a filesystem using just those.  Those modules will not change in a backwards-incompatible way between non-major versions.

.. hint:: If the current version of b2sdk is 4.5.6 and you only use the *public* interface,
  put this in your ``requirements.txt`` to be safe::

    b2sdk>=4.5.6,<5.0.0

.. note:: ``b2sdk.*._something`` and ``b2sdk.*.*._something``, having a name which begins with an underscore, are NOT considred public interface.


Internal
========

Things which sometimes might be necssary to use that are NOT considered public interface (and may change in a non-major version):

.. autosummary::
  :nosignatures:

  b2sdk.session.B2Session
  b2sdk.raw_api.B2RawApi
  b2sdk.b2http.B2Http
  b2sdk.transferer

.. note:: it is ok for you to use those (better that, than copying our source files!), however if you do, please pin your dependencies to *middle* version.

.. hint:: If the current version of b2sdk is 4.5.6 and you use the *internal* interface,
  put this in your requirements.txt::

    b2sdk>=4.5.6,<4.6.0
