########
Glossary
########

.. glossary::

    absoluteMinimumPartSize
      The smallest large file part size, as indicated during authorization process by the server (in 2019 it used to be ``5MB``, but the server can set it dynamincally)

    account ID
      An identifier of the B2 account (not login). Looks like this: ``4ba5845d7aaf``.

    application key ID
      Since every :term:`account ID` can have multiple access keys associated with it, the keys need to be distinguished from each other. :term:`application key ID` is an identifier of the access key. There are two types of keys: :term:`master application key` and :term:`non-master application key`.

    application key
      The secret associated with an :term:`application key ID`, used to authenticate with the server. Looks like this: ``N2Zug0evLcHDlh_L0Z0AJhiGGdY`` or ``0a1bce5ea463a7e4b090ef5bd6bd82b851928ab2c6`` or ``K0014pbwo1zxcIVMnqSNTfWHReU/O3s``

    b2sdk version
      Looks like this: ``v1.0.0`` or ``1.0.0`` and makes version numbers meaningful. See :ref:`Pinning versions <semantic_versioning>` for more details.

    b2sdk interface version
      Looks like this: ``v2`` or ``b2sdk.v2`` and makes maintaining backward compatibility much easier. See :ref:`interface versions <interface_versions>` for more details.

    master application key
      This is the first key you have access to, it is available on the B2 web application. This key has all capabilities, access to all :term:`buckets<bucket>`, and has no file prefix restrictions or expiration. The :term:`application key ID` of the master application key is equal to :term:`account ID`.

    non-master application key
      A key which can have restricted capabilities, can only have access to a certain :term:`bucket` or even to just part of it. See `<https://www.backblaze.com/b2/docs/application_keys.html>`_ to learn more. Looks like this: ``0014aa9865d6f0000000000b0``

    bucket
      A container that holds files. You can think of buckets as the top-level folders in your B2 Cloud Storage account. There is no limit to the number of files in a bucket, but there is a limit of 100 buckets per account. See `<https://www.backblaze.com/b2/docs/buckets.html>`_ to learn more.
