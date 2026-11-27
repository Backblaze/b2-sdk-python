Migrating from v2 to v3
#######################

This guide lists the differences between ``b2sdk.v2`` and ``b2sdk.v3``.
It covers the signatures of public methods and the behavior of the ``b2sdk.v2`` wrapper code.
The sections below list the differences found by comparing both layers.
See `How to check the differences yourself`_ to repeat the comparison.

Choose a version
================

Import names from one interface version.
To migrate, change the import and then apply the changes in this guide.

.. code-block:: python

    # before
    from b2sdk.v2 import B2Api

    # after
    from b2sdk.v3 import B2Api

The ``b2sdk.v2`` layer wraps ``b2sdk.v3`` and keeps the old signatures.
See :ref:`interface_versions` and :doc:`api_types` for the compatibility policy.

The examples in this guide run against the in-memory simulator.
The setup is the same for both layers.
Replace ``v3`` with ``v2`` in the import to run the ``b2sdk.v2`` form.

.. code-block:: python

    >>> from b2sdk.v3 import B2Api, B2HttpApiConfig, InMemoryAccountInfo, RawSimulator
    >>> api = B2Api(account_info=InMemoryAccountInfo(), api_config=B2HttpApiConfig(_raw_api_class=RawSimulator))
    >>> key_id, key = api.session.raw_api.create_account()

Authorize an account
====================

``B2Api.authorize_account`` changed the order of its arguments.

* ``b2sdk.v2`` takes ``(realm, application_key_id, application_key)``.
* ``b2sdk.v3`` takes ``(application_key_id, application_key, realm='production')``.

The ``b2sdk.v2`` method calls the ``b2sdk.v3`` method with keyword arguments.
The ``b2sdk.v3`` method does not reject the old order.
It treats ``'production'`` as the application key ID, and the authorization fails.

.. code-block:: python

    >>> # b2sdk.v2
    >>> api.authorize_account('production', key_id, key)

    >>> # b2sdk.v3
    >>> api.authorize_account(key_id, key)
    >>> api.authorize_account(key_id, key, 'production')

    >>> # the v2 order on b2sdk.v3
    >>> api.authorize_account('production', key_id, key)
    Traceback (most recent call last):
      ...
    b2sdk._internal.exception.InvalidAuthToken: Invalid authorization token. Server said: application key ID not valid (unauthorized)

To migrate, remove the ``realm`` argument from the front of the call.
Pass ``realm`` last, or omit it to use ``'production'``.
Use keyword arguments to avoid the problem.

Restrict an application key to buckets
======================================

The ``bucket_id`` argument became ``bucket_ids``.
It takes a list of bucket IDs instead of one bucket ID.
The change applies to these methods and classes:

* ``B2Api.create_key``
* ``ApplicationKey`` and ``FullApplicationKey``, including the ``B2Api.APPLICATION_KEY_CLASS`` and ``B2Api.FULL_APPLICATION_KEY_CLASS`` attributes
* ``B2Session.create_key``
* ``create_key`` in ``AbstractRawApi``, ``B2RawHTTPApi``, and ``RawSimulator``

The application key objects follow the same rename.
An ``ApplicationKey`` or ``FullApplicationKey`` object has a ``bucket_id`` attribute on ``b2sdk.v2``.
The same object has a ``bucket_ids`` attribute on ``b2sdk.v3``.
The ``b2sdk.v3`` object has no ``bucket_id`` attribute.

.. code-block:: python

    >>> bucket = api.create_bucket('my-bucket', 'allPrivate')

    >>> # b2sdk.v2
    >>> application_key = api.create_key(['listFiles'], 'my-key', bucket_id=bucket.id_)
    >>> application_key.bucket_id
    'bucket_0'

    >>> # b2sdk.v3
    >>> application_key = api.create_key(['listFiles'], 'my-key', bucket_ids=[bucket.id_])
    >>> application_key.bucket_ids
    ['bucket_0']

Each layer rejects the argument name of the other layer with a ``TypeError``.
To migrate, wrap the bucket ID in a list and rename the argument and the attribute.
Pass ``None`` for ``bucket_ids`` to create a key that is not restricted to buckets.

The ``B2Session.create_key`` method and the raw API ``create_key`` methods take the bucket IDs in the same position as before.
Pass a list of bucket IDs instead of one bucket ID.

List files in a folder
======================

The first argument of ``Bucket.ls`` changed its name and its default behavior.

* ``b2sdk.v2`` names the first argument ``folder_to_list``.
  It also takes ``folder_to_list_can_be_a_file`` and ``**kwargs``.
* ``b2sdk.v3`` names the first argument ``path``.
  It takes no ``folder_to_list_can_be_a_file`` argument and no ``**kwargs``.

The ``b2sdk.v2`` method adds ``/`` to ``folder_to_list`` when all of these conditions are true:

* ``folder_to_list_can_be_a_file`` is ``False``, which is the default.
* ``folder_to_list`` is not empty and does not end with ``/``.
* ``with_wildcard`` is ``False``.

Then the method lists only the files in that folder.
The ``b2sdk.v3`` method does not add ``/``.
For a path that does not end with ``/``, it first looks for a file with that exact name.
If the file exists, the method returns that file and does not list a folder.
If no file has that name, the method lists the files that have that folder prefix.

The example below uses a bucket that has a file named ``dir`` and a folder named ``dir/``.

.. code-block:: python

    >>> uploaded = [bucket.upload_bytes(b'x', name) for name in ['dir', 'dir/b.txt', 'dir/sub/c.txt']]
    >>> def names(entries):
    ...     return [(file_version.file_name, folder) for file_version, folder in entries]

    >>> # b2sdk.v2
    >>> names(bucket.ls(folder_to_list='dir'))
    [('dir/b.txt', None), ('dir/sub/c.txt', 'dir/sub/')]
    >>> names(bucket.ls(folder_to_list='dir', folder_to_list_can_be_a_file=True))
    [('dir', None)]

    >>> # b2sdk.v3
    >>> names(bucket.ls(path='dir'))
    [('dir', None)]
    >>> names(bucket.ls(path='dir/'))
    [('dir/b.txt', None), ('dir/sub/c.txt', 'dir/sub/')]

To migrate, follow these steps:

#. Rename ``folder_to_list`` to ``path``.
#. Remove ``folder_to_list_can_be_a_file``.
   The ``b2sdk.v3`` method always has the behavior that ``folder_to_list_can_be_a_file=True`` selects on ``b2sdk.v2``.
#. Add ``/`` to the end of every folder path.
   The ``/`` selects the folder, and it saves one API call.

Upload files
============

The ``b2sdk.v2`` upload methods accept ``*args`` and ``**kwargs``.
The ``b2sdk.v3`` upload methods name every parameter.
The ``b2sdk.v3`` methods ``Bucket.upload_bytes`` and ``Bucket.upload_local_file`` add these parameters after ``cache_control``, in this order:

#. ``expires``
#. ``content_disposition``
#. ``content_encoding``
#. ``content_language``

The ``b2sdk.v2`` methods pass ``*args`` and ``**kwargs`` on to the ``b2sdk.v3`` method.
Because of this, a ``b2sdk.v2`` call can already use the new parameters.
A ``b2sdk.v3`` call that has an argument the method does not name raises a ``TypeError``.

.. code-block:: python

    >>> file_version = bucket.upload_bytes(b'x', 'page.txt', content_disposition='attachment', expires='Wed, 21 Oct 2026 07:28:00 GMT')
    >>> file_version.file_info
    {'b2-expires': 'Wed, 21 Oct 2026 07:28:00 GMT', 'b2-content-disposition': 'attachment'}

The same call works on ``b2sdk.v2`` and on ``b2sdk.v3``.

The ``b2sdk.v2`` upload methods also accept ``file_infos`` as a deprecated name of ``file_info``.
They raise a ``DeprecationWarning`` when you use it.
The ``b2sdk.v3`` methods do not accept ``file_infos``.
To migrate, rename ``file_infos`` to ``file_info``.

.. code-block:: python

    >>> bucket.upload_bytes(b'x', 'a.txt', file_info={'author': 'me'}).file_info
    {'author': 'me'}
    >>> bucket.upload_bytes(b'x', 'b.txt', file_infos={'author': 'me'})
    Traceback (most recent call last):
      ...
    TypeError: Bucket.upload_bytes() got an unexpected keyword argument 'file_infos'. Did you mean 'file_info'?

The second call fails on ``b2sdk.v3`` only.

Lower-level upload methods
--------------------------

These methods accept ``cache_control``, ``*args``, and ``**kwargs`` on ``b2sdk.v2``:

* ``B2Session.upload_file``
* ``upload_file`` and ``get_upload_file_headers`` in ``AbstractRawApi``, ``B2RawHTTPApi``, and ``RawSimulator``
* ``BucketSimulator.upload_file``

The ``b2sdk.v3`` versions of these methods do not accept ``cache_control``, ``*args``, or ``**kwargs``.
The ``b2sdk.v2`` versions store ``cache_control`` in ``file_info`` under the key ``b2-cache-control``.
To migrate, put that key in ``file_info`` yourself.

.. code-block:: python

    >>> import hashlib, io
    >>> data = b'hello'
    >>> sha1 = hashlib.sha1(data).hexdigest()

    >>> # b2sdk.v2
    >>> api.session.upload_file(bucket.id_, 's.txt', 5, 'text/plain', sha1, {}, io.BytesIO(data), cache_control='max-age=60')['fileInfo']
    {'b2-cache-control': 'max-age=60'}

    >>> # b2sdk.v3
    >>> api.session.upload_file(bucket.id_, 's.txt', 5, 'text/plain', sha1, {'b2-cache-control': 'max-age=60'}, io.BytesIO(data))['fileInfo']
    {'b2-cache-control': 'max-age=60'}

Read the cache control of a file version
========================================

The ``FILE_VERSION_CLASS`` attribute of ``FileVersionFactory`` changed its constructor.
The ``b2sdk.v2`` class takes a ``cache_control`` argument.
If the argument is ``None``, the class reads the ``b2-cache-control`` key of ``file_info``.
The ``b2sdk.v3`` class does not take the argument.

The ``cache_control`` attribute of a ``FileVersion`` object works on both layers.
The ``b2sdk.v3`` object reads the attribute from the ``b2-cache-control`` key of ``file_info``.
To migrate code that builds a file version from a constructor call, remove the ``cache_control`` argument.
Put the value in ``file_info`` under ``b2-cache-control`` instead.

.. code-block:: python

    >>> file_version = bucket.upload_bytes(b'x', 'c.txt', cache_control='max-age=60')
    >>> file_version.cache_control
    'max-age=60'

Replace classes that exist only in v2
=====================================

Four classes are exported by ``b2sdk.v2`` and not by ``b2sdk.v3``.

.. list-table::
   :header-rows: 1

   * - ``b2sdk.v2`` name
     - What to use on ``b2sdk.v3``
   * - ``AbstractSyncPath``
     - ``AbstractPath``
   * - ``LocalSyncPath``
     - ``LocalPath``
   * - ``B2SyncPath``
     - ``B2Path``
   * - ``TempDir``
     - ``tempfile.TemporaryDirectory`` from the Python standard library

The first three names are aliases on ``b2sdk.v2``.
They refer to the same classes as the ``b2sdk.v3`` names.
Change only the name.

``TempDir`` is deprecated on ``b2sdk.v2``.
It raises a ``DeprecationWarning`` when you enter it.
Its ``__enter__`` method returns the path of a new temporary directory as a string.
The ``__exit__`` method removes the directory.
``tempfile.TemporaryDirectory`` returns a string path in the same way.

.. code-block:: python

    >>> import tempfile
    >>> with tempfile.TemporaryDirectory() as directory:
    ...     print(type(directory).__name__)
    str

How to check the differences yourself
=====================================

The layers change between releases.
Run the following script to list the differences in the installed version.
The script compares the parameter names of every public method of every class that both layers export.
It also lists the classes that only one layer exports.
It does not compare behavior.
Read the code under ``b2sdk/v2/`` to find behavior differences.

.. code-block:: python

    import inspect

    import b2sdk.v2 as v2
    import b2sdk.v3 as v3

    names_v2 = {name for name in dir(v2) if not name.startswith('_')}
    names_v3 = {name for name in dir(v3) if not name.startswith('_')}
    for name in sorted(names_v2 & names_v3):
        class_v2, class_v3 = getattr(v2, name), getattr(v3, name)
        if not (inspect.isclass(class_v2) and inspect.isclass(class_v3)):
            continue
        for method in sorted(m for m in dir(class_v2) if not m.startswith('_')):
            if not hasattr(class_v3, method):
                continue
            method_v2, method_v3 = getattr(class_v2, method), getattr(class_v3, method)
            if not (callable(method_v2) and callable(method_v3)):
                continue
            params_v2 = list(inspect.signature(method_v2).parameters)
            params_v3 = list(inspect.signature(method_v3).parameters)
            if params_v2 != params_v3:
                print(f'{name}.{method}\n  v2 {params_v2}\n  v3 {params_v3}')
    print('only v2:', sorted(names_v2 - names_v3))
    print('only v3:', sorted(names_v3 - names_v2))
