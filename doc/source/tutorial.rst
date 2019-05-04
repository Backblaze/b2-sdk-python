#########################################
Tutorial
#########################################

The first object that you need to create when using **b2sdk**, is ``AccountInfo``. It will hold information about access keys, tokens etc. Using that, we'll be able to create a ``B2Api`` object to manage a B2 account.

There are a few implementations of ``AccountInfo`` interface provided in **b2sdk**:

.. autosummary::
   :nosignatures:

   b2sdk.v1.AbstractAccountInfo
   b2sdk.v1.InMemoryAccountInfo
   b2sdk.v1.SqliteAccountInfo
   b2sdk.v1.UrlPoolAccountInfo

.. note::
   :ref:`AccountInfo` section provides guidance for choosing the correct AccountInfo class for your application.

in the tutorial we will use :py:class:`InMemoryAccountInfo`:

.. code-block:: python

    >>> from b2sdk.v1 import InMemoryAccountInfo
    >>> info = InMemoryAccountInfo()  # store credentials, tokens and cache in memory


With the ``info`` object in hand, we can now proceed to creating a ``B2Api`` object:

.. code-block:: python

    >>> from b2sdk.v1 import B2Api
    >>> api = B2Api(info)

To find out more about API object initialization, see :meth:`b2sdk.v1.B2Api.__init__`.

B2Api allows for account operations, such as:

.. currentmodule:: b2sdk.v1.B2Api

.. autosummary::
   :nosignatures:

   authorize_account
   create_bucket
   delete_bucket
   list_buckets
   get_bucket_by_name
   create_key
   list_keys
   delete_key
   download_file_by_id
   list_parts
   cancel_large_file

to find out more, see :class:`b2sdk.v1.B2Api`.
