Cache classes
===============================================

**b2sdk** caches the mapping between bucket name and bucket
id, so that the user of the library does not need to maintain
the mapping to call the api.


.. autoclass:: b2sdk.v1.AbstractCache
   :no-members:

.. autoclass:: b2sdk.v1.AuthInfoCache()
   :no-members:

.. autoclass:: b2sdk.v1.DummyCache()
   :no-members:

.. autoclass:: b2sdk.v1.InMemoryCache()
   :no-members:
