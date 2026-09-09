Exceptions
==========

Every error raised by **b2sdk** derives from :class:`b2sdk.v3.exception.B2Error`, so catching that one
class is enough to catch anything the library raises deliberately.

How to catch them
-----------------

The hierarchy is arranged so you can be as specific as you need. Catch a named class when you intend to
handle that exact condition, and fall back to a broader one otherwise:

.. code-block:: python

    >>> from b2sdk.v3.exception import B2Error, NonExistentBucket

    >>> try:
            bucket = b2_api.get_bucket_by_name('no-such-bucket')
        except NonExistentBucket:
            ...  # handle this specific case
        except B2Error as e:
            print(e)   # anything else the SDK raises

.. warning::
   The hierarchy may gain intermediate classes in a backwards-compatible release, so an exception's
   direct parent is not guaranteed to stay the same. Use ``isinstance`` and ``super()`` rather than
   naming a parent class explicitly. See :ref:`interface version compatibility
   <interface_version_compatibility>`.

Deciding whether to retry
-------------------------

Errors carry their own retry advice, so you rarely need to inspect status codes yourself:

* ``should_retry_http()`` is true when the failed HTTP call is worth repeating.
* ``should_retry_upload()`` is true when the upload should be retried after obtaining a fresh upload URL.
* ``retry_after_seconds`` is set when the server asked the client to wait before issuing more requests.
  When it is present, honour it rather than retrying immediately.

**b2sdk** already applies this logic internally for its own transfers, so these are relevant mainly if
you are driving the lower-level API yourself.

Two conveniences
----------------

Every error has a ``prefix`` property giving a readable name derived from the class name, which is what
makes the default messages presentable to end users. And
:func:`b2sdk.v3.exception.interpret_b2_error` turns a raw B2 error response into the appropriate
exception class, which is the function responsible for the hierarchy below being raised at all.

Reference
---------

.. automodule:: b2sdk.v3.exception
    :members:
    :undoc-members:
