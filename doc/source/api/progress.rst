Progress reporters
==================

**b2sdk** reports progress through two separate mechanisms, and which one you want depends on what you
are doing.

For **a single transfer** - one upload, download or copy - pass a *progress listener* to the method.
Every such method accepts a ``progress_listener`` argument, and the classes on this page are the
available implementations.

For **a whole sync operation**, which scans folders and then transfers many files, a single listener is
not enough. Sync instead takes a :class:`b2sdk.v3.SyncReport`, which tracks the scan and comparison
phases as well as the transfers. See :ref:`sync` for how to use it.

Choosing a listener
-------------------

.. list-table::
   :header-rows: 1
   :widths: 34 66

   * - Class
     - When to use it
   * - :class:`b2sdk.v3.TqdmProgressListener`
     - Interactive use. Draws a progress bar, and needs the ``tqdm`` package installed.
   * - :class:`b2sdk.v3.SimpleProgressListener`
     - Interactive use without ``tqdm``. Prints plain percentages.
   * - :class:`b2sdk.v3.DoNothingProgressListener`
     - Non-interactive use - servers, scripts, tests. Discards everything.
   * - :class:`b2sdk.v3.ProgressListenerForTest`
     - Test support. Records the calls it receives so they can be asserted on.

If you do not want to choose, :func:`b2sdk.v3.make_progress_listener` picks for you: it returns
``DoNothingProgressListener`` when ``quiet`` is set, ``TqdmProgressListener`` when ``tqdm`` is
importable, and ``SimpleProgressListener`` otherwise.

.. code-block:: python

    >>> from b2sdk.v3 import make_progress_listener

    >>> with make_progress_listener('uploading backup.tar', quiet=False) as listener:
            bucket.upload_local_file(
                local_file='backup.tar',
                file_name='backup.tar',
                progress_listener=listener,
            )

Writing your own
----------------

Subclass :class:`b2sdk.v3.AbstractProgressListener` and implement ``set_total_bytes`` and
``bytes_completed``. Two details of the contract are easy to get wrong:

* ``set_total_bytes`` is always called before the listener is entered, but **may be called again** if a
  transfer is retried.
* ``bytes_completed`` receives a **running total, not a delta**, and that total **can go down**, because
  a failed transfer restarts from the beginning. Do not accumulate the values you are given.

Listeners are context managers, and ``close`` must be called exactly once - it asserts if called twice -
so prefer a ``with`` block over calling it yourself.

.. note::
   The concrete classes on this page all implement the methods defined by
   :class:`b2sdk.v3.AbstractProgressListener`.

.. autoclass:: b2sdk.v3.AbstractProgressListener
   :inherited-members:
   :members:

.. autoclass:: b2sdk.v3.TqdmProgressListener
   :no-members:

.. autoclass:: b2sdk.v3.SimpleProgressListener
   :no-members:

.. autoclass:: b2sdk.v3.DoNothingProgressListener
   :no-members:

.. autoclass:: b2sdk.v3.ProgressListenerForTest
   :no-members:

.. autofunction:: b2sdk.v3.make_progress_listener
