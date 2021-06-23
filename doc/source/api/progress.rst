Progress reporters
==========================================

.. note::
   Concrete classes described in this chapter implement methods defined in ``AbstractProgressListener``

.. todo::
   improve documentation of progress reporters

   include info about sync progress

.. autoclass:: b2sdk.v2.AbstractProgressListener
   :inherited-members:
   :members:

.. autoclass:: b2sdk.v2.TqdmProgressListener
   :no-members:

.. autoclass:: b2sdk.v2.SimpleProgressListener
   :no-members:

.. autoclass:: b2sdk.v2.DoNothingProgressListener
   :no-members:

.. autoclass:: b2sdk.v2.ProgressListenerForTest
   :no-members:

.. autofunction:: b2sdk.v2.make_progress_listener

