.. _replication_monitoring:

Replication monitoring
======================

.. autoclass:: b2sdk.v3.ReplicationMonitor()
    :members:
    :exclude-members: source_folder, destination_folder
    :special-members: __init__

.. autoclass:: b2sdk.v3.ReplicationScanResult()
    :inherited-members:
    :members:

``ReplicationReport`` is a :class:`b2sdk.v3.CountAndSampleScanReport` whose results are
:class:`b2sdk.v3.ReplicationScanResult` instances. It groups scanned files by result rather than
listing them: ``counter_by_status`` counts the files sharing each result, while
``samples_by_status_first`` and ``samples_by_status_last`` hold the first and last example seen for
each. See :ref:`replication` for how to read one.
