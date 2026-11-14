.. _replication:

########################
Replication
########################

Replication asks B2 to copy files from one bucket to another automatically, as they are uploaded. The
bucket being copied *from* is the **source**; the bucket being copied *to* is the **destination**. One
bucket may be both at once, and a source bucket may replicate to several destinations under different
rules.

Replication is configured on the buckets, not on individual files: you attach a
:class:`b2sdk.v3.ReplicationConfiguration` to each side, and B2 does the copying. The SDK's job is to
help you write that configuration, create the application keys it needs, and inspect the outcome.

.. note::
   Replication only applies to files uploaded *after* a rule starts matching them. To bring across
   files that already exist, set ``include_existing_files=True`` on the rule.

**********************
What replication needs
**********************

Both sides need an application key, and the two sides need different capabilities:

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Side
     - Capabilities
   * - Source
     - ``readFiles``, ``readFileLegalHolds``, ``readFileRetentions``
   * - Destination
     - ``writeFiles``, ``writeFileLegalHolds``, ``writeFileRetentions``, ``deleteFiles``

The source bucket's configuration records the id of the source key. The destination bucket's
configuration maps each incoming source key id to the destination key that should be used for it - that
mapping is how a destination bucket authorises a particular source to write into it.

If the two buckets live in different accounts, you will need a separate :class:`b2sdk.v3.B2Api`
instance for each.

**********************
Setting up replication
**********************

Using the setup helper
======================

:class:`b2sdk.v3.ReplicationSetupHelper` is the supported path. It creates both application keys, adds
the rule to the source, and registers the key mapping on the destination:

.. code-block:: python

    >>> from b2sdk.v3 import ReplicationSetupHelper

    >>> source_bucket = b2_api.get_bucket_by_name('source-bucket')
    >>> destination_bucket = b2_api.get_bucket_by_name('destination-bucket')

    >>> rsh = ReplicationSetupHelper()
    >>> source_bucket, destination_bucket = rsh.setup_both(
            source_bucket=source_bucket,
            destination_bucket=destination_bucket,
            name='my-rule',
            prefix='folder/',
        )

It returns the two updated buckets, so use the returned objects rather than the ones you passed in.

The keys it creates are named after the buckets, suffixed ``-replisrc`` and ``-replidst``. If a rule is
already present, the helper places the new one ahead of the existing rules rather than overwriting
them.

The two halves are also available separately, which is what you need when the buckets are on different
accounts - run :meth:`~b2sdk.v3.ReplicationSetupHelper.setup_destination` against the destination
account first, then :meth:`~b2sdk.v3.ReplicationSetupHelper.setup_source` against the source account
with the resulting key.

Writing the configuration by hand
=================================

When you need full control, build the configuration yourself and pass it to
:meth:`b2sdk.v3.Bucket.update` (or to :meth:`b2sdk.v3.B2Api.create_bucket` for a new bucket). You are
then responsible for creating the application keys with the capabilities listed above.

.. code-block:: python

    >>> from b2sdk.v3 import ReplicationConfiguration, ReplicationRule

    >>> source_bucket.update(
            replication=ReplicationConfiguration(
                rules=[
                    ReplicationRule(
                        destination_bucket_id=destination_bucket.id_,
                        name='my-rule',
                        file_name_prefix='folder/',
                    ),
                ],
                source_key_id=source_key.id_,
            ),
        )

    >>> destination_bucket.update(
            replication=ReplicationConfiguration(
                source_to_destination_key_mapping={
                    source_key.id_: destination_key.id_,
                },
            ),
        )

A few constraints are enforced when the objects are constructed, so you will hear about mistakes before
a request is sent:

* a rule ``name`` must match ``[a-zA-Z0-9_-]`` and be at most 64 characters;
* ``destination_bucket_id`` is required;
* ``priority`` must be between 1 and 2\ :sup:`31`\ -1, and defaults to 128 - lower numbers are
  evaluated first, and the first matching rule wins;
* a configuration that has ``rules`` must also have a ``source_key_id``.

Inspecting what is configured
=============================

``bucket.replication`` is either ``None`` or a :class:`b2sdk.v3.ReplicationConfiguration`. Because one
bucket can be both ends of a replication relationship, ask which role it plays:

.. code-block:: python

    >>> bucket.replication.is_source
    True
    >>> bucket.replication.is_destination
    False
    >>> [rule.name for rule in bucket.replication.rules]
    ['my-rule']

********************
Checking on progress
********************

Per-file status
===============

A file version carries a :class:`b2sdk.v3.ReplicationStatus`, which is one of:

.. list-table::
   :header-rows: 1
   :widths: 18 82

   * - Status
     - Meaning
   * - ``PENDING``
     - On the source, and not yet copied to every destination.
   * - ``COMPLETED``
     - On the source, and copied successfully.
   * - ``FAILED``
     - On the source, and B2 will not retry. Needs investigation.
   * - ``REPLICA``
     - This file *is* a copy - the status you see on the destination side.

A source file whose status stays ``PENDING`` is usually still in flight. ``FAILED`` is terminal, and the
common causes are a key that no longer has the required capabilities, a destination key mapping that
does not match the source key in use, or a file the destination refuses because of its own lock
settings.

Monitoring a rule
=================

:class:`b2sdk.v3.ReplicationMonitor` walks one rule and reports what it finds on both sides:

.. code-block:: python

    >>> from b2sdk.v3 import ReplicationMonitor

    >>> rule = source_bucket.replication.rules[0]
    >>> monitor = ReplicationMonitor(bucket=source_bucket, rule=rule)
    >>> report = monitor.scan()

The monitor scans the source and, by default, the matching files on the destination so it can compare
them. Pass ``scan_destination=False`` to look at the source alone, which is much cheaper.

Two optional arguments matter in practice. ``destination_api`` takes a second
:class:`b2sdk.v3.B2Api`, and is required when the destination bucket is on another account; omit it and
the source bucket's own API is used. ``scan_policies_manager`` takes a
:class:`b2sdk.v3.ScanPoliciesManager`, letting you skip files you do not care about.

The monitor validates its arguments on construction: it raises ``ValueError`` if the bucket has no
replication configuration at all, or if the rule you passed does not belong to that bucket's
configuration.

Reading the report
==================

:meth:`~b2sdk.v3.ReplicationMonitor.scan` returns a :class:`b2sdk.v3.ReplicationReport`. Rather than a
flat list of files, it groups them: ``counter_by_status`` counts how many files share each distinct
:class:`b2sdk.v3.ReplicationScanResult`, and ``samples_by_status_first`` and ``samples_by_status_last``
hold the first and last example seen for each, so you have something concrete to go and look at.

.. code-block:: python

    >>> for result, count in report.counter_by_status.most_common():
            print(count, result.source_replication_status, result.destination_replication_status)

A scan result describes a source/destination *pair*, and the fields fall into three groups:

* what the source file looks like - ``source_replication_status``, ``source_has_hide_marker``,
  ``source_encryption_mode``, ``source_has_large_metadata``, ``source_has_file_retention``,
  ``source_has_legal_hold``;
* what the destination file looks like - ``destination_replication_status``;
* how the two compare - ``metadata_differs``, ``hash_differs``.

Any of these may be ``None``, which means "not known" rather than "false" - typically because only one
side of the pair exists, or because the destination was not scanned.

Interpreting partial results
============================

A scan is a snapshot of a system that is still moving, so mixed results are normal rather than a sign of
failure:

* **A destination file is missing entirely.** The destination fields are ``None``. Expected while a file
  is still ``PENDING``; worth investigating once the source reads ``COMPLETED``.
* **``hash_differs`` is true.** The two sides hold different content. This is briefly normal if the
  source file was replaced while the scan was running, and a real problem otherwise.
* **``metadata_differs`` is true.** The content matches but the file info does not. Large metadata is the
  usual cause - see ``source_has_large_metadata``, since B2 will not replicate metadata beyond a size
  limit.
* **``source_has_legal_hold`` or ``source_has_file_retention`` is set.** Copying these requires the
  ``*FileLegalHolds`` and ``*FileRetentions`` capabilities on *both* keys. Keys created by hand without
  them are a common cause of ``FAILED``.

.. note::
   Only the latest version of each file is inspected. Earlier versions are not represented in a report,
   so a clean scan is not a statement about a bucket's whole version history.

For the full signatures of everything above, see :ref:`replication_setting`, :ref:`replication_setup`,
:ref:`replication_monitoring` and :ref:`replication_types`.
