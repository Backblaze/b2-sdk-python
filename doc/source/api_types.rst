.. _semantic_versioning:

########################
API Types
########################

.. todo::
   write a few words about imports structured like ``b2sdk.vX.*``

Public interface
================

Public interface consists of **public** members of modules listed in :ref:`Public API <api_public>` section.
This should be used in 99% of use cases, it's enough to implement anything from a `console tool <https://github.com/Backblaze/B2_Command_Line_Tool>`_ to a `FUSE filesystem <https://github.com/sondree/b2_fuse>`_.

Those modules will not change in a backwards-incompatible way between non-major versions.

.. hint::
  If the current version of **b2sdk** is ``4.5.6`` and you only use the *public* interface,
  put this in your ``requirements.txt`` to be safe::

    b2sdk>=4.5.6,<5.0.0

.. note::
  ``b2sdk.*._something`` and ``b2sdk.*.*._something``, while having a name beginning with an underscore, are **NOT** considered public interface.


.. _internal_interface:

Internal interface
==================

Some rarely used features of B2 cloud are not implemented in **b2sdk**. Tracking usage of transactions and transferred data is a good example - if it is required,
additional work would need to be put into a specialized internal interface layer to enable tracking and reporting.

**b2sdk** maintainers are :ref:`very supportive <contributors_guide>` in case someone wants to contribute an additional feature. Please consider adding it to the sdk, so that more people can use it.
This way it will also receive our updates, unlike a private implementation which would not receive any updates unless you apply them manually (
but that's a lot of work and we both know it's not going to happen). In practice, an implementation can be either shared or quickly outdated. The license of **b2sdk** is very
permissive, but when considering whether to keep your patches private or public, please take into consideration the long-term cost of keeping up with a dynamic open-source project and/or
the cost of missing the updates, especially those related to performance and reliability (as those are being actively developed in parallel to documentation).

Internal interface modules are listed in :ref:`API Internal <api_internal>` section.

.. note::
  It is OK for you to use our internal interface (better that than copying our source files!), however, if you do, please pin your dependencies to **middle** version,
  as backwards-incompatible changes may be introduced in a non-major version.

  .. hint::
    If the current version of **b2sdk** is ``4.5.6`` and you are using the *internal* interface,
    put this in your requirements.txt::

      b2sdk>=4.5.6,<4.6.0
