.. _semantic_versioning:

#######################################
API types
#######################################

**b2sdk** API is divided into two parts, *public* and *internal*. Please pay attention to which interface type you use and pin the version range accordingly.

.. caution::
  The stability of your application depends on correct pinning of versions.


Public interface
================

Public interface consists of **public** members of modules listed in :ref:`API Public <api_public>` section

Those modules will not change in a backwards-incompatible way between non-major versions.

This should be used in 99% of use cases, it's enough to implement anything from a web application to a `FUSE filesystem <https://github.com/sondree/b2_fuse>`_.

.. hint::
  If the current version of **b2sdk** is 4.5.6 and you only use the *public* interface,
  put this in your ``requirements.txt`` to be safe::

    b2sdk>=4.5.6,<5.0.0

.. note::
  ``b2sdk.*._something`` and ``b2sdk.*.*._something``, having a name which begins with an underscore, are NOT considred public interface.


.. _internal_interface:

Internal interface
==================

Some rarely used features of B2 cloud are not implemented in **b2sdk**. Tracking usage of transactions and transferred data is a good example - if it is required,
additional work would need to be put into a specialized internal interface layer to enable tracking and reporting.

**b2sdk** maintainers are very supportive in case someone wants to contribute an additional feature. Please consider adding it to the sdk, so that it's centrally
supported (unlike your an implementation of your own, which would not receive updates).

Internal interface modules are listed in :ref:`API Internal <api_internal>` section.

.. note::
  It is ok for you to use our internal interface (better that, than copying our source files!), however if you do, please pin your dependencies to **middle** version
  as backwards-incompatible changes may be introduced in a non-major version.

  .. hint:: If the current version of **b2sdk** is 4.5.6 and you use the *internal* interface,
    put this in your requirements.txt::

      b2sdk>=4.5.6,<4.6.0
