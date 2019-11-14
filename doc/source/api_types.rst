########################
About API interfaces
########################

.. _semantic_versioning:

*******************
Semantic versioning
*******************

**b2sdk** follows `Semantic Versioning <https://semver.org/>`_ policy, so in essence the version number is ``MAJOR.MINOR.PATCH`` (for example ``1.2.3``) and:

- we increase `MAJOR` version when we make **incompatible** API changes
- we increase `MINOR` version when we add functionality **in a backwards-compatible manner**, and
- we increase `PATCH` version when we make backwards-compatible **bug fixes** (unless someone relies on the undocumented behavior of a fixed bug)

Therefore when setting up **b2sdk** as a dependency, please make sure to match the version appropriately, for example you could put this in your ``requirements.txt`` to make sure your code is compatible with the ``b2sdk`` version your user will get from pypi::

    b2sdk>=1.0.0,<2.0.0

.. _interface_versions:

******************
Interface versions
******************

You might notice that the import structure provided in the documentation looks a little odd: ``from b2sdk.v1 import ...``.
The ``.v1`` part is used to keep the interface fluid without risk of breaking applications that use the old signatures.
With new versions, **b2sdk** will provide functions with signatures matching the old ones, wrapping the new interface in place of the old one. What this means for a developer using **b2sdk**, is that it will just keep working. We havealready deleted some legacy functions when moving from ``.v0`` to ``.v1``, providing equivalent wrappers to reduce the migration effort for applications using pre-1.0 versions of **b2sdk** to fixing imports.

It also means that **b2sdk** developers may change the interface in the future and will not need to maintain many branches and backport fixes to keep compatibility of for users of those old branches.

.. _interface_version_compatibility:

*******************************
Interface version compatibility
*******************************

A :term:`numbered interface<b2sdk interface version>` will not be exactly identical throughout its lifespan, which should not be a problem for anyone, however just in case, the acceptable differences that the developer must tolerate, are listed below.

Exceptions
==========

The exception hierarchy may change in a backwards compatible manner and the developer must anticipate it. For example, if ``b2sdk.v1.ExceptionC`` inherits directly from ``b2sdk.v1.ExceptionA``, it may one day inherit from ``b2sdk.v1.ExceptionB``, which in turn inherits from ``b2sdk.v1.ExceptionA``. Normally this is not a problem if you use ``isinstance()`` and ``super()`` properly, but your code should not call the constructor of a parent class by directly naming it or it might skip the middle class of the hierarchy (``ExceptionB`` in this example).

Extensions
==========

Even in the same interface version, objects/classes/enums can get additional fields and their representations such as ``to_dict()`` or ``__repr__`` (but not ``__str__``) may start to contain those fields.

Methods and functions can start accepting new **optional** arguments. New methods can be added to existing classes.

Performance
===========

Some effort will be put into keeping the performance of the old interfaces, but in rare situations old interfaces may end up with a slightly degraded performance after a new version of the library is released.
If performance target is absolutely critical to your application, you can pin your dependencies to the middle version (using ``b2sdk>=X.Y.0,<X.Y+1.0``) as **b2sdk** `will` increment the middle version when introducing a new interface version if the wrapper for the older interfaces is likely to affect performance.

****************
Public interface
****************

Public interface consists of **public** members of modules listed in :ref:`Public API <api_public>` section.
This should be used in 99% of use cases, it's enough to implement anything from a `console tool <https://github.com/Backblaze/B2_Command_Line_Tool>`_ to a `FUSE filesystem <https://github.com/sondree/b2_fuse>`_.

Those modules will generally not change in a backwards-incompatible way between non-major versions. Please see :ref:`interface version compatibility <interface_version_compatibility>` chapter for notes on what changes must be expected.

.. hint::
  If the current version of **b2sdk** is ``4.5.6`` and you only use the *public* interface,
  put this in your ``requirements.txt`` to be safe::

    b2sdk>=4.5.6,<5.0.0

.. note::
  ``b2sdk.*._something`` and ``b2sdk.*.*._something``, while having a name beginning with an underscore, are **NOT** considered public interface.

.. _internal_interface:

******************
Internal interface
******************

Some rarely used features of B2 cloud are not implemented in **b2sdk**. Tracking usage of transactions and transferred data is a good example - if it is required,
additional work would need to be put into a specialized internal interface layer to enable accounting and reporting.

**b2sdk** maintainers are :ref:`very supportive <contributors_guide>` in case someone wants to contribute an additional feature. Please consider adding it to the sdk, so that more people can use it.
This way it will also receive our updates, unlike a private implementation which would not receive any updates unless you apply them manually (
but that's a lot of work and we both know it's not going to happen). In practice, an implementation can be either shared or will quickly become outdated. The license of **b2sdk** is very
permissive, but when considering whether to keep your patches private or public, please take into consideration the long-term cost of keeping up with a dynamic open-source project and/or
the cost of missing the updates, especially those related to performance and reliability (as those are being actively developed in parallel to documentation).

Internal interface modules are listed in :ref:`API Internal <api_internal>` section.

.. note::
  It is OK for you to use our internal interface (better that than copying our source files!), however, if you do, please pin your dependencies to **middle** version,
  as backwards-incompatible changes may be introduced in a non-major version.

  Furthermore, it would be greatly appreciated if an issue was filed for such situations, so that **b2sdk** interface can be improved in a future version in order to avoid strict version pinning.

  .. hint::
    If the current version of **b2sdk** is ``4.5.6`` and you are using the *internal* interface,
    put this in your requirements.txt::

      b2sdk>=4.5.6,<4.6.0
