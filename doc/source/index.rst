.. todolist::

.. note:: **Event Notifications** feature is now in **Private Preview**. See https://www.backblaze.com/blog/announcing-event-notifications/ for details.

#########################################
Overview
#########################################

**b2sdk** is a client library for easy access to all of the capabilities of B2 Cloud Storage.

`B2 command-line tool <https://github.com/Backblaze/B2_Command_Line_Tool>`_ is an example of how it can be used
to provide command-line access to the B2 service, but there are many possible applications
(including `FUSE filesystems <https://github.com/sondree/b2_fuse>`_, storage backend drivers for backup applications etc).

#########################################
Why use b2sdk?
#########################################

When building an application which uses B2 cloud, it is possible to implement an independent B2 API client, but using **b2sdk** gives you:

- code that is already written and covered by hundreds of unit tests
- :ref:`Synchronizer <sync>`, a high-performance, parallel rsync-like utility
- :ref:`Replication <replication>` support, for keeping a second bucket up to date automatically
- a developer-friendly :ref:`api version policy <semantic_versioning>` that guards your program against incompatible changes
- automatic compliance with the `B2 integration checklist <https://www.backblaze.com/b2/docs/integration_checklist.html>`_
- :doc:`raw_simulator <api/internal/raw_simulator>`, which mocks the B2 cloud so you can unit test without network access
- :doc:`progress reporting <api/progress>` to an object of your choice
- an :doc:`exception hierarchy <api/exception>` that makes it easy to show users informative messages
- automatic continuation of interrupted transfers
- a stable and mature codebase, in development for years before its 1.0.0 release


#########################################
Documentation index
#########################################

.. toctree::

   install
   tutorial
   quick_start
   server_side_encryption
   replication
   advanced
   glossary
   api_types
   api_reference
   contributing


#########################################
Indices and tables
#########################################

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
