.. todolist::

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

.. todo::
   delete doc/source/b2sdk?

.. todo::
   describe raw_simulator in detail

.. todo::
   fix list consistency style in "Why use b2sdk?", add links

When building an application which uses B2 cloud, it is possible to implement an independent B2 API client, but using **b2sdk** allows for:

- reuse of code that is already written, with hundreds of unit tests
- use of **Syncronizer**, a high-performance, parallel rsync-like utility
- developer-friendly library :ref:`api version policy <semantic_versioning>` which guards your program against incompatible changes
- `B2 integration checklist <https://www.backblaze.com/b2/docs/integration_checklist.html>`_ is passed automatically
- **raw_simulator** makes it easy to mock the B2 cloud for unit testing purposes
- reporting progress of operations to an object of your choice
- exception hierarchy makes it easy to display informative messages to users
- interrupted transfers are automatically continued
- **b2sdk** has been developed for 3 years before it version 1.0.0 was released. It's stable and mature.


#########################################
Documentation index
#########################################

.. toctree::

   install
   tutorial
   quick_start
   server_side_encryption
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
