.. _server_side_encryption:

########################
Server-Side Encryption
########################

***********************
Cloud
***********************
B2 cloud supports `Server-Side Encryption <https://www.backblaze.com/b2/docs/server_side_encryption.html>`_. All read
and write operations provided by **b2sdk** accept encryption settings as an optional argument. Not supplying this
argument means relying on bucket defaults - for **SSE-B2** and for no encryption. In case of **SSE-C**, providing a
decryption key is required for successful downloading and copying.

***
API
***
Methods and classes that require encryption settings all accept an `EncryptionSetting` object, which holds information
about present or desired encryption (mode, algorithm, key, key_id). Some, like copy operations, accept two sets of settings (for
source and for destination). Sync, however, accepts an `EncryptionSettingsProvider` object, which is an
`EncryptionSetting` factory, yielding them based on file metadata. For details, see

* :ref:`encryption_setting`
* :ref:`encryption_types`
* :ref:`encryption_provider`

******************************
High security: use unique keys
******************************
B2 cloud does not promote or discourage either reusing encryption keys or using unique keys for `SEE-C`.
In applications requiring enhanced security, using unique key per file is a good strategy. **b2sdk** follows a convention,
that makes managing such keys easier: `EncryptionSetting` holds a key identifier, aside from the key itself. This key
identifier is saved in the metadata of all files uploaded, created or copied via **b2sdk** methods using `SSE-C`,
under `sse_c_key_id` in `fileInfo`. This allows developers to create key managers that map those ids to keys, stored
securely in a file or a database. Implementing such managers, and linking them to :class:`b2sdk.v1.AbstractSyncEncryptionSettingsProvider`
implementations (necessary for using Sync) is outside of the scope of this library.

There is, however, a convention to such managers that authors of this library strongly suggest: if a manager needs to generate
a new key-key_id pair for uploading, it's best to commit this data to the underlying storage before commencing the upload.
The justification of such approach is: should the key-key_id pair be committed to permanent storage after completing an IO
operation, committing could fail after successfully upload the data. This data, however, is now just a random blob, that
can never be read, since the key to decrypting it is lost.

This approach comes an overhead: to download a file, its `fileInfo` has to be known. This means that fetching metadata
is required before downloading.

*********************************************
Moderate security: a single key with many ids
*********************************************
Should the application's infrastructure require a single key (or a limited set of keys) to be used in a bucket, authors of this
library recommend generating a unique key identifier for each file anyway (even though these unique identifiers all
point to the same key value). This obfuscates confidential metadata from malicious users, like which files are encrypted with
the same key, the total number of different keys, etc.
