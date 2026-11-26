Errors and retries
##################

This guide describes SDK behavior checked at ``v2.13.5-70-g21151d60``.
The release baseline is ``v2.13.5``.
The checked revision includes unreleased changes after that release.
An installed release can have different retry behavior.

Catch SDK errors
================

Import exceptions from ``b2sdk.v3.exception``.
``B2Error`` is the base class for the SDK exception hierarchy.
Catch a specific exception before you catch ``B2Error``.
Local file operations and argument validation can also raise standard Python exceptions.

The exception families cover several kinds of failure:

* Authorization errors include ``Unauthorized`` and its subclass ``InvalidAuthToken``.
  ``NotAllowedByAppKeyError`` is the base class for application key restriction errors.
* Missing resource errors include ``FileNotPresent`` and ``NonExistentBucket``.
* Request and limit errors include ``BadRequest``, ``InvalidRange``, and ``StorageCapExceeded``.
* Temporary service and transport errors include ``ServiceError``, ``TooManyRequests``,
  and ``B2RequestTimeout``.
* Transfer errors include ``ChecksumMismatch`` and ``MaxRetriesExceeded``.

``interpret_b2_error`` maps server responses to exception classes.
The mapping uses the HTTP status, the B2 error code, and sometimes the message.
For example, HTTP 429 maps to ``TooManyRequests``.
HTTP 500 through 599 map to ``ServiceError``.
HTTP 408 maps to ``RequestTimeout``.
``B2RequestTimeout`` represents a client-side timeout instead of an HTTP 408 response.

The following function accepts an authorized ``Bucket`` object.
See the :doc:`tutorial` for account authorization and bucket selection.
The SDK performs its automatic retries before the function catches the final error.

.. code-block:: python

    from b2sdk.v3.exception import B2Error, MaxRetriesExceeded, Unauthorized


    def upload_greeting(bucket):
        try:
            return bucket.upload_bytes(b"Hello from B2\n", "greeting.txt")
        except MaxRetriesExceeded as error:
            print(f"Upload stopped after {error.limit} attempts.")
            raise
        except Unauthorized:
            print("Check the application key and its access permissions.")
            raise
        except B2Error:
            print("The SDK could not complete the upload.")
            raise

Retry signals
=============

``B2Error`` provides three retry signals:

* ``should_retry_http()`` tells the HTTP retry loop whether an error permits another attempt.
  The default is ``False``.
* ``should_retry_upload()`` tells the upload manager whether an error permits another upload attempt.
  The default is ``False``.
* ``retry_after_seconds`` supplies a server-requested delay when available.
  The default is ``None``.

``TransientErrorMixin`` makes both retry methods return ``True``.
``ServiceError``, ``RequestTimeout``, and ``B2RequestTimeout`` use this mixin.
``TooManyRequests`` permits HTTP retries but does not permit upload-manager retries.
``BadUploadUrl``, ``BrokenPipe``, and ``ConnectionReset`` permit upload-manager retries
but do not permit HTTP retries.

These signals do not guarantee another attempt.
The attempt limit, the request replay rules, and the location where the error occurs also matter.
For example, the upload manager checks a successful response's checksum outside its retry handler.
If that check raises ``ChecksumMismatch``, the error leaves the upload manager immediately.

You can inspect the signals without making a request:

.. code-block:: python

    from b2sdk.v3.exception import TooManyRequests

    error = TooManyRequests(retry_after_seconds="12")
    assert error.should_retry_http() is True
    assert error.should_retry_upload() is False
    assert error.retry_after_seconds == 12

HTTP retries
============

Attempt limits
--------------

``B2Http._translate_and_retry`` makes at most ``try_count`` attempts.
The count includes the first attempt.
A value below 1 raises ``ValueError``.
The HTTP helpers use these defaults:

.. list-table:: Default HTTP attempt limits
   :header-rows: 1
   :widths: 45 35 20

   * - Request helper
     - Constant
     - Attempts
   * - Data requests, including upload data
     - ``TRY_COUNT_DATA``
     - 20
   * - Download requests
     - ``TRY_COUNT_DOWNLOAD``
     - 20
   * - HEAD requests
     - ``TRY_COUNT_HEAD``
     - 5
   * - JSON POST requests
     - ``TRY_COUNT_OTHER``
     - 5

These are HTTP helper defaults, not a limit for a complete SDK operation.
An operation can make several requests.
Upload-manager attempts can also contain HTTP retries.

Before another attempt, the HTTP loop checks ``should_retry_http()``.
If the result is ``False``, the loop raises the error immediately.
The JSON POST replay rules below can also stop retries.
If the final attempt fails, the loop raises that attempt's translated exception.
HTTP retry exhaustion does not wrap the error in ``MaxRetriesExceeded``.

Backoff and Retry-After
-----------------------

Without a server-requested delay, the first retry waits 1 second.
After each wait, the loop multiplies the backoff by 1.5.
When the next backoff exceeds 64 seconds, the loop sets it to 64 seconds plus a random value
from 0 up to, but not including, 1 second.
The loop adds this jitter only when the backoff exceeds 64 seconds.

For ``TooManyRequests`` and ``ServiceError``, the SDK parses the ``Retry-After`` response header.
The parser accepts integer delay-seconds and clamps the delay to 0 through 300 seconds.
It ignores HTTP-date values and invalid values.
If ``retry_after_seconds`` is not ``None``, that delay replaces the backoff for the current wait.
The loop still updates the backoff for the next retry.
A ``Retry-After`` value does not override the retry checks or the attempt limit.

JSON POST replay rules
----------------------

A request can succeed on the server even when the client does not receive its response.
Repeating such a request can create another resource.
The SDK therefore limits automatic retries for JSON POST requests.

``_REPLAYABLE_JSON_POST_ENDPOINTS`` identifies Native API operations that the HTTP layer
can repeat after an ambiguous failure.
Examples include ``b2_list_file_names``, ``b2_get_file_info``, and ``b2_delete_file_version``.
For these operations, an error must still permit HTTP retries.

For create-style or unclassified JSON POST requests, ``_is_unapplied_error`` must also return ``True``.
This check accepts ``TooManyRequests`` and recognized connection-establishment failures
that occur before sending the request.
For example, ``b2_create_bucket``, ``b2_create_key``, ``b2_start_large_file``,
``b2_copy_file``, and ``b2_finish_large_file`` use this restriction.
For these operations, response timeouts, connection loss after sending, and HTTP 5xx responses
leave the HTTP retry loop immediately.
This includes HTTP 503 responses.

After an ambiguous failure, check server state before you repeat the operation in your application.
A retry flag alone does not prove that repeating a request is safe.

Upload retries
==============

The upload manager has a separate retry loop for small files and for individual large-file parts.
``MAX_UPLOAD_ATTEMPTS`` is 5.
Each loop can therefore make 1 initial attempt and up to 4 retries.
This limit is not a limit of 5 HTTP requests for the whole file.

If an upload attempt raises ``B2Error`` inside the retry handler, the upload manager checks
``should_retry_upload()``.
If the result is ``False``, the manager raises the error immediately.
Otherwise, the manager records the error and clears the bucket's cached upload URLs.
Examples of errors that permit another upload attempt include ``ServiceError``,
``B2RequestTimeout``, ``BadUploadUrl``, and ``Unauthorized``.
Some ``Unauthorized`` subclasses, such as ``EmailNotVerified`` and ``NoPaymentHistory``,
disable upload retries.

The session returns an upload URL and its authorization token to the pool only after a successful request.
After a failed small-file attempt, the next attempt obtains a fresh upload URL when the bucket pool is empty.
Large-file parts use a separate pool.
A part retry can take another pooled URL or request a new URL if none is available.
An upload retry does not always make a new upload-URL request to the server.

The upload-manager loop adds no delay of its own.
The HTTP layer can wait and retry within an upload attempt.
If all 5 upload attempts fail with errors that permit upload retries, the manager raises
``MaxRetriesExceeded``.
Its ``limit`` attribute holds the attempt limit.
Its ``exception_info_list`` attribute holds the errors recorded by the upload retry loop.

See the :doc:`exception reference <api/exception>` for individual exception classes.
