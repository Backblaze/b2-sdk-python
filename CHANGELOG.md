# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
* Require `typing_extensions` on Python 3.11 (already required on earlier versinons) for better compatibility with pydantic v2
* Fix `RawSimulator` handling of `cache_control` parameter during tests.

## [1.22.1] - 2023-07-24

### Fixed
* Fix regression in dir exclusion patterns introduced in 1.22.0


## [1.22.0] - 2023-07-21

### Added
* Declare official support of Python 3.12
* Improved `lifecycle_rules` argument type annotations

### Deprecated
* Deprecate `file_infos` argument. Use `file_info` instead. Old argument name won't be supported in v3.

### Changed
* `version_utils` decorators now ignore `current_version` parameter to better fit `apiver` needs

### Fixed
* Circular symlinks no longer cause infinite loops when syncing a folder
* Fix crash on upload retry with unbound data source

### Infrastructure
* Remove unsupported PyPy versions (3.7, 3.8) from tests matrix and add PyPy 3.9 & 3.10 instead
* Replaced `pyflakes` with `ruff` for linting
* Refactored logic for resuming large file uploads to unify code paths, correct inconsistencies, and enhance configurability (#381)
* Automatically set copyright date when generating the docs
* Use modern type hints in documentation (achieved through combination of PEP 563 & 585 and `sphinx-autodoc-typehints`)

## [1.21.0] - 2023-04-17

### Added
* Add support for custom upload timestamp
* Add support for cache control header while uploading

### Infrastructure
* Remove dependency from `arrow`
* Build Python wheels for distribution

## [1.20.0] - 2023-03-23

### Added
* Add `use_cache` parameter to `B2Api.list_buckets`

### Changed
* Connection timeout is now being set explicitly

### Fixed
* Small files downloaded twice

### Infrastructure
* Disable changelog verification for dependabot PRs

## [1.19.0] - 2023-01-24

### Added
* Authorizing a key for a single bucket ensures that this bucket is cached
* `Bucket.ls` operation supports wildcard matching strings
* Documentation for `AbstractUploadSource` and its children
* `InvalidJsonResponse` when the received error is not a proper JSON document
* Raising `PotentialS3EndpointPassedAsRealm` when a specific misconfiguration is suspected
* Add `large_file_sha1` support
* Add support for incremental upload and sync
* Ability to stream data from an unbound source to B2 (for example stdin)

### Fixed
* Removed information about replication being in closed beta
* Don't throw raw `OSError` exceptions when using `DownloadedFile.save_to` to a path that doesn't exist, is a directory or the user doesn't have permissions to write to

### Infrastructure
* Additional tests for listing files/versions
* Ensured that changelog validation only happens on pull requests
* Upgraded GitHub actions checkout to v3, python-setup to v4
* Additional tests for `IncrementalHexDigester`

## [1.18.0] - 2022-09-20

### Added
* Logging performance summary of parallel download threads
* Add `max_download_streams_per_file` parameter to B2Api class and underlying structures
* Add `is_file_lock_enabled` parameter to `Bucket.update()` and related methods

### Fixed
* Replace `ReplicationScanResult.source_has_sse_c_enabled` with `source_encryption_mode`
* Fix `B2Api.get_key()` and `RawSimulator.delete_key()`
* Fix calling `CopySizeTooBig` exception

### Infrastructure
* Fix nox's deprecated `session.install()` calls
* Re-enable changelog validation in CI
* StatsCollector contains context managers for gathering performance statistics

## [1.17.3] - 2022-07-15

### Fixed
* Fix `FileVersion._get_upload_headers` when encryption key is `None`

### Infrastructure
* Fix download integration tests on non-production environments
* Add `B2_DEBUG_HTTP` env variable to enable network-level test debugging
* Disable changelog validation temporarily

## [1.17.2] - 2022-06-24

### Fixed
* Fix a race in progress reporter
* Fix import of replication

## [1.17.1] - 2022-06-23 [YANKED]

### Fixed
* Fix importing scan module

## [1.17.0] - 2022-06-23 [YANKED]

As in version 1.16.0, the replication API may still be unstable, however
no backward-incompatible changes are planned at this point.

### Added
* Add `included_sources` module for keeping track of included modified third-party libraries
* Add `include_existing_files` parameter to `ReplicationSetupHelper`
* Add `get_b2sdk_doc_urls` function for extraction of external documentation URLs during runtime

### Changed
* Downloading compressed files with `Content-Encoding` header set no longer causes them to be decompressed on the fly - it's an option
* Change the per part retry limit from 5 to 20 for data transfer operations. Please note that the retry system is not considered to be a part of the public interface and is subject to be adjusted
* Do not wait more than 64 seconds between retry attempts (unless server asks for it)
* On longer failures wait an additional (random, up to 1s) amount of time to prevent client synchronization
* Flatten `ReplicationConfiguration` interface
* Reorder actions of `ReplicationSetupHelper` to avoid zombie rules

### Fixed
* Fix: downloading compressed files and decompressing them on the fly now does not cause a TruncatedOutput error
* Fix `AccountInfo.is_master_key()`
* Fix docstring of `SqliteAccountInfo`
* Fix lifecycle rule type in the docs

### Infrastructure
* Add 3.11.0-beta.1 to CI
* Change Sphinx major version from 5 to 6
* Extract folder/bucket scanning into a new `scan` module
* Enable pip cache in CI

## [1.16.0] - 2022-04-27

This release contains a preview of replication support. It allows for basic
usage of B2 replication feature (currently in closed beta).

As the interface of the sdk (and the server api) may change, the replication
support shall be considered PRIVATE interface and should be used with caution.
Please consult the documentation on how to safely use the private api interface.

Expect substantial amount of work on sdk interface:
* The interface of `ReplicationConfiguration` WILL change
* The interface of `FileVersion.replication_status` MIGHT change
* The interface of `FileVersionDownload` MIGHT change

### Added
* Add basic replication support to `Bucket` and `FileVersion`
* Add `is_master_key()` method to `AbstractAccountInfo`
* Add `readBucketReplications` and `writeBucketReplications` to `ALL_CAPABILITIES`
* Add log tracing of `interpret_b2_error`
* Add `ReplicationSetupHelper`

### Fixed
* Fix license test on Windows
* Fix cryptic errors when running integration tests with a non-full key

## [1.15.0] - 2022-04-12

### Changed
* Don't run coverage in pypy in CI
* Introduce a common thread worker pool for all downloads
* Increase http timeout to 20min (for copy using 5GB parts)
* Remove inheritance from object (leftover from python2)
* Run unit tests on all CPUs

### Added
* Add pypy-3.8 to test matrix
* Add support for unverified checksum upload mode
* Add dedicated exception for unverified email
* Add a parameter to customize `sync_policy_manager`
* Add parameters to set the min/max part size for large file upload/copy methods
* Add CopySourceTooBig exception
* Add an option to set a custom file version class to `FileVersionFactory`
* Add an option for B2Api to turn off hash checking for downloaded files
* Add an option for B2Api to set write buffer size for `DownloadedFile.save_to` method
* Add support for multiple profile files for SqliteAccountInfo

### Fixed
* Fix copying objects larger than 1TB
* Fix uploading objects larger than 1TB
* Fix downloading files with unverified checksum
* Fix decoding in filename and file info of `DownloadVersion`
* Fix an off-by-one bug and other bugs in the Simulator copy functionality

### Removed
* Drop support for Python 3.5 and Python 3.6

## [1.14.1] - 2022-02-23

### Security
* Fix setting permissions for local sqlite database (thanks to Jan Schejbal for responsible disclosure!)

## [1.14.0] - 2021-12-23

### Fixed
* Relax constraint on arrow to allow for versions >= 1.0.2

## [1.13.0] - 2021-10-24

### Added
* Add support for Python 3.10

### Changed
* Update a list with all capabilities

### Fixed
* Fix pypy selector in CI

## [1.12.0] - 2021-08-06

### Changed
* The `importlib-metadata` requirement is less strictly bound now (just >=3.3.0 for python > 3.5).
* `B2Api` `update_file_legal_hold` and `update_file_retention_setting` now return the set values

### Added
* `BucketIdNotFound` thrown based on B2 cloud response
* `_clone` method to `FileVersion` and `DownloadVersion`
* `delete`, `update_legal_hold`, `update_retention` and `download` methods added to `FileVersion`

### Fixed
* FileSimulator returns special file info headers properly

### Removed
* One unused import.

## [1.11.0] - 2021-06-24

### Changed
* apiver `v2` interface released. `from b2sdk.v2 import ...` is now the recommended import,
  but `from b2sdk.v1 import ...` works as before

## [1.10.0] - 2021-06-23

### Added
* `get_fresh_state` method added to `FileVersion` and `Bucket`

### Changed
* `download_file_*` methods refactored to allow for inspecting DownloadVersion before downloading the whole file
* `B2Api.get_file_info` returns a `FileVersion` object in v2
* `B2RawApi` renamed to `B2RawHTTPApi`
* `B2HTTP` tests are now common
* `B2HttpApiConfig` class introduced to provide parameters like `user_agent_append` to `B2Api` without using internal classes in v2
* `Bucket.update` returns a `Bucket` object in v2
* `Bucket.ls` argument `show_versions` renamed to `latest_only` in v2
* `B2Api` application key methods refactored to operate with dataclasses instead of dicts in v2
* `B2Api.list_keys` is a generator lazily fetching all keys in v2
* `account_id` and `bucket_id` added to FileVersion

### Fixed
* Fix EncryptionSetting.from_response_headers
* Fix FileVersion.size and FileVersion.mod_time_millis type ambiguity
* Old buckets (from past tests) are cleaned up before running integration tests in a single thread

### Removed
* Remove deprecated `SyncReport` methods

## [1.9.0] - 2021-06-07

### Added
* `ScanPoliciesManager` is able to filter b2 files by upload timestamp

### Changed
* `Synchronizer.make_file_sync_actions` and `Synchronizer.make_folder_sync_actions` were made private in v2 interface
* Refactored `sync.file.*File` and `sync.file.*FileVersion` to `sync.path.*SyncPath` in v2
* Refactored `FileVersionInfo` to `FileVersion` in v2
* `ScanPoliciesManager` exclusion interface changed in v2
* `B2Api` unittests for v0, v1 and v2 are now common
* `B2Api.cancel_large_file` returns a `FileIdAndName` object instead of a `FileVersion` object in v2
* `FileVersion` has a mandatory `api` parameter in v2
* `B2Folder` holds a handle to B2Api
* `Bucket` unit tests for v1 and v2 are now common

### Fixed
* Fix call to incorrect internal api in `B2Api.get_download_url_for_file_name`

## [1.8.0] - 2021-05-21

### Added
* Add `get_bucket_name_or_none_from_bucket_id` to `AccountInfo` and `Cache`
* Add possibility to change realm during integration tests
* Add support for "file locks": file retention, legal hold and default bucket retention

### Fixed
* Cleanup sync errors related to directories
* Use proper error handling in `ScanPoliciesManager`
* Application key restriction message reverted to previous form
* Added missing apiver wrappers for FileVersionInfo
* Fix crash when Content-Range header is missing
* Pin dependency versions appropriately

### Changed
* `b2sdk.v1.sync` refactored to reflect `b2sdk.sync` structure
* Make `B2Api.get_bucket_by_id` return populated bucket objects in v2
* Add proper support of `recommended_part_size` and `absolute_minimum_part_size` in `AccountInfo`
* Refactored `minimum_part_size` to `recommended_part_size` (the value used stays the same)
* Encryption settings, types and providers are now part of the public API

### Removed
* Remove `Bucket.copy_file` and `Bucket.start_large_file`
* Remove `FileVersionInfo.format_ls_entry` and `FileVersionInfo.format_folder_ls_entry`

## [1.7.0] - 2021-04-22

### Added
* Add `__slots__` and `__eq__` to `FileVersionInfo` for memory usage optimization and ease of testing
* Add support for SSE-C server-side encryption mode
* Add support for `XDG_CONFIG_HOME` for determining the location of `SqliteAccountInfo` db file

### Changed
* `BasicSyncEncryptionSettingsProvider` supports different settings sets for reading and writing
* Refactored AccountInfo tests to a single file using pytest

### Fixed
* Fix clearing cache during `authorize_account`
* Fix `ChainedStream` (needed in `Bucket.create_file` etc.)
* Make tqdm-based progress reporters less jumpy and easier to read
* Fix emerger examples in docs

## [1.6.0] - 2021-04-08

### Added
* Fetch S3-compatible API URL from `authorize_account`

### Fixed
* Exclude packages inside the test package when installing
* Fix for server response change regarding SSE

## [1.5.0] - 2021-03-25

### Added
* Add `dependabot.yml`
* Add support for SSE-B2 server-side encryption mode

### Changed
* Add upper version limit for the requirements

### Fixed
* Pin `setuptools-scm<6.0` as `>=6.0` doesn't support Python 3.5

## [1.4.0] - 2021-03-03

### Changed
* Add an ability to provide `bucket_id` filter parameter for `list_buckets`
* Add `is_same_key` method to `AccountInfo`
* Add upper version limit for arrow dependency, because of a breaking change

### Fixed
* Fix docs autogen

## [1.3.0] - 2021-01-13

### Added
* Add custom exception for `403 transaction_cap_exceeded`
* Add `get_file_info_by_id` and `get_file_info_by_name` to `Bucket`
* `FileNotPresent` and `NonExistentBucket` now subclass new exceptions `FileOrBucketNotFound` and `ResourceNotFound`

### Changed
* Fix missing import in the synchronization example
* Use `setuptools-scm` for versioning
* Clean up CI steps

## [1.2.0] - 2020-11-03

### Added
* Add support for Python 3.9
* Support for bucket to bucket sync
* Add a possibility to append a string to the User-Agent in `B2Http`

### Changed
* Change default fetch count for `ls` to 10000

### Removed
* Drop Python 2 and Python 3.4 support :tada:
* Remove `--prefix` from `ls` (it didn't really work, use `folderName` argument)

### Fixed
* Allow to set an empty bucket info during the update
* Fix docs generation in CI

## [1.1.4] - 2020-07-15

### Added
* Allow specifying custom realm in B2Session.authorize_account

## [1.1.2] - 2020-07-06

### Fixed
* Fix upload part for file range on Python 2.7

## [1.1.0] - 2020-06-24

### Added
* Add `list_file_versions` method to buckets.
* Add server-side copy support for large files
* Add ability to synthesize objects from local and remote sources
* Add AuthInfoCache, InMemoryCache and AbstractCache to public interface
* Add ability to filter in ScanPoliciesManager based on modification time
* Add ScanPoliciesManager and SyncReport to public interface
* Add md5 checksum to FileVersionInfo
* Add more keys to dicts returned by as_dict() methods

### Changed
* Make sync treat hidden files as deleted
* Ignore urllib3 "connection pool is full" warning

### Removed
* Remove arrow warnings caused by https://github.com/crsmithdev/arrow/issues/612

### Fixed
* Fix handling of modification time of files

## [1.0.2] - 2019-10-15

### Changed
* Remove upper version limit for arrow dependency

## [1.0.0] - 2019-10-03

### Fixed
* Minor bug fix.

## [1.0.0-rc1] - 2019-07-09

### Deprecated
* Deprecate some transitional method names to v0 in preparation for v1.0.0.

## [0.1.10] - 2019-07-09

### Removed
* Remove a parameter (which did nothing, really) from `b2sdk.v1.Bucket.copy_file` signature

## [0.1.8] - 2019-06-28

### Added
* Add support for b2_copy_file
* Add support for `prefix` parameter on ls-like calls

## [0.1.6] - 2019-04-24

### Changed
* Rename account ID for authentication to application key ID.
Account ID is still backwards compatible, only the terminology
has changed.

### Fixed
* Fix transferer crashing on empty file download attempt

## [0.1.4] - 2019-04-04

### Added
Initial official release of SDK as a separate package (until now it was a part of B2 CLI)

[Unreleased]: https://github.com/Backblaze/b2-sdk-python/compare/v1.22.1...HEAD
[1.22.1]: https://github.com/Backblaze/b2-sdk-python/compare/v1.22.0...v1.22.1
[1.22.0]: https://github.com/Backblaze/b2-sdk-python/compare/v1.21.0...v1.22.0
[1.21.0]: https://github.com/Backblaze/b2-sdk-python/compare/v1.20.0...v1.21.0
[1.20.0]: https://github.com/Backblaze/b2-sdk-python/compare/v1.19.0...v1.20.0
[1.19.0]: https://github.com/Backblaze/b2-sdk-python/compare/v1.18.0...v1.19.0
[1.18.0]: https://github.com/Backblaze/b2-sdk-python/compare/v1.17.3...v1.18.0
[1.17.3]: https://github.com/Backblaze/b2-sdk-python/compare/v1.17.2...v1.17.3
[1.17.2]: https://github.com/Backblaze/b2-sdk-python/compare/v1.17.1...v1.17.2
[1.17.1]: https://github.com/Backblaze/b2-sdk-python/compare/v1.17.0...v1.17.1
[1.17.0]: https://github.com/Backblaze/b2-sdk-python/compare/v1.16.0...v1.17.0
[1.16.0]: https://github.com/Backblaze/b2-sdk-python/compare/v1.15.0...v1.16.0
[1.15.0]: https://github.com/Backblaze/b2-sdk-python/compare/v1.14.1...v1.15.0
[1.14.1]: https://github.com/Backblaze/b2-sdk-python/compare/v1.14.0...v1.14.1
[1.14.0]: https://github.com/Backblaze/b2-sdk-python/compare/v1.13.0...v1.14.0
[1.13.0]: https://github.com/Backblaze/b2-sdk-python/compare/v1.12.0...v1.13.0
[1.12.0]: https://github.com/Backblaze/b2-sdk-python/compare/v1.11.0...v1.12.0
[1.11.0]: https://github.com/Backblaze/b2-sdk-python/compare/v1.10.0...v1.11.0
[1.10.0]: https://github.com/Backblaze/b2-sdk-python/compare/v1.9.0...v1.10.0
[1.9.0]: https://github.com/Backblaze/b2-sdk-python/compare/v1.8.0...v1.9.0
[1.8.0]: https://github.com/Backblaze/b2-sdk-python/compare/v1.7.0...v1.8.0
[1.7.0]: https://github.com/Backblaze/b2-sdk-python/compare/v1.6.0...v1.7.0
[1.6.0]: https://github.com/Backblaze/b2-sdk-python/compare/v1.5.0...v1.6.0
[1.5.0]: https://github.com/Backblaze/b2-sdk-python/compare/v1.4.0...v1.5.0
[1.4.0]: https://github.com/Backblaze/b2-sdk-python/compare/v1.3.0...v1.4.0
[1.3.0]: https://github.com/Backblaze/b2-sdk-python/compare/v1.2.0...v1.3.0
[1.2.0]: https://github.com/Backblaze/b2-sdk-python/compare/v1.1.4...v1.2.0
[1.1.4]: https://github.com/Backblaze/b2-sdk-python/compare/v1.1.2...v1.1.4
[1.1.2]: https://github.com/Backblaze/b2-sdk-python/compare/v1.1.0...v1.1.2
[1.1.0]: https://github.com/Backblaze/b2-sdk-python/compare/v1.0.2...v1.1.0
[1.0.2]: https://github.com/Backblaze/b2-sdk-python/compare/v1.0.0...v1.0.2
[1.0.0]: https://github.com/Backblaze/b2-sdk-python/compare/v1.0.0-rc1...v1.0.0
[1.0.0-rc1]: https://github.com/Backblaze/b2-sdk-python/compare/v0.1.10...v1.0.0-rc1
[0.1.10]: https://github.com/Backblaze/b2-sdk-python/compare/v0.1.8...v0.1.10
[0.1.8]: https://github.com/Backblaze/b2-sdk-python/compare/v0.1.6...v0.1.8
[0.1.6]: https://github.com/Backblaze/b2-sdk-python/compare/v0.1.4...v0.1.6
[0.1.4]: https://github.com/Backblaze/b2-sdk-python/compare/4fd290c...v0.1.4
