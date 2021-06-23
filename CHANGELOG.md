# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
* Refactored `minimum_part_size` to `recommended_part_size` (tha value used stays the same)
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

[Unreleased]: https://github.com/Backblaze/b2-sdk-python/compare/v1.10.0...HEAD
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
