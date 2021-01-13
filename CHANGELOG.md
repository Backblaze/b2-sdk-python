# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [1.3.0] - 2021-01-13

### Added
* Add custom exception for `403 transaction_cap_exceeded`
* Add `get_file_info_by_id` and `get_file_info_by_name` to `Bucket`
* `FileNotPresent` and `NonExistentBucket` now subclass new exceptions `FileOrBucketNotFound` and `ResourceNotFound`

### Changed
* Fix missing import in the synchronization example
* Use setuptools-scm for versioning

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

[Unreleased]: https://github.com/Backblaze/b2-sdk-python/compare/v1.3.0...HEAD
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
