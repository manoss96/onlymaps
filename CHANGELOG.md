# Changelog
All notable changes to this project are documented in this file.

## [Unreleased]

### Added

- Oracle databases are now supported via driver `oracledb`. [#5]
- (INTERNAL) `BaseDriver` subclasses can now optionally implement an `init_connection` method which is called right after a `PyDbAPIv2Connection` is created so as to perform any driver specific initializations on said connection. [#5]

### Changed

- (INTERNAL) Renamed certain functions to better convey their meaning. [#5]

### Fixed

- Bug that resulted in query parameters not being properly handled when wrapped within
  `Bulk`. [#7]


## 0.1.1 [2025-11-26]

### Fixed

- Bug that caused an exception to be raised when the type provided to the `fetch`/`iter` methods was a model type with an `Optional` field. [#6]
      


[#5]: https://github.com/manoss96/onlymaps/pull/5
[#6]: https://github.com/manoss96/onlymaps/pull/6
[#7]: https://github.com/manoss96/onlymaps/pull/7