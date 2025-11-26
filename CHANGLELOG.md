# Changelog
All notable changes to this project are documented here.

## [Unreleased]

### Public

- Added:
    - Oracle databases are now supported via driver `oracledb`. [#5]

### Internal

- Added:
    - `BaseDriver` subclasses can now optionally implement an `init_connection` method which is called right after
    a `PyDbAPIv2Connection` is created so as to perform any
    driver specific initializations on said connection. [#5]

- Changed:
    - Renamed some functions to better convey their meaning. [#5]


[#5]: https://github.com/manoss96/onlymaps/pull/5