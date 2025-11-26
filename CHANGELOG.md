# Changelog
All notable changes to this project are documented in this file.

## 0.1.1 [2025-11-26]

### Fixed

- Bug that caused an exception to be raised when the type provided to the `fetch`/`iter` methods was a Pydantic model with an `Optional` field. [#6]
      



[#6]: https://github.com/manoss96/onlymaps/pull/6