
# Change Log
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/)
and this project adheres to [Semantic Versioning](http://semver.org/).

## [Unreleased] - 2022-02-11

### Added

### Changed

### Fixed

## [0.2.4] - 2022-02-11

Introduction of the repository changelog.

### Added

- Github actions run when pushing changes or making a pull request to `dev` branch ([#113](https://github.com/alkemics/pandagg/pull/113)).
- `match_all`, and `match_none` query clauses ([103](https://github.com/alkemics/pandagg/issues/103#issuecomment-1040425685), [#112](https://github.com/alkemics/pandagg/pull/112)).

### Changed

- Handle deprecation warnings introduced in [elasticsearch-py](https://github.com/elastic/elasticsearch-py/issues/1698) ([#109](https://github.com/alkemics/pandagg/pull/109)).
- Improved IMDB and NY-restaurants examples, by allowing them to be ingested on client cluster by a simple command line ([#116](https://github.com/alkemics/pandagg/pull/116)).

### Fixed

- Fix aggregation scan via composite aggregation, the first batch was not yielded ([#101](https://github.com/alkemics/pandagg/issues/101), [#110](https://github.com/alkemics/pandagg/pull/110)).
- Fix search scan, by allowing passing of parameters ([#103](https://github.com/alkemics/pandagg/issues/103#issuecomment-1040445479), [#111](https://github.com/alkemics/pandagg/pull/111)).
