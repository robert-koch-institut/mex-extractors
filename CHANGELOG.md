# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changes

- BREAKING: refactor package structure from `mex.foo` to `mex.extractors.foo`
- model update: international-projects, seqrepo, synopse, blueant, sumo, biospecimen,
  odk, datscha-web, confluence-vvt, grippeweb, voxco
- confluence vvt now ignores ill templated pages
- artifical extractor now creates data for type 'consent'

### Deprecated

### Removed

### Fixed

- bug in seq-repo that caused exponential run-time as well as incorrect resource
  keywords
- fixed artificial data generation for Integers, Loinc, and BibliographicResources

### Security

## [0.18.0] - 2024-08-07

### Added

- transform voxco resources and variables
- setting for configuring extractors to skip in dagster

### Changes

- combine seq-repo distribution and resource extraction in one asset
- duplicate seq-repo activities are filtered out
- make dependent extractors explicitly depend on each other
  (grippeweb on confluence-vvt, biospecimen on synopse, odk on international-projects)
- add publisher pipeline to pull all merged items from backend and write them to ndjson
- BREAKING: integrate extractor specific settings in main extractor settings class.
  Environment variables change from `EXTRACTOR_PARAMETER` to `MEX_EXTRACTOR__PARAMETER`,
  access from `ExtractorSettings.parameter` to `settings.extractor.parameter`.
- update mex-common to 0.32.0

### Removed

- remove mypy ignores for arg-type and call-arg
- remove unused ldap module with is_person_in_identity_map_and_ldap function

### Fixed

- fix confluence_vvt: use interne Vorgangsnummer as identifierInPrimarySource
- remaining issues in voxco extractor
- bug in seq-repo that caused exponential run-time as well as incorrect resource
  keywords

### Security

## [0.17.1] - 2024-06-14

### Fixed

- hotfix pydantic version

## [0.17.0] - 2024-06-14

### Added

- entrypoint `all-extractors`: run all extractors
- transform grippeweb resources
- wikidata aux extractor into seq-repo
- function `get_merged_organization_id_by_query_with_transform_and_load` to
  wikidata.extract module
- extract voxco data

### Changes

- update mex-common to 0.27.1
- move `mex.pipeline` documentation to `__init__` to have it in sphinx
- consolidate mocked drop connector into one general mock

### Removed

- remove unused organization_stable_target_id_by_query_sumo asset

### Fixed

- first test does not receive isolated settings but potentially production settings
- mex-drop api connector trailing slash in send request

## [0.16.0] - 2024-05-16

### Added

- add settings attributes `drop_api_key` and `drop_api_url`
- drop api connector for listing and loading files from the drop api
- add basic docker configuration with dockerfile, ignore and compose
- implement odk transform functions
- grippeweb extract and transform
- dagster schedules for non-default groups, configurable via setting `schedule`

### Changes

- update cruft template with new linters
- utilize new, more precise Identifier subclasses
- make pyodbc a soft dependency (only pipelines that use it may fail)
- switch from poetry to pdm
- move MSSQL Server authentication to general settings
- receive one or None organization from wikidata aux extractor
- adjust Timestamp usage to TemporalEntity
- move quotation marks (") filtering to mex-common from requested wikidata label

### Deprecated

- get seq-repo data via mex-drop connector (was: file)

### Removed

- remove sync-persons pipeline, stopgap mx-1572
- remove `public` as a valid sink option

### Fixed

- fix some docstring indents and typings
- ifsg extractor

## [0.15.0] - 2024-02-27

### Added

- configure open-code synchronization workflow
- add mapping connector and integrate into ifsg, seq-repo and sumo
- add odk extraction assets
- LDAP and Organigram integration for seq-repo

### Changes

- update cruft template to latest version
- update to pytest 8 and other minor/patch bumps

## [0.15.0] - 2024-01-31

### Added

- `CHANGELOG.md` documenting notable changes to this project
- a template for pull requests
- ifsg connector and entry point
- Transform seq-repo sources to ExtractedData models
- dependency: dagster-postgres
- documentation workflow to Makefile, mex.bat and as github action
- README.md for mesh data
- ifsg raw data extraction
- ifsg resource transformation
- add IFSG Variable and VariableGroup transformation
- add wikidata dagster assets
- integrate wikidata and organigram into the ifsg extractor

### Changes

- update dependencies
- split synopse extractor into multiple dagster assets
- handle new entityType attribute in merged and extracted models
- split confluence-vvt extractor into multiple dagster assets
- split-up workflow jobs
- harmonize boilerplate
- clean-up tests, raw-data and code for open-sourcing
- split sumo extractor into multiple dagster assets

### Fixed

- materialize only required assets from default asset group
- convert dagster-webserver to main dependency
- update variable filter according to mapping changes
- update resource_disease title according to mapping changes
- cover none values in data with different filtering
- fix valueset extraction

## [0.14.1] - 2023-12-20

First release in changelog
