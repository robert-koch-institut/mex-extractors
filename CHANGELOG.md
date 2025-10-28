# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changes

### Deprecated

### Removed

### Fixed

### Security

## [1.4.0] - 2025-10-28

### Changes

- streamline dagster asset names
- publish all consenting persons in publisher
- bump mex-common dependency to 1.16.0
- use primary source id convenience function
- update open data resource according to mapping changes

### Fixed

- consent-mailer: fetch consent from DB in batches, to fix "URL too long" error

## [1.3.0] - 2025-10-14

### Changes

- igs wrap up

## [1.2.0] - 2025-10-07

### Added

- extract mandatory fields for igs variables and variable groups
- optional value fields for igs access platform and resource
- update mex-common dependency to 1.5
- persons have rki org as affiliation
- create s3 sink base class and bequeath to s3Sink (writes ndjson) and new S3XlsxSink (writs xlsx)
- publisher: replace references to non-consenting person ids with their respective unit ids

### Changes

- read datenkompass default values from mapping

### Fixed

- empty list handling in datenkompass
- odk variable identifier in primary source
- random list element order in some extracted entity attributes

## [1.1.1] - 2025-09-22

### Fixed

- apply odk variable filter rule

## [1.1.0] - 2025-09-11

### Added

- datenkompass: add function for html-link handling, add tests

### Changes

- update datenkompass models to not contain lists or unnecessarily optional fields

### Fixed

- make consent mailer settings inherit from BaseModel

## [1.0.0] - 2025-09-04

### Changes

- update Datenkompass according to feedback #2
- fix forgotten german PrefLabel in Beschreibung datenkompass
- log number of published datenkompass items
- fix some typos

## [0.42.0] - 2025-09-04

### Added

- `AssetCheckRule` model into `mex.extractors.pipeline.checks.models.check`
- assetcheck implementation for `x_items_more_than`-rule in `mex.extractors.pipeline.checks.main`
- AssetChecks `x_items_more_than` for blueant `mex.extractors.blueant.checks` and endnote `mex.extractors.endnote.checks`
- tests for blueant, endnote and pipeline
- yaml rule data for testing `assets\raw-data\pipeline\{extractor}\{entityType}`
- `all_checks_path` added in `mex.extractors.settings` for rules
- finished consent-mailer pipeline
- settings for the content-mailer
  - smtp server address for sending mails
  - mailHog api endpoint url for testing mail sending
- add return values to all assets for debugging and bookkeeping

### Changes

- `extracted_endnote_bibliographic_resources()` and `extracted_blueant_activities()` return `Output`-object
- improve usage of backend endpoints for merged items
- add resources by unit filter to datenkompass
- update mex-common dependency to 1.2
- update mex-artificial dependency to >1.0
- wrap up synopse

## [0.41.0] - 2025-08-21

### Changes

- update synopse according to mapping changes
- update datenkompass according to mapping changes and save items as xlsx

### Fixed

- igs test main
- reading of xml-files for endnote

## [0.40.0] - 2025-07-30

### Changes

- update mex-common to 0.65

## [0.39.0] - 2025-07-28

### Added

- add consent-mailer pipeline (unfinished)

### Changes

- bumped cookiecutter template to e886ec
- try to remove broken person references in the publisher

## [0.38.0] - 2025-07-24

### Added

- add RKI organization as unitOf to organigram units
- enable mypy linting for tests folder
- include persons from allowed primary sources in the publishing pipeline
- add contact-point mini extractor to get default contact points
- add mex-extractors version to asset and job metadata
- publisher for datenkompass

### Changes

- ensure dagster definitions are resolved, for dagster 1.11 compat
- summarize logging for synopse filtering functions
- filter out unpublished persons from contact fields and set a fallback

### Removed

- remove unused pytest fixtures

## [0.37.4] - 2025-07-11

### Changes

- increase zenodo connector patience with more backoff time

## [0.37.3] - 2025-07-10

### Changes

- endnote extractor ignores records with 50 or more authors.
- improve endnote reading from extracted raw record

## [0.37.2] - 2025-07-10

### Fixed

- change endnote identifierInPrimarySource hyphenation
- avoid identifier collision in endnote organizations and persons

## [0.37.1] - 2025-07-01

### Added

- assign fullName for endnote person

### Fixed

- remove boto3 pin again
- drop s3 signature version from client config

## [0.37.0] - 2025-07-01

### Added

- transform mandatory fields of IGS access platform and resources
- waiting time in Open Data request to API

### Changes

- odk resource hadPrimarysource to odk

### Fixed

- ifsg variable extraction
- temporarily pin boto3 to below 1.38.38

## [0.36.0] - 2025-06-19

### Added

- extract from IGS Open API

### Changes

- update endnote and open-data according to mapping changes

## [0.35.0] - 2025-06-17

### Changes

- bump mex-common dependency to 0.62.0
- install mex-common and mex-artificial from pypi instead of github

## [0.34.5] - 2025-06-16

### Added

- transform and wrap up endnote
- add `topological_sort` function to reorder a list of items based on topology

### Changes

- use helper from mex-artificial to generate and load artificial extracted items
- order organizational units according to organigram hierarchy

## [0.34.4] - 2025-05-19

### Changes

- change return type of `run_job_in_process` to bool indicating success
- update mex-common to 0.61.1

## [0.34.3] - 2025-05-19

### Changes

- update mex-common to 0.61.0

## [0.34.2] - 2025-05-15

### Removed

- remove partitioning seq-repo resources

### Fixed

- odk resource relations

## [0.34.1] - 2025-05-14

### Changes

- try out partitioning seq-repo resources

## [0.34.0] - 2025-05-13

### Added

- enabled scanning for asset_checks for dagster definitions

### Changes

- switch `asset` decorator over to dagster for proper typing
- update mex-common to 0.60.0

### Fixed

- always load parent before child
- ifsg keyword extraction
- voxco variable lookup

## [0.33.0] - 2025-04-29

### Added

- Dagster sensor to run publisher after other extractor jobs

### Changes

- update mex-common to 0.59.1 reducing generator calls and unsafe caching
- remove linting exceptions from toml and fix or skip lines directly

## [0.32.3] - 2025-04-22

### Changes

- update mex-common to 0.58.2 to increase BackendApiSink timeout to 90s

## [0.32.2] - 2025-04-17

### Changes

- update mex-common to 0.58.1 to log sink errors

## [0.32.1] - 2025-04-16

### Changes

- update mex-common to 0.58.0 with reconfigured request timeout

## [0.32.0] - 2025-04-14

### Added

- extract from endnote xml files

### Changes

- split seq-repo load function into new asset
- apply all activity filters to all extractors(except synopse) with activities
- refactor filter functionality to filter module

### Removed

- removed RDMO unused extractor
- BREAKING: remove all activity filter rules from settings

### Fixed

- exclude skipped extractor from `all_extractors` job

## [0.31.0] - 2025-03-24

### Changes

- updated mex-common dependency to 0.55.0

## [0.30.1] - 2025-03-20

### Changes

- updated mex-common dependency to 0.54.4

## [0.30.0] - 2025-03-18

### Changes

- output settings as job metadata
- update mex-common to 0.54.3
- expand publisher filter to not push MergedPersons
- quickfix publisher to always write an ndjson directly to S3
  (will become outdated by MX-1808)

## [0.29.1] - 2025-03-14

### Fixed

- provide wikidata extractor with wikidata id instead of url

## [0.29.0] - 2025-03-13

### Added

- add settings for wikidata mapping

### Changes

- update test mappings
- switch out wikidata query by names with query by id
- update seq-repo, odk, datscha-web, sumo, synopse test data
- simplify tests by using settings fixture

### Fixed

- ignore and log errors while getting vvt persons or units

## [0.28.2] - 2025-03-07

### Fixed

- fix ambiguous identifierInPrimarySources for ifsg variables and disease-resources
- fix contact-point for synopse not being loaded correctly

## [0.28.1] - 2025-03-05

### Changes

- update mex-common to 0.54.2

## [0.28.0] - 2025-03-05

### Added

- add s3 sink implementation for use with the publisher

### Changes

- let `load` function accept rule-sets and merged-items, just like sinks
- let publisher use `load` function with configurable sinks
- wrap up synopse
- update mex-common dependency to 0.53

### Removed

- remove `PublisherContainer` and `write_merged_items` in favor of generic versions
- remove `organigram` extractor, it was duplicating `mex.extractors.pipeline.organigram`

## [0.27.0] - 2025-02-17

### Changes

- update mex-common to version 0.51.0
- use new static mapping models
- move inline test mappings to `/assets` with suffix `_mock`

### Removed

- remove `mex.extractors.mapping` model in favor of `load_yaml` and `model_validate`

### Fixed

- fix setValues listyness of test mappings
- fix test mapping schema path
- fix test mapping indentations

## [0.26.0] - 2025-02-13

### Changes

- update mex-common to version 0.48.0
- simplify load function to wrap the Sink.load methods
- wrap up: confluence-vvt

### Removed

- remove BackendIdentityProvider in favor of mex-common implementation
- remove ExtractorIdentityProvider in favor of mex-common enum
- remove identity_provider setting in favor of mex-common version

## [0.25.0] - 2025-01-23

### Changes

- update mex-common to 0.47.0

## [0.24.1] - 2025-01-21

### Changes

- set dockerfile base to bullseye

### Fixed

- fix module name for dagster startup in compose.yaml

## [0.24.0] - 2025-01-15

### Added

- extractor for Open Data

### Changes

- wrap up sumo model v3 update

## [0.23.0] - 2024-12-18

### Changes

- extractors now use wikidata helper function
- BREAKING: rename artificial provider function `extracted_data` to `extracted_items`
- prefer concrete unions over base classes for merged and extracted item typing
- update mex-common to 0.45.0 and mex-model to 3.4.0

### Fixed

- fix coverage and linting issues

## [0.22.0] - 2024-12-10

### Changes

- wrap up ifsg model v3 update
- wrap up seq-repo model v3 update

## [0.21.0] - 2024-11-19

### Added

- convenience / helper functions for wikidata and primary source

### Changes

- make datscha ignore organizations with name "None"
- update mex-common to 0.41 and mex-model to 3.2

### Fixed

- fix a bunch of linting errors and remove ignored ruff codes

## [0.20.0] - 2024-11-11

### Added

- increase minimum valid artificial data count to two times the number of entity types

### Changes

- improve publishing pipeline: logging, Backend connector, allow-list

### Removed

- remove `matched` setting for the artificial extractor, since that was not implemented
- stop configuring entity-type weights for artificial data, since that broke determinism
- removed unused `-c` alias for the count setting of the artificial extractor
- deprecated mapping commit hashes in README

## [0.19.0] - 2024-10-29

### Added

- setting for configuring extractors to skip in dagster

### Changes

- BREAKING: refactor package structure from `mex.foo` to `mex.extractors.foo`
- BREAKING: Mapping extractors now returns Mapping models instead of nested dictionaries
- model v3 update: artificial, international-projects, seq-repo, synopse, blueant, sumo,
  biospecimen, odk, datscha-web, confluence-vvt, grippeweb, voxco, ifsg

### Fixed

- fix bug in seq-repo that caused exponential run-time and incorrect resource keywords
- fix artificial data generation for Integers, Loinc, and BibliographicResources
- make confluence-vvt ignore ill templated pages
- make ifsg identifierInPrimarySource unique to avoid stableTargetId collisions

## [0.18.0] - 2024-08-07

### Added

- transform voxco resources and variables

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
