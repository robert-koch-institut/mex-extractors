# MEx extractors

ETL pipelines for the RKI Metadata Exchange.

[![cookiecutter](https://github.com/robert-koch-institut/mex-extractors/actions/workflows/cookiecutter.yml/badge.svg)](https://github.com/robert-koch-institut/mex-template)
[![cve-scan](https://github.com/robert-koch-institut/mex-extractors/actions/workflows/cve-scan.yml/badge.svg)](https://github.com/robert-koch-institut/mex-extractors/actions/workflows/cve-scan.yml)
[![documentation](https://github.com/robert-koch-institut/mex-extractors/actions/workflows/documentation.yml/badge.svg)](https://robert-koch-institut.github.io/mex-extractors)
[![linting](https://github.com/robert-koch-institut/mex-extractors/actions/workflows/linting.yml/badge.svg)](https://github.com/robert-koch-institut/mex-extractors/actions/workflows/linting.yml)
[![open-code](https://github.com/robert-koch-institut/mex-extractors/actions/workflows/open-code.yml/badge.svg)](https://gitlab.opencode.de/robert-koch-institut/mex/mex-extractors)
[![testing](https://github.com/robert-koch-institut/mex-extractors/actions/workflows/testing.yml/badge.svg)](https://github.com/robert-koch-institut/mex-extractors/actions/workflows/testing.yml)

## Project

The Metadata Exchange (MEx) project is committed to improve the retrieval of RKI
research data and projects. How? By focusing on metadata: instead of providing the
actual research data directly, the MEx metadata catalog captures descriptive information
about research data and activities. On this basis, we want to make the data FAIR[^1] so
that it can be shared with others.

Via MEx, metadata will be made findable, accessible and shareable, as well as available
for further research. The goal is to get an overview of what research data is available,
understand its context, and know what needs to be considered for subsequent use.

RKI cooperated with D4L data4life gGmbH for a pilot phase where the vision of a
FAIR metadata catalog was explored and concepts and prototypes were developed.
The partnership has ended with the successful conclusion of the pilot phase.

After an internal launch, the metadata will also be made publicly available and thus be
available to external researchers as well as the interested (professional) public to
find research data from the RKI.

For further details, please consult our
[project page](https://www.rki.de/DE/Aktuelles/Publikationen/Forschungsdaten/MEx/metadata-exchange-plattform-mex-node.html).

[^1]: FAIR is referencing the so-called
[FAIR data principles](https://www.go-fair.org/fair-principles/) – guidelines to make
data Findable, Accessible, Interoperable and Reusable.

**Contact** \
For more information, please feel free to email us at [mex@rki.de](mailto:mex@rki.de).

### Publisher

**Robert Koch-Institut** \
Nordufer 20 \
13353 Berlin \
Germany

## Package

The `mex-extractors` package implements a variety of ETL pipelines to **extract**
metadata from primary data sources using a range of different technologies and
protocols. Then, we **transform** the metadata into a standardized format using models
provided by `mex-common`. The last step in this process is to **load** the harmonized
metadata into a sink (file output, API upload, etc).

## License

This package is licensed under the [MIT license](/LICENSE). All other software
components of the MEx project are open-sourced under the same license as well.

## Development

### Installation

- install python3.11 on your system
- on unix, run `make install`
- on windows, run `.\mex.bat install`

### Linting and testing

- run all linters with `make lint` or `.\mex.bat lint`
- run unit and integration tests with `make test` or `.\mex.bat test`
- run just the unit tests with `make unit` or `.\mex.bat unit`

### Updating dependencies

- update boilerplate files with `cruft update`
- update global requirements in `requirements.txt` manually
- update git hooks with `pre-commit autoupdate`
- update package dependencies using `pdm update-all`
- update github actions in `.github/workflows/*.yml` manually

### Creating release

- run `pdm release RULE` to release a new version where RULE determines which part of
  the version to update and is one of `major`, `minor`, `patch`.

### Container workflow

- build image with `make image`
- run directly using docker `make run`
- start with docker compose `make start`

## Commands

- run `pdm run {command} --help` to print instructions
- run `pdm run {command} --debug` for interactive debugging

### dagster

- `pdm run dagster dev` to launch a local dagster UI

### all extractors

- `pdm run all-extractors` executes all extractors
- execute only in local or dev environment

### artificial extractor

- `pdm run artificial` creates deterministic artificial sample data
- execute only in local or dev environment

### biospecimen extractor

- `pdm run biospecimen` extracts sources from the Biospecimen excel files

### blueant extractor

- `pdm run blueant` extracts sources from the Blue Ant project management software

### confluence-vvt extractor

- `pdm run confluence-vvt` extracts sources from the VVT confluence page

### consent-mailer

- `pdm run consent-mailer` send emails to collect publishing consents

### contact-point

- `pdm run contact-point` extracts default contact points

### datscha-web extractor

- `pdm run datscha-web` extracts sources from the datscha web app

### endnote extractor

- `pdm run endnote` extracts from endnote XML files

### ff-projects extractor

- `pdm run ff-projects` extracts sources from the FF Projects excel file

### ifsg extractor

- `pdm run ifsg` extracts sources from the ifsg data base

### international-projects extractor

- `pdm run international-projects` extracts sources from the international projects excel

### grippeweb extractor

- `pdm run grippeweb` extracts grippeweb metadata from grippeweb database

### odk extractor

- `pdm run odk` extracts ODK survey data from excel files

### open-data extractor

- `pdm run open-data` extracts Open Data sources from the Zenodo API

### seq-repo extractor

- `pdm run seq-repo` extracts sources from seq-repo JSON file

### sumo extractor

- `pdm run sumo` extract sumo data from xlsx files

### synopse extractor

- `pdm run synopse` extracts synopse data from report-server exports

### voxco extractor

- `pdm run voxco` extracts voxco data from voxco JSON files

### publisher

- `pdm run publisher` gets merged items from backend and publishes them into sink
