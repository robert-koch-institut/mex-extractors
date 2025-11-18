.PHONY: all test setup hooks install lint unit test wheel image run start docs
all: install lint test

LATEST = $(shell git describe --tags $(shell git rev-list --tags --max-count=1))
PWD = $(shell pwd)

setup:
	# install meta requirements system-wide
	@ echo installing requirements; \
	pip --disable-pip-version-check install --force-reinstall -r requirements.txt; \

hooks:
	# install pre-commit hooks when not in CI
	@ if [ -z "$$CI" ]; then \
		pre-commit install; \
	fi; \

install: setup hooks
	# install packages from lock file in local virtual environment
	@ echo installing package; \
	pdm install-all; \

lint:
	# run the linter hooks from pre-commit on all files
	@ echo linting all files; \
	pre-commit run --all-files; \

unit:
	# run the test suite with all unit tests
	@ echo running unit tests; \
	pdm run pytest -m 'not integration'; \

test:
	# run the unit and integration test suites
	@ echo running all tests; \
	pdm run pytest --numprocesses=auto --dist=worksteal; \

requires_rki_infrastructure:
	# run the tests marked with requires_rki_infrastructure
	@ echo running tests marked with requires_rki_infrastructure
	pdm run pytest -m 'requires_rki_infrastructure'

wheel:
	# build the python package
	@ echo building wheel; \
	pdm build --no-sdist; \

image:
	# build the docker image
	@ echo building docker image mex-extractors:${LATEST}; \
	export DOCKER_BUILDKIT=1; \
	docker build \
		--tag rki/mex-extractors:${LATEST} \
		--tag rki/mex-extractors:latest .; \

run: image
	# run the extractors using docker
	@ echo running docker container mex-extractors:${LATEST}; \
	mkdir --parents --mode 777 work; \
	docker run \
		--env MEX_WORK_DIR=/work \
		--volume ${PWD}/work:/work \
		rki/mex-extractors:${LATEST}; \

start: image
	# start the service using docker compose
	@ echo start mex-extractors:${LATEST} with compose; \
	export DOCKER_BUILDKIT=1; \
	export COMPOSE_DOCKER_CLI_BUILD=1; \
	docker compose up --remove-orphans; \

docs:
	# use sphinx to auto-generate html docs from code
	@ echo generating docs; \
	pdm doc; \
