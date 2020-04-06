# Makefile utilities for running tests and publishing the package

PACKAGE_NAME=stor
TEST_OUTPUT?=nosetests.xml
PIP_INDEX_URL=https://pypi.python.org/simple/
PYTHON?=$(shell which python)

ifdef TOX_ENV
	TOX_ENV_FLAG := -e $(TOX_ENV)
else
	TOX_ENV_FLAG :=
endif

.PHONY: default
default:
	python setup.py check build

VENV_DIR?=.venv
VENV_ACTIVATE=$(VENV_DIR)/bin/activate
WITH_VENV=. $(VENV_ACTIVATE);
WITH_PBR=$(WITH_VENV) PBR_REQUIREMENTS_FILES=requirements-pbr.txt

.PHONY: venv
venv: $(VENV_ACTIVATE)

$(VENV_ACTIVATE): poetry.lock
	poetry install
	touch $@

develop: venv

.PHONY: docs
docs: venv clean-docs
	$(WITH_VENV) cd docs && make html


.PHONY: setup
setup: ##[setup] Run an arbitrary setup.py command
setup: venv
ifdef ARGS
	@echo "Args no longer supported b/c of poetry" && exit 1
endif

.PHONY: clean
clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg*/
	rm -rf __pycache__/
	rm -f MANIFEST
	rm -f $(TEST_OUTPUT)
	find $(PACKAGE_NAME) -type f -name '*.pyc' -delete
	rm -rf nosetests* "${TEST_OUTPUT}" coverage .coverage


.PHONY: clean-docs
clean-docs:
	$(WITH_VENV) cd docs && make clean


.PHONY: teardown
teardown:
	rm -rf $(VENV_DIR)

.PHONY: lint
lint: venv
	$(WITH_VENV) flake8 $(PACKAGE_NAME)/

.PHONY: unit-test
unit-test: venv
	poetry run pytest -vvv -srx --cov=stor --cov-branch stor

.PHONY: test
test: venv docs unit-test
ifndef SWIFT_TEST_USERNAME
	echo "Please set SWIFT_TEST_USERNAME and SWIFT_TEST_PASSWORD to run swift integration tests" 1>&2
endif
ifndef AWS_TEST_ACCESS_KEY_ID
	echo "Please set AWS_TEST_ACCESS_KEY_ID and AWS_TEST_SECRET_ACCESS_KEY to run s3 integration tests" 1>&2
endif
ifndef DX_AUTH_TOKEN
	echo "Please set DX_AUTH_TOKEN to run DX integration tests" 1>&2
endif

# Distribution

VERSION=$(shell $(WITH_PBR) python setup.py --version | sed 's/\([0-9]*\.[0-9]*\.[0-9]*\).*$$/\1/')

.PHONY: tag
tag: ##[distribution] Tag the release.
tag: venv
	echo "Tagging version as ${VERSION}"
	git tag -a ${VERSION} -m "Version ${VERSION}"
	# We won't push changes or tags here allowing the pipeline to do that, so we don't accidentally do that locally.

.PHONY: dist
dist: venv fullname
	$(WITH_VENV) python setup.py sdist

.PHONY: publish-docs
publish-docs:
	./publish_docs.sh

.PHONY: sdist
sdist: dist
	@echo "runs dist"

.PHONY: version
version:
	@echo ${VERSION}

.PHONY: fullname
fullname:
	$(PYTHON) setup.py --fullname
