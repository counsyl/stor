# Makefile utilities for running tests and publishing the package

PACKAGE_NAME=stor
TEST_OUTPUT?=nosetests.xml
PIP_INDEX_URL=https://pypi.python.org/simple/

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

$(VENV_ACTIVATE): requirements*.txt
	test -f $@ || virtualenv $(VENV_DIR)
	$(WITH_VENV) pip install -r requirements-setup.txt --index-url=${PIP_INDEX_URL}
	$(WITH_VENV) pip install -e . --index-url=${PIP_INDEX_URL}
	$(WITH_VENV) pip install -r requirements-dev.txt  --index-url=${PIP_INDEX_URL}
	$(WITH_VENV) pip install -r requirements-docs.txt --index-url=${PIP_INDEX_URL}
	touch $@

develop: venv
	$(WITH_VENV) python setup.py develop

.PHONY: docs
docs: venv clean-docs
	$(WITH_VENV) cd docs && make html


.PHONY: setup
setup: ##[setup] Run an arbitrary setup.py command
setup: venv
ifdef ARGS
	$(WITH_PBR) python setup.py ${ARGS}
else
	@echo "Won't run 'python setup.py ${ARGS}' without ARGS set."
endif

.PHONY: clean
clean:
	python setup.py clean
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
	$(WITH_VENV) flake8 -v $(PACKAGE_NAME)/

.PHONY: unit-test
unit-test: venv
	$(WITH_VENV) \
	coverage erase; \
	tox -v $(TOX_ENV_FLAG); \
	status=$$?; \
	exit $$status;

.PHONY: test
test: venv docs unit-test
ifndef SWIFT_TEST_USERNAME
	echo "Please set SWIFT_TEST_USERNAME and SWIFT_TEST_PASSWORD to run swift integration tests" 1>&2
endif
ifndef AWS_TEST_ACCESS_KEY_ID
	echo "Please set AWS_TEST_ACCESS_KEY_ID and AWS_TEST_SECRET_ACCESS_KEY to run s3 integration tests" 1>&2
endif

# setting this up so that we can use virtualenv, coverage, etc
.PHONY: travis-test
travis-test: venv
	$(WITH_VENV) \
	coverage erase; \
	coverage run setup.py test; \
	status=$$?; \
	coverage report && exit $$status;

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
	python setup.py --fullname
