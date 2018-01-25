"""Module for the suites configuration."""

from __future__ import absolute_import

import collections
import optparse
import os

from . import config as _config
from . import errors
from . import utils
from . import testing
from .. import resmokeconfig


def get_named_suites():
    """
    Returns the list of suites available to execute.
    """

    # Skip "with_*server" and "no_server" because they do not define any test files to run.
    executor_only = set(["with_server", "with_external_server", "no_server"])
    suite_names = [suite for suite in resmokeconfig.NAMED_SUITES if suite not in executor_only]
    suite_names.sort()
    return suite_names


def create_test_membership_map(fail_on_missing_selector=False, test_kind=None):
    """
    Returns a dict keyed by test name containing all of the suites that will run that test.

    If 'test_kind' is specified then only the mappings for that kind are returned.
    Since this iterates through every available suite, it should only be run once.
    """

    test_membership = collections.defaultdict(list)
    suite_names = get_named_suites()
    for suite_name in suite_names:
        try:
            suite_config = _get_suite_config(suite_name)
            if test_kind and suite_config.get("test_kind") != test_kind:
                continue
            suite = testing.suite.Suite(suite_name, suite_config)
        except IOError as err:
            # If unittests.txt or integration_tests.txt aren't there we'll ignore the error because
            # unittests haven't been built yet (this is highly likely using find interactively).
            if err.filename in _config.EXTERNAL_SUITE_SELECTORS:
                if not fail_on_missing_selector:
                    continue
            raise

        for testfile in suite.tests:
            if isinstance(testfile, dict):
                continue
            test_membership[testfile].append(suite_name)
    return test_membership


def get_suites(suite_files, test_files, exclude_with_any_tags, include_with_any_tags):
    suite_roots = None
    if test_files:
        # Do not change the execution order of the tests passed as args, unless a tag option is
        # specified. If an option is specified, then sort the tests for consistent execution order.
        _config.ORDER_TESTS_BY_NAME = any(tag_filter is not None for
                                          tag_filter in (exclude_with_any_tags,
                                                         include_with_any_tags))
        # Build configuration for list of files to run.
        suite_roots = _make_suite_roots(test_files)

    suites = []
    for suite_filename in suite_files:
        suite_config = _get_suite_config(suite_filename)
        if suite_roots:
            # Override the suite's default test files with those passed in from the command line.
            suite_config.update(suite_roots)
        suite = testing.suite.Suite(suite_filename, suite_config)
        suites.append(suite)
    return suites


def _make_suite_roots(files):
    return {"selector": {"roots": files}}


def _get_suite_config(pathname):
    """
    Attempts to read a YAML configuration from 'pathname' that describes
    what tests to run and how to run them.
    """
    return _get_yaml_config("suite", pathname)


def _get_yaml_config(kind, pathname):
    # Named executors or suites are specified as the basename of the file, without the .yml
    # extension.
    if not utils.is_yaml_file(pathname) and not os.path.dirname(pathname):
        if pathname not in resmokeconfig.NAMED_SUITES:
            raise errors.SuiteNotFound("Unknown %s '%s'" % (kind, pathname))
        pathname = resmokeconfig.NAMED_SUITES[pathname]  # Expand 'pathname' to full path.

    if not utils.is_yaml_file(pathname) or not os.path.isfile(pathname):
        raise optparse.OptionValueError("Expected a %s YAML config, but got '%s'"
                                        % (kind, pathname))
    return utils.load_yaml_file(pathname)
