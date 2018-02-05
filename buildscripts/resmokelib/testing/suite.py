"""
Holder for the (test kind, list of tests) pair with additional metadata about when and how they
execute.
"""

from __future__ import absolute_import

import threading

from .. import config as _config
from .. import selector as _selector


class Suite(object):
    """
    A suite of tests of a particular kind (e.g. C++ unit tests, dbtests, jstests).
    """

    def __init__(self, suite_name, suite_config, suite_options=_config.SuiteOptions.ALL_INHERITED):
        """
        Initializes the suite with the specified name and configuration.
        """
        self._lock = threading.RLock()

        self._suite_name = suite_name
        self._suite_config = suite_config
        self._suite_options = suite_options

        self.test_kind = self.get_test_kind_config()
        self.tests, self.excluded = self._get_tests_for_kind(self.test_kind)

    def _get_tests_for_kind(self, test_kind):
        """
        Returns the tests to run based on the 'test_kind'-specific
        filtering policy.
        """
        test_info = self.get_selector_config()

        # The mongos_test doesn't have to filter anything, the test_info is just the arguments to
        # the mongos program to be used as the test case.
        if test_kind == "mongos_test":
            mongos_options = test_info  # Just for easier reading.
            if not isinstance(mongos_options, dict):
                raise TypeError("Expected dictionary of arguments to mongos")
            return [mongos_options], []

        tests, excluded = _selector.filter_tests(test_kind, test_info)
        if _config.ORDER_TESTS_BY_NAME:
            return sorted(tests, key=str.lower), sorted(excluded, key=str.lower)

        return tests, excluded

    def get_name(self):
        """
        Returns the name of the test suite.
        """
        return self._suite_name

    def get_display_name(self):
        """
        Returns the name of the test suite with a unique identifier for its SuiteOptions.
        """

        if self.options.description is None:
            return self.get_name()

        return "{} ({})".format(self.get_name(), self.options.description)

    def get_selector_config(self):
        """
        Returns the "selector" section of the YAML configuration.
        """

        if "selector" not in self._suite_config:
            return {}
        selector = self._suite_config["selector"].copy()

        if self.options.include_tags is not None:
            if "include_tags" in selector:
                selector["include_tags"] = {"$allOf": [
                    selector["include_tags"],
                    self.options.include_tags,
                ]}
            elif "exclude_tags" in selector:
                selector["exclude_tags"] = {"$anyOf": [
                    selector["exclude_tags"],
                    {"$not": self.options.include_tags},
                ]}
            else:
                selector["include_tags"] = self.options.include_tags

        return selector

    def get_executor_config(self):
        """
        Returns the "executor" section of the YAML configuration.
        """
        return self._suite_config["executor"]

    def get_test_kind_config(self):
        """
        Returns the "test_kind" section of the YAML configuration.
        """
        return self._suite_config["test_kind"]

    @property
    def options(self):
        return self._suite_options.resolve()

    def with_options(self, suite_options):
        """
        Returns a Suite instance with the specified resmokelib.config.SuiteOptions.
        """

        return Suite(self._suite_name, self._suite_config, suite_options)
