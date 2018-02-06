#!/usr/bin/env python
"""Command line utility for execution MongoDB tests of all kinds."""

from __future__ import absolute_import

import os
import random
import sys

# Get relative imports to work when the package is not installed on the PYTHONPATH.
if __name__ == "__main__" and __package__ is None:
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from buildscripts.resmokelib import config
from buildscripts.resmokelib import errors
from buildscripts.resmokelib import logging
from buildscripts.resmokelib import parser
from buildscripts.resmokelib import reportfile
from buildscripts.resmokelib import sighandler
from buildscripts.resmokelib import suites as suitesconfig
from buildscripts.resmokelib import testing
from buildscripts.resmokelib import utils


class Resmoke(object):
    def __init__(self):
        self._report = testing.report.ResmokeReport()
        self._report.record_start()
        self._config = None
        self._exec_logger = None
        self._resmoke_logger = None

    def configure_from_command_line(self):
        self._config = parser.parse_command_line()

    def _setup_logging(self):
        logging.loggers.configure_loggers(self._config.logging_config)
        logging.flush.start_thread()
        self._exec_logger = logging.loggers.EXECUTOR_LOGGER
        self._resmoke_logger = self._exec_logger.new_resmoke_logger()

    def run(self):
        if self._config is None:
            raise RuntimeError("Resmoke must be configured before calling run()")
        self._setup_logging()

        if self._config.list_suites:
            self.list_suites()
        elif self._config.find_suites:
            self.find_suites()
        elif self._config.dry_run == "tests":
            self.dry_run()
        else:
            self.run_tests()

    def list_suites(self):
        """Lists the suites that are available to execute."""
        suite_names = suitesconfig.get_named_suites()
        self._resmoke_logger.info("Suites available to execute:\n%s", "\n".join(suite_names))

    def find_suites(self):
        """Lists the suites that run the sepcified tests."""
        suites = self._get_suites()
        suites_by_test = self._find_suites_by_test(suites)
        for test in sorted(suites_by_test):
            suite_names = suites_by_test[test]
            self._resmoke_logger.info("%s will be run by the following suite(s): %s",
                                      test, suite_names)

    def _find_suites_by_test(self, suites):
        """
        Looks up what other resmoke suites run the tests specified in the suites
        parameter. Returns a dict keyed by test name, value is array of suite names.
        """
        memberships = {}
        test_membership = suitesconfig.create_test_membership_map()
        for suite in suites:
            for test in suite.tests:
                memberships[test] = test_membership[test]
        return memberships

    def dry_run(self):
        """Lists which tests would run and which tests would be excluded in a resmoke invocation."""
        suites = self._get_suites()
        for suite, _ in suites:
            self._shuffle_tests(suite)
            sb = ["Tests that would be run in suite {}".format(suite.get_display_name())]
            sb.extend(self._tests_display_list(suite.tests))
            sb.append("Tests that would be excluded from suite {}".format(suite.get_display_name()))
            sb.extend(self._tests_display_list(suite.excluded))
            self._exec_logger.info("\n".join(sb))

    @staticmethod
    def _tests_display_list(tests):
        if tests:
            return tests
        else:
            return ["(no tests)"]

    def run_tests(self):
        """Runs the suite and tests specified."""
        self._resmoke_logger.info("resmoke.py invocation: %s", " ".join(sys.argv))
        try:
            suites = self._get_suites()
            suites_and_reports = []

            for suite in suites:
                suite_report = testing.report.SuiteReport(
                    suite.get_display_name(), len(suite.tests))
                self._report.add_suite(suite_report)
                suites_and_reports.append((suite, suite_report))

            self._setup_signal_handler()

            for suite, suite_report in suites_and_reports:
                self.run_suite(suite, suite_report)

            self._report.record_end()
            self._log_resmoke_summary()

            # Exit with a nonzero code if any of the suites failed.
            exit_code = max(report.return_code for _, report in suites_and_reports)
            self.exit(exit_code)
        finally:
            # TODO do we want to skip if interrupted?
            logging.flush.stop_thread()
            reportfile.write_evergreen_report(self._report)

    def run_suite(self, suite, suite_report):
        """Runs a test suite."""
        self._log_suite_config(suite)
        suite_report.record_suite_start()
        self._execute_suite(suite, suite_report)
        suite_report.record_suite_end()
        self._log_suite_summary(suite_report)
        self._handle_suite_result(suite, suite_report)

    def _log_resmoke_summary(self):
        """Logs a summary of the resmoke run."""
        if len(self._config.suite_files) > 1:
            self._resmoke_logger.info("=" * 80)
            self._resmoke_logger.info(self._report.get_summary())

    def _log_suite_summary(self, suite_report):
        """Logs a summary of the suite run."""
        self._resmoke_logger.info("=" * 80)
        self._resmoke_logger.info(suite_report.get_summary())

    def _execute_suite(self, suite, suite_report):
        self._shuffle_tests(suite)
        if not suite.tests:
            self._exec_logger.info("Skipping %s, no tests to run", suite.test_kind)
            return
        executor = testing.executor.TestSuiteExecutor(
            self._exec_logger, suite, suite_report)
        executor.run()

    def _shuffle_tests(self, suite):
        if not config.SHUFFLE:
            return
        self._exec_logger.info("Shuffling order of tests for %ss in suite %s. The seed is %d.",
                               suite.test_kind, suite.get_display_name(),
                               config.RANDOM_SEED)
        random.seed(config.RANDOM_SEED)
        random.shuffle(suite.tests)

    def _handle_suite_result(self, suite, suite_report):
        if suite_report.interrupted or suite.options.fail_fast and suite_report.return_code != 0:
            self._report.record_end()
            self._log_resmoke_summary()
            self.exit(suite_report.return_code)

    def _setup_signal_handler(self):
        sighandler.register(self._resmoke_logger, self._report)

    def _log_suite_config(self, suite):
        sb = ["YAML configuration of suite {}".format(suite.get_display_name())]
        sb.append(utils.dump_yaml({"test_kind": suite.get_test_kind_config()}))
        sb.append("")
        sb.append(utils.dump_yaml({"selector": suite.get_selector_config()}))
        sb.append("")
        sb.append(utils.dump_yaml({"executor": suite.get_executor_config()}))
        sb.append("")
        # Also logging the logging configuration because that's how it's done now
        # Maybe we can move that somewhere else
        sb.append(utils.dump_yaml({"logging": self._config.logging_config}))
        self._resmoke_logger.info("\n".join(sb))

    def _get_suites(self):
        try:
            return suitesconfig.get_suites(self._config.suite_files,
                                           self._config.test_files,
                                           self._config.exclude_with_any_tags,
                                           self._config.include_with_any_tags)
        except errors.SuiteNotFound as err:
            self._resmoke_logger.error("Failed to parse YAML suite definition: %s", str(err))
            self.list_suites()
            self.exit(1)

    def exit(self, exit_code):
        self._resmoke_logger.info("Exiting with code: %d", exit_code)
        sys.exit(exit_code)


def main():
    resmoke = Resmoke()
    resmoke.configure_from_command_line()
    resmoke.run()


if __name__ == "__main__":
    main()
