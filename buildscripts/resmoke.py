#!/usr/bin/env python
"""
Command line utility for execution MongoDB tests of all kinds.
"""

from __future__ import absolute_import

import random
import sys

import resmokelib.config
import resmokelib.parser
import resmokelib.sighandler
import resmokelib.suites
from resmokelib import errors
from resmokelib import testing
from resmokelib import logging
from resmokelib import utils


# TODO go through resmoke.py and copy comments
# TODO add more comments
# TODO check all is implemented


class Resmoke(object):
    def __init__(self):
        self._report = testing.report.ResmokeReport()
        self._report.record_start()
        self._config = None
        self._exec_logger = None
        self._resmoke_logger = None

    def configure(self, resmoke_config):
        self._config = resmoke_config

    def configure_from_command_line(self):
        self._config = resmokelib.parser.parse_command_line()

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
        elif self._config.dry_run:
            self.dry_run()
        else:
            self.run_tests()

    def list_suites(self):
        # TODO find a way to move this out of parser.py
        suite_names = resmokelib.suites.get_named_suites()
        self._resmoke_logger.info("Suites available to execute:\n%s", "\n".join(suite_names))

    def find_suites(self):
        suites = self._get_suites()
        suites_by_test = self._find_suites_by_test(suites)
        for test in sorted(suites_by_test):
            suite_names = suites_by_test[test]
            self._resmoke_logger.info("%s will be run by the following suite(s): %s",
                                      test, suite_names)

    def _find_suites_by_test(suites):
        """
        Looks up what other resmoke suites run the tests specified in the suites
        parameter. Returns a dict keyed by test name, value is array of suite na  mes.
        """
        # TODO: maybe find a way to move membership stuff out of parser.py
        memberships = {}
        test_membership = resmokelib.suites.create_test_membership_map()
        for suite in suites:
            for test in suite.tests:
                memberships[test] = test_membership[test]
        return memberships

    def dry_run(self):
        suites = self._get_suites()
        for suite, _ in suites:
            self._shuffle_tests(suite)
            sb = []
            sb.append("Tests that would be run in suite %s" %
                      suite.get_display_name())
            sb.extend(self._tests_display_list(suite.tests))
            sb.append("Tests that would be excluded from suite %s" %
                      suite.get_display_name())
            sb.extend(self._tests_display_list(suite.excluded))
            self._exec_logger.info("\n".join(sb))

    @staticmethod
    def _tests_display_list(tests):
        if tests:
            return tests
        else:
            return ["(no tests)"]

    def run_tests(self):
        # Move to _get_suites?
        suites = self._get_suites()

        for suite, suite_report in suites:
            self._report.add_suite(suite_report)
        self._setup_signal_handler()

        for suite, suite_report in suites:
            self.run_suite(suite, suite_report)
        # Log the resmoke run summary.
        self._resmoke_logger.info(self._report.summary())

    def run_suite(self, suite, suite_report):
        self.log_suite_config(suite)
        suite_report.record_suite_start()
        self._execute_suite(suite, suite_report)
        suite_report.record_suite_end()
        self._resmoke_logger.info("=" * 80)
        self._resmoke_logger.info(suite_report.get_summary())

        self._handle_suite_result(suite, suite_report)
        # "Summary of %s suite: %s", suite.get_display_name(), suite_report.get_summary())

    def _execute_suite(self, suite, suite_report):
        self._shuffle_tests(suite)
        if not suite.tests:
            self._exec_logger.info("Skipping %s, no tests to run", suite.test_kind)
            # set return code on suite_report?
            return
        executor_config = suite.get_executor_config()
        # TODO figure out executor parameters
        executor = testing.executor.TestSuiteExecutor(
            self._exec_logger, suite, **executor_config)
        executor.run()

    def _shuffle_tests(self, suite):
        if not self._config.shuffle:
            return
        self._exec_logger.info("Shuffling order of tests for %ss in suite %s. The seed is %d.",
                               suite.test_kind, suite.get_display_name(),
                               self._config.random_seed)
        random.seed(self._config.random_seed)
        random.shuffle(suite.tests)

    def _handle_suite_result(self, suite, suite_report):
        if suite_report.interrupted or suite.options.fail_fast and suite_report.return_code != 0:
            self._resmoke_logger.info(self._report.summary())
            self.exit(suite.return_code)

    def _setup_signal_handler(self):
        resmokelib.sighandler.register(self._resmoke_handler, self._report)

    def log_suite_config(self, suite):
        sb = []
        sb.append("YAML configuration of suite %s" % (suite.get_display_name()))
        sb.append(utils.dump_yaml({"test_kind": suite.get_test_kind_config()}))
        sb.append("")
        sb.append(utils.dump_yaml({"selector": suite.get_selector_config()}))
        sb.append("")
        sb.append(utils.dump_yaml({"executor": suite.get_executor_config()}))
        sb.append("")
        # Also logging the logging configuration because that's how it's done now
        # Maybe we can move that somewhere else
        sb.append(utils.dump_yaml({"logging": self._config.logging_config.to_json()}))
        self._resmoke_logger.info("\n".join(sb))

    def _get_suites(self):
        try:
            suites = resmokelib.suites.get_suites(self._config.suite_files,
                                                  self._conifg.test_files,
                                                  self._config.exclude_with_any_tags,
                                                  self._config.include_with_any_tags)
            suites = []
            # also add the reports to the ResmokeReport
            return suites  # list of tuples
        except errors.SuiteNotFound as err:
            self._resmoke_logger.error("Failed to parse YAML suite definition: %s", str(err))
            self.list_suites()
            self.exit(1)

    def exit(self, exit_code, message=None):
        raise errors.ResmokeError("Resmoke error (%d)" % exit_code)


class ResmokeCli(Resmoke):
    def __init__(self):
        config = resmokelib.parser.parse_command_line()
        Resmoke.__init__(self, config)

    def run_tests(self):
        self._resmoke_logger.info("resmoke.py invocation: %s", " ".join(sys.argv))
        Resmoke.run_tests(self)
        # Maybe catch exceptions here and handle the exit_codes

    def exit(self, exit_code, message=None):
        sys.exit(exit_code)


if __name__ == "__main__":
    resmoke = Resmoke()
    resmoke.configure_from_command_line()
    resmoke.run()
