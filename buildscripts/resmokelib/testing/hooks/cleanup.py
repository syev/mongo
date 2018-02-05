"""
Testing hook for cleaning up data files created by the fixture.
"""

from __future__ import absolute_import

import os

from . import interface
from ..testcases import interface as testcase
from ... import errors


class CleanEveryN(interface.CustomBehavior):
    """
    Restarts the fixture after it has ran 'n' tests.
    On mongod-related fixtures, this will clear the dbpath.
    """

    DEFAULT_N = 20

    def __init__(self, hook_logger, fixture, n=DEFAULT_N):
        description = "CleanEveryN (restarts the fixture after running `n` tests)"
        interface.CustomBehavior.__init__(self, hook_logger, fixture, description)

        # Try to isolate what test triggers the leak by restarting the fixture each time.
        if "detect_leaks=1" in os.getenv("ASAN_OPTIONS", ""):
            self.logger.info("ASAN_OPTIONS environment variable set to detect leaks, so restarting"
                             " the fixture after each test instead of after every %d.", n)
            n = 1

        self.n = n
        self.tests_run = 0

    def after_test(self, test, test_report, job_logger):
        self.tests_run += 1
        if self.tests_run < self.n:
            return

        test_name = "{}:{}".format(test.short_name(), self.__class__.__name__)
        hook_test_case = CleanEveryNTestCase(test_name, self.fixture, self.tests_run)

        hook_test_case.run(job_logger, test_report)
        self.tests_run = 0

        if hook_test_case.return_code != 0:
            raise errors.StopExecution("Encountered an error while restarting the fixture")


class CleanEveryNTestCase(testcase.TestCase):
    def __init__(self, test_name, fixture, nb_tests_run):
        testcase.TestCase.__init__(self, "Hook", test_name, dynamic=True)
        self._fixture = fixture
        self._nb_tests_run = nb_tests_run

    def run_test(self, test_logger):
        try:
            test_logger.info("%d tests have been run against the fixture, stopping it...",
                             self._nb_tests_run)
            self._fixture.teardown()
            test_logger.info("Starting the fixture back up again...")
            self._fixture.setup()
            self._fixture.await_ready()
            self.return_code = 0
        except Exception as err:
            self.return_code = 2
            raise errors.TestFailure("Encountered an error while restarting the fixture: %s", err)
