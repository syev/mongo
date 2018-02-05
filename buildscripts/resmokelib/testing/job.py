"""
Enables supports for running tests simultaneously by processing them
from a multi-consumer queue.
"""

from __future__ import absolute_import

import sys

from .. import config
from .. import errors
from ..utils import queue as _queue


class JobCoordinator(object):
    def __init__(self):
        self.interrupted = False
        self.teardown_error = False
        self.teardown_on_exit = False

    def set_interrupted(self):
        self.interrupted = True

    def set_teardown_error(self):
        self.teardown_error = True

    def set_teardown_on_exit(self):
        self.teardown_on_exit = True


class Job(object):
    """
    Runs tests from a queue.
    """

    # def __init__(self, logger, fixture, hooks, report, suite_options):
    def __init__(self, logger, fixture, hooks, suite_options):
        """Initializes the job with the specified fixture and custom behaviors."""
        self.logger = logger
        self.fixture = fixture
        self.hooks = hooks
        self.suite_options = suite_options

    # def __call__(self, queue, interrupt_flag, teardown_flag=None):
    def __call__(self, queue, test_report, coordinator, teardown=False):
        """
        Continuously executes tests from 'queue' and records their
        details in 'test_report'.

        If 'teardown_flag' is not None, then 'self.fixture.teardown()'
        will be called before this method returns. If an error occurs
        while destroying the fixture, then the 'teardown_flag' will be
        set.
        """

        try:
            self._run(queue, test_report, coordinator)
        except errors.StopExecution as err:
            # Stop running tests immediately.
            self.logger.error("Received a StopExecution exception: %s.", err)
            self._handle_job_interruption(queue, coordinator)
        except:
            # Unknown error, stop execution.
            self.logger.exception("Encountered an error during test execution.")
            self._handle_job_interruption(queue, coordinator)
        finally:
            if teardown:
                self._teardown_fixture(coordinator)

    @staticmethod
    def _handle_job_interruption(queue, coordinator):
        coordinator.set_interrupted()
        # Drain the queue to unblock the main thread.
        Job._drain_queue(queue)

    def _teardown_fixture(self, coordinator):
        try:
            if not self.fixture.teardown(finished=True):
                pass
        except errors.ServerFailure as err:
            self.logger.warn("Teardown of %s was not successful: %s", self.fixture, err)
            coordinator.set_teardown_error()
        except:
            self.logger.exception("Encountered an error while tearing down %s.", self.fixture)
            coordinator.set_teardown_error()

    def _run(self, queue, test_report, coordinator):
        """
        Calls the before/after suite hooks and continuously executes
        tests from 'queue'.
        """

        for hook in self.hooks:
            hook.before_suite(test_report, self.logger)

        while not coordinator.interrupted:
            test = queue.get_nowait()
            try:
                if test is None:
                    # Sentinel value received, so exit.
                    break
                self._execute_test(test, test_report)
            finally:
                queue.task_done()

        for hook in self.hooks:
            hook.after_suite(test_report, self.logger)

    def _execute_test(self, test, test_report):
        """
        Calls the before/after test hooks and executes 'test'.
        """

        test.configure(self.fixture, config.NUM_CLIENTS_PER_FIXTURE)
        self._run_hooks_before_tests(test, test_report)

        test.run(self.logger, test_report)
        if self.suite_options.fail_fast and not test_report.was_successful():
            self.logger.info("%s failed, so stopping..." % (test.short_description()))
            raise errors.StopExecution("%s failed" % (test.short_description()))

        if not self.fixture.is_running():
            self.logger.error("%s marked as a failure because the fixture crashed during the test.",
                              test.short_description())
            test_report.update_fail_test(test.id(), return_code=2)
            # Always fail fast if the fixture fails.
            raise errors.StopExecution("%s not running after %s" %
                                       (self.fixture, test.short_description()))

        self._run_hooks_after_tests(test, test_report)

    def _run_hooks_before_tests(self, test, test_report):
        """
        Runs the before_test method on each of the hooks.

        Swallows any TestFailure exceptions if set to continue on
        failure, and reraises any other exceptions.
        """

        try:
            for hook in self.hooks:
                hook.before_test(test, test_report, self.logger)

        except errors.StopExecution:
            raise

        except errors.ServerFailure:
            self.logger.exception("%s marked as a failure by a hook's before_test.",
                                  test.short_description())
            test_report.start_fail_stop(test.id(), return_code=2)
            raise errors.StopExecution("A hook's before_test failed")

        except errors.TestFailure:
            self.logger.exception("%s marked as a failure by a hook's before_test.",
                                  test.short_description())
            test_report.start_fail_stop(test.id(), return_code=1)
            if self.suite_options.fail_fast:
                raise errors.StopExecution("A hook's before_test failed")

        except:
            # Record the test as errored in the report.
            test_report.start_error_stop(test, sys.exc_info())
            raise

    def _run_hooks_after_tests(self, test, test_report):
        """
        Runs the after_test method on each of the hooks.

        Swallows any TestFailure exceptions if set to continue on
        failure, and reraises any other exceptions.
        """
        try:
            for hook in self.hooks:
                hook.after_test(test, test_report, self.logger)

        except errors.StopExecution:
            raise

        except errors.ServerFailure:
            self.logger.exception("%s marked as a failure by a hook's after_test.",
                                  test.short_description())
            test_report.update_fail_test(test.id(), return_code=2)
            raise errors.StopExecution("A hook's after_test failed")

        except errors.TestFailure:
            self.logger.exception("%s marked as a failure by a hook's after_test.",
                                  test.short_description())
            test_report.update_fail_test(test.id(), return_code=1)
            if self.suite_options.fail_fast:
                raise errors.StopExecution("A hook's after_test failed")

        except:
            test_report.update_error_test(test.id())
            raise

    @staticmethod
    def _drain_queue(queue):
        """
        Removes all elements from 'queue' without actually doing
        anything to them. Necessary to unblock the main thread that is
        waiting for 'queue' to be empty.
        """

        try:
            while not queue.empty():
                queue.get_nowait()
                queue.task_done()
        except _queue.Empty:
            # Multiple threads may be draining the queue simultaneously, so just ignore the
            # exception from the race between queue.empty() being false and failing to get an item.
            pass
