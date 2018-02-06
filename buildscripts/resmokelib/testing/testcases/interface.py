"""
Subclass of TestCase with helpers for spawning a separate process to perform the actual test case.
"""

from __future__ import absolute_import

import os
import os.path
import sys

from ... import errors
from ... import logging
from ...utils import registry


_TEST_CASES = {}


def make_test_case(test_kind, *args, **kwargs):
    """Factory function for creating TestCase instances."""

    if test_kind not in _TEST_CASES:
        raise ValueError("Unknown test kind '%s'" % test_kind)
    return _TEST_CASES[test_kind](*args, **kwargs)


class TestCase(object):
    """A test case to execute."""

    __metaclass__ = registry.make_registry_metaclass(_TEST_CASES)

    REGISTERED_NAME = registry.LEAVE_UNREGISTERED

    def __init__(self, test_kind, test_name, dynamic=False):
        """Initializes the TestCase with the name of the test."""

        if not isinstance(test_kind, basestring):
            raise TypeError("test_kind must be a string")

        if not isinstance(test_name, basestring):
            raise TypeError("test_name must be a string")

        self.test_kind = test_kind
        self.test_name = test_name
        self.dynamic = dynamic

        self.fixture = None
        self.return_code = None
        self.exception = None

        self.is_configured = False

    def basename(self):
        """Returns the basename of the test."""
        return os.path.basename(self.test_name)

    def short_name(self):
        """Returns the basename of the test without the file extension."""
        return os.path.splitext(self.basename())[0]

    def id(self):
        return self.test_name

    def short_description(self):
        return "%s %s" % (self.test_kind, self.test_name)

    def configure(self, fixture, *args, **kwargs):
        """Stores 'fixture' as an attribute for later use during execution."""
        if self.is_configured:
            raise RuntimeError("configure can only be called once")

        self.is_configured = True
        self.fixture = fixture

    def reset(self):
        self.return_code = None
        self.exception = None

    def run(self, job_logger, test_report):
        test_logger = None
        try:
            if self.dynamic:
                command = "(dynamic test case)"
                job_logger.info("Running %s... (dynamic test case)", self.basename())
            else:
                command = self._as_command(None)
                job_logger.info("Running %s...\n%s", self.basename(), command)
            test_logger = job_logger.new_test_logger(self.short_name(), self.basename(),
                                                     command)
            test_report.start_test(self.id(), test_logger.url_endpoint, dynamic=self.dynamic)
            self.run_test(test_logger)
            # test_report.add_success(self.id())
            test_report.pass_test(self.id(), self.return_code)
        except errors.TestFailure:
            # test_report.add_failure(self.id())
            test_report.fail_test(self.id(), self.return_code)
            self.exception = sys.exc_info()
        except KeyboardInterrupt:
            # Should not happen as tests are not run in the main thread
            raise
        except:
            # test_report.add_error(self.id(), sys.exc_info())
            test_report.error_test(self.id(), self.return_code)
            self.exception = sys.exc_info()
        finally:
            time_taken = test_report.stop_test(self.id())
            job_logger.info("%s ran in %0.2f seconds.", self.basename(), time_taken)
            if test_logger:
                # Asynchronously closes the buildlogger test handler to avoid having too many
                # threads open on 32-bit systems.
                for handler in test_logger.handlers:
                    # We ignore the cancellation token returned by close_later() since we always
                    # want the logs to eventually get flushed.
                    logging.flush.close_later(handler)

    # TODO is it the responsability of run_test to set self.return_code?
    # Maybe move this notion of return_code to a ProcessTestCase subclass
    def run_test(self, test_logger):
        """
        Runs the specified test.
        """
        raise NotImplementedError("run_test must be implemented by TestCase subclasses")

    def _as_command(self, test_logger):
        """
        Returns the command invocation used to run the test.
        """
        # TODO maybe we can create the command without making the process object
        # or create the process without giving the test_logger
        return self._make_process(test_logger).as_command()

    def _execute(self, test_logger, process):
        """
        Runs the specified process.
        """
        test_logger.info("Starting %s...\n%s", self.short_description(), process.as_command())

        process.start()
        test_logger.info("%s started with pid %s.", self.short_description(), process.pid)

        self.return_code = process.wait()
        if self.return_code != 0:
            raise errors.TestFailure("%s failed" % (self.short_description()))

        test_logger.info("%s finished.", self.short_description())

    def _make_process(self, test_logger):
        """
        Returns a new Process instance that could be used to run the
        test or log the command.
        """
        raise NotImplementedError("_make_process must be implemented by TestCase subclasses")


# class DynamicTestCase(TestCase):
#     def as_command(self):
#         return "(dynamic test case)"
#
#
# class ProcessTestCase(TestCase):
#     def as_command(self):
#         """
#         Returns the command invocation used to run the test.
#         """
#         # TODO maybe we can create the command without making the process object
#         # or create the process without giving the test_logger
#         return self._make_process().as_command()
#
#     def _execute(self, test_logger, process):
#         """
#         Runs the specified process.
#         """
#         test_logger.info("Starting %s...\n%s", self.short_description(), process.as_command())
#
#         process.start()
#         test_logger.info("%s started with pid %s.", self.short_description(), process.pid)
#
#         self.return_code = process.wait()
#         if self.return_code != 0:
#             raise errors.TestFailure("%s failed" % (self.short_description()))
#
#         test_logger.info("%s finished.", self.short_description())
#
#     def _make_process(self, test_logger):
#         """
#         Returns a new Process instance that could be used to run the
#         test or log the command.
#         """
#         raise NotImplementedError("_make_process must be implemented by TestCase subclasses")
