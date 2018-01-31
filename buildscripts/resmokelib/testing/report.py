"""
Extension to the unittest.TestResult to support additional test status
and timing information for the report.json file.
"""

from __future__ import absolute_import

import collections
import copy
import itertools
import threading
import time
import unittest

from .. import config as _config
from .. import logging


STATUS_SUCCESS = "pass"
STATUS_FAIL = "fail"
STATUS_ERROR = "error"
STATUS_TIMEOUT = "timeout"
RETURN_CODE_TIMEOUT = -2
EVG_STATUS_SUCCESS = "success"
EVG_STATUS_FAIL = "fail"


class ResmokeReport(object):
    def __init__(self):
        self.start_time = None
        self.end_time = None
        self.suite_reports = []

    def record_start(self):
        self.start_time = time.time()

    def record_end(self):
        self.end_time = time.time()

    def add_suite(self, suite_report):
        self.suite_reports.append(suite_report)

    def get_summary(self):
        time_taken = self.end_time - self.start_time
        sb = ["Summary of all suites: {:d} suites ran in {:0.2f} seconds".format(
            len(self.suite_reports), time_taken)]
        for suite_report in self.suite_reports:
            sb.append("    {}: {}".format(suite_report.suite_name, suite_report.get_summary()))
        return "\n".join(sb)


class SuiteReport(object):
    def __init__(self, suite_name, nb_tests):
        self.suite_name = suite_name
        self.nb_tests = nb_tests
        self.suite_start = None
        self.suite_end = None

        self.interrupted = False
        self.return_code = None

        # TODO Replace with better handling of times
        self.execution_start_times = []
        self.execution_end_times = []
        self.executions = []
        self.current_execution = None

    def record_suite_start(self):
        assert self.suite_start is None, "record_suite_start() can only be called once."
        self.suite_start = time.time()

    def record_suite_end(self):
        assert self.suite_start is not None, "cannot end a suite that was not started."
        assert self.suite_end is None, "record_suite_end() can only be called once."
        self.suite_end = time.time()

    def record_execution_start(self):
        assert self.current_execution is None, ("Cannot start an execution, "
                                                "previous execution was not stopped.")
        self.execution_start_times.append(time.time())
        self.current_execution = []

    def record_execution_end(self):
        assert self.current_execution is not None, ("Cannot stop an execution, "
                                                    "no execution in progress.")
        report_infos = [report.get_info() for report in self.current_execution]
        self.executions.append(TestReportInfo.combine(report_infos))
        self.execution_end_times.append(time.time())
        self.current_execution = None

    # FIXME we shouldn't need these parameters
    def create_test_report(self, job_logger, suite_options):
        test_report = TestReport(job_logger, suite_options)
        assert self.current_execution is not None
        self.current_execution.append(test_report)
        return test_report

    def set_success(self):
        self.return_code = 0

    def set_interrupted(self, return_code):
        self.interrupted = True
        self.return_code = return_code

    def set_error(self, return_code):
        self.return_code = return_code

    def get_summary(self):
        header, details = self._get_summary()
        summary = "Summary of {} suite: {}".format(self.suite_name, header)
        if details:
            summary += "\n" + details
        return summary

    def _get_all_executions(self):
        executions = copy.copy(self.executions)
        start_times = copy.copy(self.execution_start_times)
        end_times = copy.copy(self.execution_end_times)
        if self.current_execution:
            report_infos = [report.get_info() for report in self.current_execution]
            executions.append(TestReportInfo.combine(report_infos))
            end_times.append(time.time())
        return start_times, end_times, executions

    def _get_summary(self):
        start_times, end_times, executions = self._get_all_executions()
        nb_executions = len(executions)

        if nb_executions == 0:
            return "No tests ran", None
        elif nb_executions == 1:
            report = executions[0]
            time_taken = end_times[0] - start_times[0]
            return self._get_single_execution_summary(report, time_taken)
        else:
            return self._get_multi_executions_summary(executions, start_times, end_times)

    def _get_single_execution_summary(self, report, time_taken):
        if report.wasSuccessfull():
            header = "All tests passed."
        else:
            header = "Failures or errors occured."
        return header, "\n".join(self._get_report_summary(report, time_taken))

    def _get_multi_executions_summary(self, reports, start_times, end_times):
        time_taken = end_times[-1] - start_times[0]
        header = "Executed {:d} times in {:0.2f} seconds.".format(len(reports), time_taken)
        combined_report = TestReportInfo.combine(reports)
        sb = self._get_report_summary(combined_report, time_taken, details=False)
        execution_nb = 1
        for report, start_time, end_time in zip(reports, start_times, end_times):
            sb.append("* Execution #{:d}:".format(execution_nb))
            report_time_taken = end_time - start_time
            report_sb = self._get_report_summary(report, report_time_taken)
            sb.extend(self._indent(report_sb))
            execution_nb += 1
        return header, "\n".join(sb)

        # if nb_executions == 0:
        #     return "No tests ran.", None
        # elif nb_executions == 1:
        #     start_time = start_times[0]
        #     end_time = end_times[0]
        #     report = executions[0]
        # else:
        #     start_time = start_times[0]
        #     end_time = end_times[-1]
        #     report = TestReportInfo.combine(executions)

        # time_taken = end_time - start_time

        # if nb_executions > 1:
        #     header = "Executed {:d} times in {:0.2f} seconds.".format(nb_executions, time_taken)
        # elif report.was_successful():
        #     header = "All tests passed."
        # else:
        #     header = "Failures or errors occured."

        # details = "\n".join(self._get_report_summary(report, time_taken))
        # return header, details

    def _get_report_summary(self, test_report, time_taken, details=True):
        num_failed = test_report.num_failed + test_report.num_interrupted
        num_run = test_report.num_succeeded + test_report.num_errored + num_failed
        num_skipped = self.nb_tests + test_report.num_dynamic - num_run
        sb = [("{:d} test(s) ran in {:0.2f} seconds "
               "({:d} succeeded, {:d} were skipped, "
               "{:d} failed, {:d} errored)").format(
            num_run, time_taken, test_report.num_succeeded,
            num_skipped, num_failed, test_report.num_errored)]
        if not details:
            return sb
        if num_failed > 0:
            sb.append("The following tests failed (with exit code):")
            for test_info in itertools.chain(test_report.get_failed(),
                                             test_report.get_interrupted()):
                sb.append("    {} ({:d})".format(test_info.test_id, test_info.return_code))
        if test_report.num_errored > 0:
            sb.append("The following tests had errors:")
            for test_info in test_report.get_errored():
                sb.append("    {}".format(test_info.test_id))
        return sb

    def get_last_execution_summary(self):
        if not self.executions:
            return "Summary: No execution."
        nb_finished_executions = len(self.executions)
        report = self.executions[nb_finished_executions - 1]
        start_time = self.execution_start_times[nb_finished_executions - 1]
        end_time = self.execution_end_times[nb_finished_executions - 1]
        report_summary = self._get_report_summary(report, end_time - start_time)
        return "Summary: {}".format("\n".join(report_summary))

    @staticmethod
    def _indent(text, nb_spaces=4):
        indent = " " * nb_spaces
        if isinstance(text, str):
            return indent + text
        return [indent + line for line in text]


class TestReportInfo(object):
    def __init__(self):
        self._test_infos = []
        self.num_dynamic = 0
        self.num_succeeded = 0
        self.num_failed = 0
        self.num_errored = 0
        self.num_interrupted = 0

    def clone(self):
        clone = TestReportInfo()
        clone._test_infos = copy.copy(self._test_infos)
        clone.num_dynamic = self.num_dynamic
        clone.num_succeeded = self.num_succeeded
        clone.num_failed = self.num_failed
        clone.num_errored = self.num_errored
        clone.num_interrupted = self.num_interrupted
        return clone

    def end_report(self, end_time):
        for test_info in self._test_infos:
            self._end_test_info(test_info, end_time)

    def _end_test_info(self, test_info, end_time):
        if test_info.status is None or test_info.return_code is None:
            # TODO move this to the reportfile module.
            # Mark the test as having timed out if it was interrupted. It might have
            # passed if the suite ran to completion, but we wouldn't know for sure.
            #
            # Until EVG-1536 is completed, we shouldn't distinguish between failures and
            # interrupted tests in the report.json file. In Evergreen, the behavior to
            # sort tests with the "timeout" test status after tests with the "pass" test
            # status effectively hides interrupted tests from the test result sidebar
            # unless sorting by the time taken.
            test_info.status = STATUS_TIMEOUT
            test_info.evergreen_status = EVG_STATUS_FAIL
            test_info.return_code = RETURN_CODE_TIMEOUT

        if test_info.end_time is None:
            test_info.end_time = end_time

    @classmethod
    def combine(cls, report_infos):
        combined_info = cls()
        combining_time = time.time()
        for info in report_infos:
            info.end_report(combining_time)
            combined_info._test_infos.extend(info._test_infos)
            combined_info.num_dynamic += info.num_dynamic

        # Recompute number of success, failures, and errors.
        combined_info.num_succeeded = len(combined_info.get_by_status(STATUS_SUCCESS))
        combined_info.num_failed = len(combined_info.get_by_status(STATUS_FAIL))
        combined_info.num_errored = len(combined_info.get_by_status(STATUS_ERROR))
        combined_info.num_interrupted = len(combined_info.get_by_status(STATUS_TIMEOUT))

        return combined_info

    def get_by_status(self, status):
        return [info for info in self._test_infos if info.status == status]

    def get_errored(self):
        return self.get_by_status(STATUS_ERROR)

    def get_interrupted(self):
        return self.get_by_status(STATUS_TIMEOUT)

    def get_failed(self):
        return self.get_by_status(STATUS_FAIL)

    def get_by_id(self, test_id):
        # Search the list backwards to efficiently find the status and timing information of a test
        # that was recently started.
        for test_info in reversed(self._test_infos):
            if test_info.test_id == test_id:
                return test_info
        raise ValueError("Details for {} not found in the report".format(test_id))

    def start_test(self, test_id, test_name, dynamic):
        test_info = _TestInfo(test_id, dynamic)
        test_info.start_time = time.time()
        self._test_infos.append(test_info)
        if dynamic:
            self.num_dynamic += 1
        return test_info

    def stop_test(self, test_id):
        # return time taken
        test_info = self.get_by_id(test_id)
        assert test_info.end_time is None, "Test {} was already marked as stopped".format(test_id)
        test_info.end_time = time.time()
        return test_info.start_time - test_info.end_time

    def add_success(self, test_id, return_code):
        test_info = self.get_by_id(test_id)
        test_info.status = STATUS_SUCCESS
        test_info.evergreen_status = EVG_STATUS_SUCCESS
        test_info.return_code = return_code
        self.num_succeeded += 1

    def set_error(self, test_id, return_code):
        test_info = self.get_by_id(test_id)
        # We don't distinguish between test failures and Python errors in Evergreen.
        test_info.status = STATUS_ERROR
        test_info.evergreen_status = EVG_STATUS_FAIL
        test_info.return_code = return_code
        self.num_errored += 1

    def update_error(self, test_id, return_code):
        test_info = self.get_by_id(test_id)
        assert test_info.end_time is not None, "Test {} was not stopped".format(test_id)
        # We don't distinguish between test failures and Python errors in Evergreen.
        test_info.status = STATUS_ERROR
        test_info.evergreen_status = EVG_STATUS_FAIL
        test_info.return_code = return_code
        self._recompute_nums()

    def add_failure(self, test_id, return_code, report_failure_status):
        test_info = self.get_by_id(test_id)
        test_info.status = STATUS_FAIL
        if test_info.dynamic:
            # Dynamic tests are used for data consistency checks, so the failures are never
            # silenced.
            test_info.evergreen_status = EVG_STATUS_FAIL
        else:
            test_info.evergreen_status = report_failure_status
        test_info.return_code = return_code
        self.num_failed += 1

    def update_failure(self, test_id, return_code, report_failure_status):
        test_info = self.get_by_id(test_id)
        assert test_info.end_time is not None, "Test {} was not stopped".format(test_id)
        test_info.status = STATUS_FAIL
        if test_info.dynamic:
            # Dynamic tests are used for data consistency checks, so the failures are never
            # silenced.
            test_info.evergreen_status = EVG_STATUS_FAIL
        else:
            test_info.evergreen_status = report_failure_status
        test_info.return_code = return_code
        self._recompute_nums()

    def was_successful(self):
        return self.num_failed == self.num_errored == self.num_interrupted == 0

    def _recompute_nums(self):
        self.num_succeeded = len(self.get_by_status(STATUS_SUCCESS))
        self.num_failed = len(self.get_by_status(STATUS_FAIL))
        self.num_errored = len(self.get_by_status(STATUS_ERROR))
        self.num_interrupted = len(self.get_by_status(STATUS_TIMEOUT))


class TestReport(unittest.TestResult):
    """Records test status and timing information."""

    def __init__(self, job_logger, suite_options):
        """Initializes the TestReport with the buildlogger configuration."""
        unittest.TestResult.__init__(self)

        self.job_logger = job_logger
        self.suite_options = suite_options
        self._lock = threading.Lock()
        self._report_info = TestReportInfo()
        self.__original_loggers = {}

    def get_info(self):
        with self._lock:
            return self._report_info.clone()

    def startTest(self, test, dynamic=False):
        """
        Called immediately before 'test' is run.
        """

        unittest.TestResult.startTest(self, test)

        # test_info = _TestInfo(test.id(), dynamic)
        # test_info.start_time = time.time()

        basename = test.basename()
        if dynamic:
            command = "(dynamic test case)"
        else:
            command = test.as_command()
        self.job_logger.info("Running %s...\n%s", basename, command)

        with self._lock:
            # self._report_info.add_test_info(test_info)
            test_info = self._report_info.start_test(test.id(), basename, dynamic)

        # Set up the test-specific logger.
        test_logger = self.job_logger.new_test_logger(test.short_name(), test.basename(),
                                                      command, test.logger)
        test_info.url_endpoint = test_logger.url_endpoint

        self.__original_loggers[test_info.test_id] = test.logger
        test.logger = test_logger

    def stopTest(self, test):
        """
        Called immediately after 'test' has run.
        """

        unittest.TestResult.stopTest(self, test)

        with self._lock:
            time_taken = self._report_info.stop_test(test.id())
            # test_info = self._find_test_info(test)
            # test_info.end_time = time.time()

        # time_taken = test_info.end_time - test_info.start_time
        self.job_logger.info("%s ran in %0.2f seconds.", test.basename(), time_taken)

        # Asynchronously closes the buildlogger test handler to avoid having too many threads open
        # on 32-bit systems.
        for handler in test.logger.handlers:
            # We ignore the cancellation token returned by close_later() since we always want the
            # logs to eventually get flushed.
            logging.flush.close_later(handler)

        # Restore the original logger for the test.
        #
        # TestReport.combine() doesn't access the '__original_loggers' attribute, so we don't bother
        # protecting it with the lock.
        test.logger = self.__original_loggers.pop(test.id())

    def addError(self, test, err):
        """
        Called when a non-failureException was raised during the
        execution of 'test'.
        """

        unittest.TestResult.addError(self, test, err)

        with self._lock:
            self._report_info.set_error(test.id(), test.return_code)

    def setError(self, test):
        """Used to change the outcome of an existing test to an error."""

        with self._lock:
            self._report_info.update_error(test.id(), return_code=2)

    def addFailure(self, test, err):
        """Called when a failureException was raised during the execution of 'test'."""

        unittest.TestResult.addFailure(self, test, err)

        with self._lock:
            self._report_info.add_failure(
                test.id(), test.return_code, self.suite_options.report_failure_status)

    def setFailure(self, test, return_code=1):
        """
        Used to change the outcome of an existing test to a failure.
        """

        with self._lock:
            self._report_info.update_failure(
                test.id(), test.return_code, self.suite_options.report_failure_status)

    def addSuccess(self, test):
        """
        Called when 'test' executed successfully.
        """

        unittest.TestResult.addSuccess(self, test)

        with self._lock:
            self._report_info.add_success(test.id(), test.return_code)

    def wasSuccessful(self):
        """
        Returns true if all tests executed successfully.
        """

        with self._lock:
            return self._report_info.was_successful()

    def as_dict(self):
        """
        Return the test result information as a dictionary.

        Used to create the report.json file.
        """
        results = []
        with self._lock:
            for test_info in self.test_infos:
                result = {
                    "test_file": test_info.test_id,
                    "status": test_info.evergreen_status,
                    "exit_code": test_info.return_code,
                    "start": test_info.start_time,
                    "end": test_info.end_time,
                    "elapsed": test_info.end_time - test_info.start_time,
                }

                if test_info.url_endpoint is not None:
                    result["url"] = test_info.url_endpoint
                    result["url_raw"] = test_info.url_endpoint + "?raw=1"

                results.append(result)

            return {
                "results": results,
                "failures": self.num_failed + self.num_errored + self.num_interrupted,
            }

    @classmethod
    def from_dict(cls, report_dict):
        """
        Returns the test report instance copied from a dict (generated in as_dict).

        Used when combining reports instances.
        """
        report = cls(logging.loggers.EXECUTOR_LOGGER, _config.SuiteOptions.ALL_INHERITED.resolve())
        for result in report_dict["results"]:
            # By convention, dynamic tests are named "<basename>:<hook name>".
            is_dynamic = ":" in result["test_file"]
            test_info = _TestInfo(result["test_file"], is_dynamic)
            test_info.url_endpoint = result.get("url")
            test_info.status = result["status"]
            test_info.evergreen_status = test_info.status
            test_info.return_code = result["exit_code"]
            test_info.start_time = result["start"]
            test_info.end_time = result["end"]
            report.test_infos.append(test_info)

            if is_dynamic:
                report.num_dynamic += 1

        # Update cached values for number of successful and failed tests.
        report.num_failed = len(report.get_failed())
        report.num_errored = len(report.get_errored())
        report.num_interrupted = len(report.get_interrupted())
        report.num_succeeded = len(report.get_successful())

        return report

    def _find_test_info(self, test):
        """Returns the status and timing information associated with 'test'."""
        # test_id = test.id()
        test_info = self._report_info.get_by_id(test.id())
        if test_info:
            return test_info
        raise ValueError("Details for %s not found in the report" % (test.basename()))


class _TestInfo(object):
    """Holder for the test status and timing information."""

    def __init__(self, test_id, dynamic):
        """Initializes the _TestInfo instance."""
        self.test_id = test_id
        self.dynamic = dynamic

        self.start_time = None
        self.end_time = None
        self.status = None
        self.evergreen_status = None
        self.return_code = None
        self.url_endpoint = None
