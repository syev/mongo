"""Driver of the test execution framework."""
import threading
import time

from . import fixtures
from . import hooks as _hooks
from . import job as _job
from . import testcases
from .. import config as _config
from .. import errors
from ..core import network
from ..utils import queue as _queue


class TestSuiteExecutor(object):
    """
    Executes a test suite.

    Responsible for setting up and tearing down the fixtures that the
    tests execute against.
    """

    _TIMEOUT = 24 * 60 * 60  # =1 day (a long time to have tests run)

    def __init__(self, exec_logger, suite, suite_report):
        self.logger = exec_logger
        self._suite = suite
        self._suite_report = suite_report

        executor_config = suite.get_executor_config()
        self._test_config = executor_config.get("config", {})
        self._hooks_config = executor_config.get("hooks", [])
        self._fixture_config = executor_config.get("fixture")
        self._override_config()

        self._coordinator = _job.JobCoordinator()
        # create jobs
        self._jobs = self._make_jobs()

    def _override_config(self):
        """Performs any override of the default suite configuration needed for this execution."""
        if _config.SHELL_CONN_STRING is not None:
            # Specifying the shellConnString command line option should override the fixture
            # specified in the YAML configuration to be the no-op fixture.
            self.fixture_config = {"class": fixtures.NOOP_FIXTURE_CLASS}

    def run(self):
        try:
            self._run()
            self._suite_report.set_success()
        except errors.UserInterrupt:
            self.logger.warning("Suite execution stopping after user interrupt")
            # Simulate SIGINT as exit code.
            self._suite_report.set_interrupted(return_code=130)
        except IOError:
            self.logger.warning("Suite execution stopping after I/O error")
            # Exit code for IOError on POSIX systems.
            self._suite_report.set_interrupted(return_code=74)
        except errors.FixtureError:
            self.logger.warning("Suite execution stopping after fixture error")
            self._suite_report.set_error(return_code=2)
        except:
            self.logger.exception("Encountered an error when running suite %s (%s).",
                                  self._suite.get_display_name(), self._suite.test_kind)
            self._suite_report.set_error(return_code=2)

    def _run(self):
        self.logger.info("Starting execution of %ss...", self._suite.test_kind)
        self._setup_fixtures()
        last_execution = False
        try:
            num_repeats = self._suite.options.num_repeats
            while num_repeats > 0:
                last_execution = num_repeats == 1
                test_queue = self._make_test_queue()
                self._suite_report.record_execution_start()
                self._run_execution(test_queue, teardown=last_execution)
                self._suite_report.record_execution_end()
                # TODO should this be done in _run_execution?
                if self._coordinator.interrupted:
                    raise errors.UserInterrupt()
                self._log_execution_summary()
                num_repeats -= 1
        finally:
            if not last_execution:
                self._teardown_fixtures()

    def _run_execution(self, test_queue, teardown):
        threads = []
        try:
            for job in self._jobs:
                threads.append(self._start_job(job, test_queue, teardown))
                self._wait_if_stagger_jobs()
            self._wait_for_queue_processing(test_queue)
            for thread in threads:
                thread.join()
        except (KeyboardInterrupt, SystemExit):
            raise errors.UserInterrupt()

    def _log_execution_summary(self):
        # TODO check if this is same as original
        summary = self._suite_report.get_last_execution_summary()
        self.logger.info(summary)

    def _start_job(self, job, test_queue, teardown):
        # FIXME We shouldn't need to pass these
        # TestReport should handle logging and stuff
        test_report = self._suite_report.create_test_report(job.logger, self._suite.options)
        thread = threading.Thread(
            target=job, args=(test_queue, test_report, self._coordinator, teardown))
        thread.daemon = True
        thread.start()
        return thread

    def _wait_if_stagger_jobs(self):
        if _config.STAGGER_JOBS and len(self._jobs) >= 5:
            time.sleep(10)

    @staticmethod
    def _wait_for_queue_processing(test_queue):
        all_tests_run = False
        while not all_tests_run:
            all_tests_run = test_queue.join(TestSuiteExecutor._TIMEOUT)

    def _make_test_queue(self):
        """
        Returns a queue of TestCase instances.

        Use a multi-consumer queue instead of a unittest.TestSuite so
        that the test cases can be dispatched to multiple threads.
        """

        test_queue_logger = self.logger.new_testqueue_logger(self._suite.test_kind)
        # Put all the test cases in a queue.
        queue = _queue.Queue()
        for test_name in self._suite.tests:
            test_case = testcases.make_test_case(self._suite.test_kind,
                                                 test_queue_logger,
                                                 test_name,
                                                 **self._test_config)
            queue.put(test_case)

        # Add sentinel value for each job to indicate when there are no more items to process.
        for _ in xrange(len(self._jobs)):
            queue.put(None)

        return queue

    # #######################
    # Jobs
    # #######################

    def _make_jobs(self):
        # Only start as many jobs as we need. Note this means that the number of jobs we run may not
        # actually be _config.JOBS or self._suite.options.num_jobs.
        jobs_to_start = self._suite.options.num_jobs
        num_tests = len(self._suite.tests)

        if num_tests < jobs_to_start:
            self.logger.info(
                "Reducing the number of jobs from %d to %d since there are only %d test(s) to run.",
                self._suite.options.num_jobs, num_tests, num_tests)
            jobs_to_start = num_tests
        return [self._make_job(job_num) for job_num in xrange(jobs_to_start)]

    def _make_job(self, job_num):
        """Returns a Job instance with its own fixture and hooks."""
        job_logger = self.logger.new_job_logger(self._suite.test_kind, job_num)

        fixture = self._make_fixture(job_num, job_logger)
        hooks = self._make_hooks(fixture)

        # TODO

        # return _job.Job(job_logger, fixture, hooks, report, self._suite.options)
        return _job.Job(job_logger, fixture, hooks, self._suite.options)

    # #######################
    # Fixtures
    # #######################

    def _make_fixture(self, job_num, job_logger):
        """Creates a fixture for a job."""
        fixture_config = {}
        fixture_class = fixtures.NOOP_FIXTURE_CLASS

        if self._fixture_config is not None:
            fixture_config = self._fixture_config.copy()
            fixture_class = fixture_config.pop("class")

        fixture_logger = job_logger.new_fixture_logger(fixture_class)

        return fixtures.make_fixture(fixture_class, fixture_logger, job_num, **fixture_config)

    def _setup_fixtures(self):
        """
        Sets up all the fixtures of the suite.

        Raises:
            FixtureError if a fixture cannot be set up successfully.
        """
        # We reset the internal state of the PortAllocator before calling job.fixture.setup() so
        # that ports used by the fixture during a test suite run earlier can be reused during this
        # current test suite.
        network.PortAllocator.reset()
        for job in self._jobs:
            # setup() must log and throw the FixtureError
            job.fixture.setup()
        for job in self._jobs:
            job.fixture.await_ready()

    def _teardown_fixtures(self):
        """
        Tears down all the fixtures of the suite.

        Raises:
            FixtureError if a fixture cannot be torn down successfully.
        """
        # FIXME make sure we can call this when the fixtures have already been torn down?
        for job in self._jobs:
            job.fixture.teardown(finished=True)

    # #######################
    # Hooks
    # #######################

    def _make_hooks(self, fixture):
        """Creates the custom behaviors for the job's fixture."""

        behaviors = []
        for behavior_config in self._hooks_config:
            behavior_config = behavior_config.copy()
            behavior_class = behavior_config.pop("class")

            hook_logger = self.logger.new_hook_logger(behavior_class, fixture.logger)
            behavior = _hooks.make_custom_behavior(behavior_class,
                                                   hook_logger,
                                                   fixture,
                                                   **behavior_config)
            behaviors.append(behavior)

        return behaviors
