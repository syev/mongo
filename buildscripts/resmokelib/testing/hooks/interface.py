"""Interface for hooks customizing the behavior of a test fixture."""

from __future__ import absolute_import

from ... import errors
from ...logging import loggers
from ...utils import registry


_HOOKS = {}


def make_hook(class_name, *args, **kwargs):
    """Factory function for creating Hook instances."""

    if class_name not in _HOOKS:
        raise ValueError("Unknown hook class '%s'" % class_name)

    return _HOOKS[class_name](*args, **kwargs)


class Hook(object):
    """The common interface all Hooks will inherit from."""

    __metaclass__ = registry.make_registry_metaclass(_HOOKS)

    REGISTERED_NAME = registry.LEAVE_UNREGISTERED

    def __init__(self, hook_logger, fixture, description):
        """Initializes the Hook with the specified fixture."""

        if not isinstance(hook_logger, loggers.HookLogger):
            raise TypeError("logger must be a HookLogger instance")

        self.logger = hook_logger
        self.fixture = fixture
        self.description = description

    def before_suite(self, test_report, job_logger):
        """The test runner calls this exactly once before they start running the suite."""
        pass

    def after_suite(self, test_report, job_logger):
        """
        The test runner calls this exactly once after all tests have
        finished executing. Be sure to reset the behavior back to its
        original state so that it can be run again.
        """
        pass

    def before_test(self, test, test_report, job_logger):
        """Each test will call this before it executes."""
        pass

    def after_test(self, test, test_report, job_logger):
        """Each test will call this after it executes."""
        pass


class TestCaseHook(Hook):
    """A Hook subclass that runs a dynamic TestCase after tests."""
    REGISTERED_NAME = registry.LEAVE_UNREGISTERED

    def __init__(self, hook_logger, fixture, description):
        Hook.__init__(self, hook_logger, fixture, description)

    def _should_run_after_test(self):
        """Indicates if the hook should run after the current test.

        This method returns True and can be overriden by subclasses as necessary.
        """
        return True

    def after_test(self, test, test_report, job_logger):
        """Creates and runs a TestCase if _should_run_after_test() returns True."""
        if not self._should_run_after_test():
            return
        hook_test_case = self._create_test_case(test)
        hook_test_case.configure(self.fixture)
        hook_test_case.run(job_logger, test_report)
        if hook_test_case.return_code != 0:
            raise errors.StopExecution(str(hook_test_case.exception))

    def _create_test_case(self, test):
        """Creates a new TestCase to run after 'test'."""
        test_name = "{}:{}".format(test.short_name(), self.__class__.__name__)
        description = "{0} after running '{1}'".format(self.description, test.short_name())
        return self._create_test_case_impl(test_name, description, test.short_name())

    def _create_test_case_impl(self, test_name, description, base_test_name):
        """Returns a new TestCase instance that will run after the test 'base_test_name'

        This method is to be implemented by subclasses.

        Args:
            test_name: The new TestCase's name.
            description: The new TestCase's description.
            base_test_name: The name of the test the new TestCase will run after.

        Returns:
            An instance of TestCase.
        """
        raise NotImplementedError()
