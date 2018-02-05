"""Interface for hooks customizing the behavior of a test fixture."""

from __future__ import absolute_import

from ... import errors
from ...logging import loggers
from ...utils import registry


_HOOKS = {}


def make_custom_behavior(class_name, *args, **kwargs):
    """
    Factory function for creating CustomBehavior instances.
    """

    if class_name not in _HOOKS:
        raise ValueError("Unknown custom behavior class '%s'" % class_name)

    return _HOOKS[class_name](*args, **kwargs)


class CustomBehavior(object):
    """
    The common interface all CustomBehaviors will inherit from.
    """

    __metaclass__ = registry.make_registry_metaclass(_HOOKS)

    REGISTERED_NAME = registry.LEAVE_UNREGISTERED

    def __init__(self, hook_logger, fixture, description):
        """
        Initializes the CustomBehavior with the specified fixture.
        """

        if not isinstance(hook_logger, loggers.HookLogger):
            raise TypeError("logger must be a HookLogger instance")

        self.logger = hook_logger
        self.fixture = fixture
        self.description = description

    def before_suite(self, test_report, job_logger):
        """
        The test runner calls this exactly once before they start
        running the suite.
        """
        pass

    def after_suite(self, test_report, job_logger):
        """
        The test runner calls this exactly once after all tests have
        finished executing. Be sure to reset the behavior back to its
        original state so that it can be run again.
        """
        pass

    def before_test(self, test, test_report, job_logger):
        """
        Each test will call this before it executes.
        """
        pass

    def after_test(self, test, test_report, job_logger):
        """
        Each test will call this after it executes.
        """
        pass


class TestCaseHook(CustomBehavior):
    REGISTERED_NAME = registry.LEAVE_UNREGISTERED

    def __init__(self, hook_logger, fixture, description):
        CustomBehavior.__init__(self, hook_logger, fixture, description)

    def _should_run_after_test(self):
        return True

    def after_test(self, test, test_report, job_logger):
        if not self._should_run_after_test():
            return
        hook_test_case = self._create_test_case(test)
        hook_test_case.configure(self.fixture)
        # self.hook_test_case.reset()
        # Change test_name and description to be more descriptive.
        # description = "{0} after running '{1}'".format(self.description, test.short_name())
        # test_name = "{}:{}".format(test.short_name(), self.__class__.__name__)
        # self.hook_test_case.test_name = test_name
        hook_test_case.run(job_logger, test_report)
        if hook_test_case.return_code != 0:
            raise errors.StopExecution("TODO")

    def _create_test_case(self, test):
        test_name = "{}:{}".format(test.short_name(), self.__class__.__name__)
        description = "{0} after running '{1}'".format(self.description, test.short_name())
        return self._create_test_case_impl(test_name, description, test.short_name())

    def _create_test_case_impl(self, test_name, description, base_test_name):
        raise NotImplementedError()

# class HookTestCase(object):
#     def __init__(self, hook_name):
#         self.hook_name
#         self.base_test = None
#
#     def set_test(self, test):
#         self.reset()
#         test_name = "{}:{}".format(test.short_name(), self.hook_name)
