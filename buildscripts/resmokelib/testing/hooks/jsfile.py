"""
Interface for customizing the behavior of a test fixture by executing a
JavaScript file.
"""

from __future__ import absolute_import

from . import interface
from ..testcases import jstest
from ...utils import registry


class JsHook(interface.TestCaseHook):
    """A hook running a JavaScript file as a dynamic test case after a test has run."""
    REGISTERED_NAME = registry.LEAVE_UNREGISTERED

    def __init__(self, hook_logger, fixture, js_filename, description, shell_options=None):
        """Initializes the JsHook.

        Args:
            hook_logger: A HookLogger instance.
            fixture: The fixture this hook will act on.
            js_filename: The path to the JavaScript file that will be run as a dynamic test case.
            description: A description of this hook.
            shell_options: A dictionary defining options passed to the mongo shell when executing
                the JavaScript file.
        """
        interface.TestCaseHook.__init__(self, hook_logger, fixture, description)
        self._js_file_name = js_filename
        self._shell_options = shell_options

    def _create_test_case_impl(self, test_name, description, base_test_name):
        """Creates a TestCase instance that will run this hook's JavaScript file."""
        test_case = jstest.JSTestCase(self._js_file_name, shell_options=self._shell_options,
                                      test_kind="Hook", dynamic=True)
        test_case.test_name = test_name
        return test_case
