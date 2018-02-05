"""
Interface for customizing the behavior of a test fixture by executing a
JavaScript file.
"""

from __future__ import absolute_import

from . import interface
from ..testcases import jstest
from ...utils import registry


class JsCustomBehavior(interface.TestCaseHook):
    REGISTERED_NAME = registry.LEAVE_UNREGISTERED

    def __init__(self, hook_logger, fixture, js_filename, description, shell_options=None):
        interface.TestCaseHook.__init__(self, hook_logger, fixture, description)
        self._js_file_name = js_filename
        self._shell_options = shell_options

    def _create_test_case_impl(self, test_name, description, base_test_name):
        test_case = jstest.JSTestCase(self._js_file_name, shell_options=self._shell_options,
                                      test_kind="Hook", dynamic=True)
        test_case.test_name = test_name
