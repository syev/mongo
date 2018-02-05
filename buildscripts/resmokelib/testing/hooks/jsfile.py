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
        test_case = jstest.JSTestCase(js_filename, shell_options=shell_options, test_kind="Hook",
                                      dynamic=True)
        interface.TestCaseHook.__init__(self, hook_logger, fixture, description, test_case)
