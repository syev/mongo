"""TestCase for C++ integration tests."""

from __future__ import absolute_import

from . import interface
from ... import core
from ... import errors
from ... import utils


class CPPIntegrationTestCase(interface.TestCase):
    """
    A C++ integration test to execute.
    """

    REGISTERED_NAME = "cpp_integration_test"

    def __init__(self,
                 program_executable,
                 program_options=None):
        """
        Initializes the CPPIntegrationTestCase with the executable to run.
        """

        interface.TestCase.__init__(self, "Program", program_executable)

        self.program_executable = program_executable
        self.program_options = utils.default_if_none(program_options, {}).copy()

    def configure(self, fixture, *args, **kwargs):
        interface.TestCase.configure(self, fixture, *args, **kwargs)

        self.program_options["connectionString"] = self.fixture.get_internal_connection_string()

    def run_test(self, test_logger):
        try:
            program = self._make_process(test_logger)
            self._execute(test_logger, program)
        except errors.TestFailure:
            raise
        except:
            test_logger.exception("Encountered an error running C++ integration test %s.",
                                  self.basename())
            raise

    def _make_process(self, test_logger):
        return core.programs.generic_program(test_logger,
                                             [self.program_executable],
                                             **self.program_options)
