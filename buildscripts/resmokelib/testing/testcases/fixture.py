from buildscripts.resmokelib.testing.testcases import interface
from buildscripts.resmokelib.utils import registry


class FixtureTestCase(interface.TestCase):

    REGISTERED_NAME = registry.LEAVE_UNREGISTERED

    def __init__(self, logger, job_name, phase):
        assert phase in ("setup", "teardown")
        interface.TestCase.__init__(self, logger,
                                    "Fixture test", "{}_fixture_{}".format(job_name, phase),
                                    dynamic=True)
        self.job_name = job_name


class FixtureSetupTestCase(FixtureTestCase):

    REGISTERED_NAME = registry.LEAVE_UNREGISTERED
    PHASE = "setup"

    def __init__(self, fixture, job_name):
        FixtureTestCase.__init__(self, fixture.logger, job_name, self.PHASE)
        self.fixture = fixture

    def run_test(self):
        self.return_code = 2
        self.logger.info("Starting the setup of %s", self.fixture)
        self.fixture.setup()
        self.logger.info("Waiting for %s to be ready", self.fixture)
        self.fixture.await_ready()
        self.logger.info("Finished the setup of %s", self.fixture)
        self.return_code = 0


class FixtureTeardownTestCase(FixtureTestCase):

    REGISTERED_NAME = registry.LEAVE_UNREGISTERED
    PHASE = "teardown"

    def __init__(self, fixture, job_name):
        FixtureTestCase.__init__(self, fixture.logger, job_name, self.PHASE)
        self.fixture = fixture

    def run_test(self):
        self.return_code = 2
        self.logger.info("Starting the teardown of %s", self.fixture)
        self.fixture.teardown(finished=True)
        self.logger.info("Finished the teardown of %s", self.fixture)
        self.return_code = 0
