"""
Testing hook for verifying correctness of initial sync.
"""

from __future__ import absolute_import

import os.path
import random

import bson
import pymongo
import pymongo.errors

from . import interface
from . import cleanup
from ..fixtures import replicaset
from ..testcases import jstest
from ... import errors


DEFAULT_N = cleanup.CleanEveryN.DEFAULT_N


class BackgroundInitialSync(interface.TestCaseHook):
    """
    After every test, this hook checks if a background node has finished initial sync and if so,
    validates it, tears it down, and restarts it.

    This test accepts a parameter 'n' that specifies a number of tests after which it will wait for
    replication to finish before validating and restarting the initial sync node. It also accepts
    a parameter 'use_resync' for whether to restart the initial sync node with resync or by
    shutting it down and restarting it.

    This requires the ReplicaSetFixture to be started with 'start_initial_sync_node=True'. If used
    at the same time as CleanEveryN, the 'n' value passed to this hook should be equal to the 'n'
    value for CleanEveryN.
    """

    def __init__(self, hook_logger, fixture, use_resync=False, n=DEFAULT_N, shell_options=None):
        if not isinstance(fixture, replicaset.ReplicaSetFixture):
            raise ValueError("`fixture` must be an instance of ReplicaSetFixture, not {}".format(
                fixture.__class__.__name__))
        description = "Background Initial Sync"
        interface.TestCaseHook.__init__(self, hook_logger, fixture, description)
        self._use_resync = use_resync
        self._n = n
        self._shell_options = shell_options

    def _create_test_case_impl(self, test_name, description, base_test_name):
        return BackgroundInitialSyncTestCase(test_name, description, base_test_name,
                                             self.fixture, self._shell_options,
                                             self._use_resync, self._n)


class BackgroundInitialSyncTestCase(jstest.JSTestCase):
    def __init__(self, test_name, description, base_test_name,
                 fixture, shell_options, use_resync=False, n=DEFAULT_N):
        js_filename = os.path.join("jstests", "hooks", "run_initial_sync_node_validation.js")
        jstest.JSTestCase.__init__(self, js_filename, shell_options=shell_options, test_kind="Hook",
                                   dynamic=True)
        self.test_name = test_name
        self.description = description
        self.base_test_name = base_test_name

        self.fixture = fixture
        self._use_resync = use_resync
        self._n = n
        self._tests_run = 0
        self._random_restarts = 0

    def run_test(self, test_logger):
        self._tests_run += 1
        sync_node = self.fixture.get_initial_sync_node()
        sync_node_conn = sync_node.mongo_client()

        # If it's been 'n' tests so far, wait for the initial sync node to finish syncing.
        if self._tests_run >= self._n:
            test_logger.info("%d tests have been run against the fixture, waiting for initial sync"
                             " node to go into SECONDARY state",
                             self._tests_run)
            self._tests_run = 0

            cmd = bson.SON([("replSetTest", 1),
                            ("waitForMemberState", 2),
                            ("timeoutMillis", 20 * 60 * 1000)])
            sync_node_conn.admin.command(cmd)

        # Check if the initial sync node is in SECONDARY state. If it's been 'n' tests, then it
        # should have waited to be in SECONDARY state and the test should be marked as a failure.
        # Otherwise, we just skip the hook and will check again after the next test.
        try:
            state = sync_node_conn.admin.command("replSetGetStatus").get("myState")
            if state != 2:
                if self._tests_run == 0:
                    msg = "Initial sync node did not catch up after waiting 20 minutes"
                    test_logger.exception("{0} failed: {1}".format(self.description, msg))
                    raise errors.TestFailure(msg)

                test_logger.info(
                    "Initial sync node is in state %d, not state SECONDARY (2)."
                    " Skipping BackgroundInitialSync hook for %s",
                    state, self.base_test_name)

                # If we have not restarted initial sync since the last time we ran the data
                # validation, restart initial sync with a 20% probability.
                if self._random_restarts < 1 and random.random() < 0.2:
                    hook_type = "resync" if self._use_resync else "initial sync"
                    test_logger.info("randomly restarting " + hook_type +
                                     " in the middle of " + hook_type)
                    self.__restart_init_sync(sync_node, sync_node_conn, test_logger)
                    self._random_restarts += 1
                return
        except pymongo.errors.OperationFailure:
            # replSetGetStatus can fail if the node is in STARTUP state. The node will soon go into
            # STARTUP2 state and replSetGetStatus will succeed after the next test.
            test_logger.info(
                "replSetGetStatus call failed in BackgroundInitialSync hook, skipping hook for %s",
                self.base_test_name)
            return

        self._random_restarts = 0

        # Run data validation and dbhash checking.
        jstest.JSTestCase.run_test(self, test_logger)

        self.__restart_init_sync(sync_node, sync_node_conn, test_logger)

    # Restarts initial sync by shutting down the node, clearing its data, and restarting it,
    # or by calling resync if use_resync is specified.
    def __restart_init_sync(self, sync_node, sync_node_conn, test_logger):
        if self._use_resync:
            test_logger.info("Calling resync on initial sync node...")
            cmd = bson.SON([("resync", 1), ("wait", 0)])
            sync_node_conn.admin.command(cmd)
        else:
            # Tear down and restart the initial sync node to start initial sync again.
            sync_node.teardown()

            test_logger.info("Starting the initial sync node back up again...")
            sync_node.setup()
            sync_node.await_ready()


class IntermediateInitialSync(interface.TestCaseHook):
    """
    This hook accepts a parameter 'n' that specifies a number of tests after which it will start up
    a node to initial sync, wait for replication to finish, and then validate the data. It also
    accepts a parameter 'use_resync' for whether to restart the initial sync node with resync or by
    shutting it down and restarting it.

    This requires the ReplicaSetFixture to be started with 'start_initial_sync_node=True'.
    """

    def __init__(self, hook_logger, fixture, use_resync=False, n=DEFAULT_N):
        if not isinstance(fixture, replicaset.ReplicaSetFixture):
            raise ValueError("`fixture` must be an instance of ReplicaSetFixture, not {}".format(
                fixture.__class__.__name__))

        description = "Intermediate Initial Sync"
        interface.TestCaseHook.__init__(self, hook_logger, fixture, description)
        self._use_resync = use_resync
        self._n = n
        self._tests_run = 0

    def _should_run_after_test(self):
        self._tests_run += 1

        # If we have not run 'n' tests yet, skip this hook.
        if self._tests_run < self._n:
            return False

        self._tests_run = 0
        return True

    def _create_test_case_impl(self, test_name, description, base_test_name):
        return IntermediateInitialSyncTestCase(test_name, description,
                                               self.fixture, self._use_resync, self._n)


class IntermediateInitialSyncTestCase(jstest.JSTestCase):
    def __init__(self, test_name, description, base_test_name,
                 fixture, use_resync=False, n=DEFAULT_N):
        js_filename = os.path.join("jstests", "hooks", "run_initial_sync_node_validation.js")
        jstest.JSTestCase.__init__(self, js_filename, test_kind="Hook", dynamic=True)

        self.test_name = test_name
        self.description = description
        self.base_test_name = base_test_name

        self.fixture = fixture
        self._use_resync = use_resync
        self._n = n

    def run_test(self, test_logger):
        sync_node = self.fixture.get_initial_sync_node()
        sync_node_conn = sync_node.mongo_client()

        if self._use_resync:
            test_logger.info("Calling resync on initial sync node...")
            cmd = bson.SON([("resync", 1)])
            sync_node_conn.admin.command(cmd)
        else:
            sync_node.teardown()

            test_logger.info("Starting the initial sync node back up again...")
            sync_node.setup()
            sync_node.await_ready()

        # Do initial sync round.
        test_logger.info("Waiting for initial sync node to go into SECONDARY state")
        cmd = bson.SON([("replSetTest", 1),
                        ("waitForMemberState", 2),
                        ("timeoutMillis", 20 * 60 * 1000)])
        sync_node_conn.admin.command(cmd)

        # Run data validation and dbhash checking.
        jstest.JSTestCase.run_test(self, test_logger)
