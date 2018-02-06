"""
Parser for command line arguments.
"""

from __future__ import absolute_import

import collections
import os
import os.path
import optparse

from . import config as _config
from . import utils
from .. import resmokeconfig


# Mapping of the attribute of the parsed arguments (dest) to its key as it appears in the options
# YAML configuration file. Most should only be converting from snake_case to camelCase.
DEST_TO_CONFIG = {
    "base_port": "basePort",
    "buildlogger_url": "buildloggerUrl",
    "continue_on_failure": "continueOnFailure",
    "dbpath_prefix": "dbpathPrefix",
    "dbtest_executable": "dbtest",
    "distro_id": "distroId",
    "dry_run": "dryRun",
    "exclude_with_any_tags": "excludeWithAnyTags",
    "include_with_any_tags": "includeWithAnyTags",
    "jobs": "jobs",
    "mongo_executable": "mongo",
    "mongod_executable": "mongod",
    "mongod_parameters": "mongodSetParameters",
    "mongos_executable": "mongos",
    "mongos_parameters": "mongosSetParameters",
    "no_journal": "nojournal",
    "num_clients_per_fixture": "numClientsPerFixture",
    "patch_build": "patchBuild",
    "prealloc_journal": "preallocJournal",
    "repeat": "repeat",
    "report_failure_status": "reportFailureStatus",
    "report_file": "reportFile",
    "seed": "seed",
    "service_executor": "serviceExecutor",
    "shell_conn_string": "shellConnString",
    "shell_port": "shellPort",
    "shell_read_mode": "shellReadMode",
    "shell_write_mode": "shellWriteMode",
    "shuffle": "shuffle",
    "stagger_jobs": "staggerJobs",
    "storage_engine": "storageEngine",
    "storage_engine_cache_size": "storageEngineCacheSizeGB",
    "tag_file": "tagFile",
    "task_id": "taskId",
    "task_name": "taskName",
    "transport_layer": "transportLayer",
    "variant_name": "variantName",
    "wt_coll_config": "wiredTigerCollectionConfigString",
    "wt_engine_config": "wiredTigerEngineConfigString",
    "wt_index_config": "wiredTigerIndexConfigString"
}


ResmokeConfig = collections.namedtuple(
    "ResmokeConfig", ["list_suites", "find_suites", "dry_run", "suite_files", "test_files",
                      "include_with_any_tags", "exclude_with_any_tags", "logging_config"])


def _make_parser():
    parser = optparse.OptionParser()

    parser.add_option("--suites", dest="suite_files", metavar="SUITE1,SUITE2",
                      help=("Comma separated list of YAML files that each specify the configuration"
                            " of a suite. If the file is located in the resmokeconfig/suites/"
                            " directory, then the basename without the .yml extension can be"
                            " specified, e.g. 'core'. If a list of files is passed in as"
                            " positional arguments, they will be run using the suites'"
                            " configurations"))

    parser.add_option("--log", dest="logger_file", metavar="LOGGER",
                      help=("A YAML file that specifies the logging configuration. If the file is"
                            " located in the resmokeconfig/suites/ directory, then the basename"
                            " without the .yml extension can be specified, e.g. 'console'."))

    parser.add_option("--options", dest="options_file", metavar="OPTIONS",
                      help="A YAML file that specifies global options to resmoke.py.")

    parser.add_option("--basePort", dest="base_port", metavar="PORT",
                      help=("The starting port number to use for mongod and mongos processes"
                            " spawned by resmoke.py or the tests themselves. Each fixture and Job"
                            " allocates a contiguous range of ports."))

    parser.add_option("--buildloggerUrl", action="store", dest="buildlogger_url", metavar="URL",
                      help="The root url of the buildlogger server.")

    parser.add_option("--continueOnFailure", action="store_true", dest="continue_on_failure",
                      help="Executes all tests in all suites, even if some of them fail.")

    parser.add_option("--dbpathPrefix", dest="dbpath_prefix", metavar="PATH",
                      help=("The directory which will contain the dbpaths of any mongod's started"
                            " by resmoke.py or the tests themselves."))

    parser.add_option("--dbtest", dest="dbtest_executable", metavar="PATH",
                      help="The path to the dbtest executable for resmoke to use.")

    parser.add_option("--excludeWithAnyTags", action="append", dest="exclude_with_any_tags",
                      metavar="TAG1,TAG2",
                      help=("Comma separated list of tags. Any jstest that contains any of the"
                            " specified tags will be excluded from any suites that are run."))

    parser.add_option("-f", "--findSuites", action="store_true", dest="find_suites",
                      help="List the names of the suites that will execute the specified tests.")

    parser.add_option("--includeWithAnyTags", action="append", dest="include_with_any_tags",
                      metavar="TAG1,TAG2",
                      help=("Comma separated list of tags. For the jstest portion of the suite(s),"
                            " only tests which have at least one of the specified tags will be"
                            " run."))

    parser.add_option("-n", action="store_const", const="tests", dest="dry_run",
                      help="Output the tests that would be run.")

    # TODO: add support for --dryRun=commands
    parser.add_option("--dryRun", type="choice", action="store", dest="dry_run",
                      choices=("off", "tests"), metavar="MODE",
                      help=("Instead of running the tests, output the tests that would be run"
                            " (if MODE=tests). Defaults to MODE=%default."))

    parser.add_option("-j", "--jobs", type="int", dest="jobs", metavar="JOBS",
                      help=("The number of Job instances to use. Each instance will receive its own"
                            " MongoDB deployment to dispatch tests to."))

    parser.add_option("-l", "--listSuites", action="store_true", dest="list_suites",
                      help="List the names of the suites available to execute.")

    parser.add_option("--mongo", dest="mongo_executable", metavar="PATH",
                      help="The path to the mongo shell executable for resmoke.py to use.")

    parser.add_option("--mongod", dest="mongod_executable", metavar="PATH",
                      help="The path to the mongod executable for resmoke.py to use.")

    parser.add_option("--mongodSetParameters", dest="mongod_parameters",
                      metavar="{key1: value1, key2: value2, ..., keyN: valueN}",
                      help=("Pass one or more --setParameter options to all mongod processes"
                            " started by resmoke.py. The argument is specified as bracketed YAML -"
                            " i.e. JSON with support for single quoted and unquoted keys."))

    parser.add_option("--mongos", dest="mongos_executable", metavar="PATH",
                      help="The path to the mongos executable for resmoke.py to use.")

    parser.add_option("--mongosSetParameters", dest="mongos_parameters",
                      metavar="{key1: value1, key2: value2, ..., keyN: valueN}",
                      help=("Pass one or more --setParameter options to all mongos processes"
                            " started by resmoke.py. The argument is specified as bracketed YAML -"
                            " i.e. JSON with support for single quoted and unquoted keys."))

    parser.add_option("--nojournal", action="store_true", dest="no_journal",
                      help="Disable journaling for all mongod's.")

    parser.add_option("--nopreallocj", action="store_const", const="off", dest="prealloc_journal",
                      help="Disable preallocation of journal files for all mongod processes.")

    parser.add_option("--numClientsPerFixture", type="int", dest="num_clients_per_fixture",
                      help="Number of clients running tests per fixture")

    parser.add_option("--preallocJournal", type="choice", action="store", dest="prealloc_journal",
                      choices=("on", "off"), metavar="ON|OFF",
                      help=("Enable or disable preallocation of journal files for all mongod"
                            " processes. Defaults to %default."))

    parser.add_option("--shellConnString", dest="shell_conn_string",
                      metavar="CONN_STRING",
                      help="Override the default fixture and connect to an existing MongoDB"
                           " cluster instead. This is useful for connecting to a MongoDB"
                           " deployment started outside of resmoke.py including one running in a"
                           " debugger.")

    parser.add_option("--shellPort", dest="shell_port", metavar="PORT",
                      help="Convenience form of --shellConnString for connecting to an"
                           " existing MongoDB cluster with the URL mongodb://localhost:[PORT]."
                           " This is useful for connecting to a server running in a debugger.")

    parser.add_option("--repeat", type="int", dest="repeat", metavar="N",
                      help="Repeat the given suite(s) N times, or until one fails.")

    parser.add_option("--reportFailureStatus", type="choice", action="store",
                      dest="report_failure_status", choices=("fail", "silentfail"),
                      metavar="STATUS",
                      help="Controls if the test failure status should be reported as failed"
                           " or be silently ignored (STATUS=silentfail). Dynamic test failures will"
                           " never be silently ignored. Defaults to STATUS=%default.")

    parser.add_option("--reportFile", dest="report_file", metavar="REPORT",
                      help="Write a JSON file with test status and timing information.")

    parser.add_option("--seed", type="int", dest="seed", metavar="SEED",
                      help=("Seed for the random number generator. Useful in combination with the"
                            " --shuffle option for producing a consistent test execution order."))

    parser.add_option("--serviceExecutor", dest="service_executor", metavar="EXECUTOR",
                      help="The service executor used by jstests")

    parser.add_option("--transportLayer", dest="transport_layer", metavar="TRANSPORT",
                      help="The transport layer used by jstests")

    parser.add_option("--shellReadMode", type="choice", action="store", dest="shell_read_mode",
                      choices=("commands", "compatibility", "legacy"), metavar="READ_MODE",
                      help="The read mode used by the mongo shell.")

    parser.add_option("--shellWriteMode", type="choice", action="store", dest="shell_write_mode",
                      choices=("commands", "compatibility", "legacy"), metavar="WRITE_MODE",
                      help="The write mode used by the mongo shell.")

    parser.add_option("--shuffle", action="store_const", const="on", dest="shuffle",
                      help=("Randomize the order in which tests are executed. This is equivalent"
                            " to specifying --shuffleMode=on."))

    parser.add_option("--shuffleMode", type="choice", action="store", dest="shuffle",
                      choices=("on", "off", "auto"), metavar="ON|OFF|AUTO",
                      help=("Control whether to randomize the order in which tests are executed."
                            " Defaults to auto when not supplied. auto enables randomization in"
                            " all cases except when the number of jobs requested is 1."))

    parser.add_option("--staggerJobs", type="choice", action="store", dest="stagger_jobs",
                      choices=("on", "off"), metavar="ON|OFF",
                      help=("Enable or disable the stagger of launching resmoke jobs."
                            " Defaults to %default."))

    parser.add_option("--storageEngine", dest="storage_engine", metavar="ENGINE",
                      help="The storage engine used by dbtests and jstests.")

    parser.add_option("--storageEngineCacheSizeGB", dest="storage_engine_cache_size",
                      metavar="CONFIG", help="Set the storage engine cache size configuration"
                                             " setting for all mongod's.")

    parser.add_option("--tagFile", dest="tag_file", metavar="OPTIONS",
                      help="A YAML file that associates tests and tags.")

    parser.add_option("--wiredTigerCollectionConfigString", dest="wt_coll_config", metavar="CONFIG",
                      help="Set the WiredTiger collection configuration setting for all mongod's.")

    parser.add_option("--wiredTigerEngineConfigString", dest="wt_engine_config", metavar="CONFIG",
                      help="Set the WiredTiger engine configuration setting for all mongod's.")

    parser.add_option("--wiredTigerIndexConfigString", dest="wt_index_config", metavar="CONFIG",
                      help="Set the WiredTiger index configuration setting for all mongod's.")

    parser.add_option("--executor", dest="executor_file",
                      help="OBSOLETE: Superceded by --suites; specify --suites=SUITE path/to/test"
                           " to run a particular test under a particular suite configuration.")

    evergreen_options = optparse.OptionGroup(
        parser,
        title="Evergreen options",
        description=("Options used to propagate information about the Evergreen task running this"
                     " script."))
    parser.add_option_group(evergreen_options)

    evergreen_options.add_option("--distroId", dest="distro_id", metavar="DISTRO_ID",
                                 help=("Set the identifier for the Evergreen distro running the"
                                       " tests."))

    evergreen_options.add_option("--patchBuild", action="store_true", dest="patch_build",
                                 help=("Indicate that the Evergreen task running the tests is a"
                                       " patch build."))

    evergreen_options.add_option("--taskName", dest="task_name", metavar="TASK_NAME",
                                 help="Set the name of the Evergreen task running the tests.")

    evergreen_options.add_option("--taskId", dest="task_id", metavar="TASK_ID",
                                 help="Set the Id of the Evergreen task running the tests.")

    evergreen_options.add_option("--variantName", dest="variant_name", metavar="VARIANT_NAME",
                                 help=("Set the name of the Evergreen build variant running the"
                                       " tests."))

    parser.set_defaults(logger_file="console",
                        dry_run="off",
                        find_suites=False,
                        list_suites=False,
                        suite_files="with_server",
                        prealloc_journal="off",
                        shuffle="auto",
                        stagger_jobs="off")
    return parser


def parse_command_line():
    """
    Parses the command line arguments passed to resmoke.py.
    """
    parser = _make_parser()
    options, args = parser.parse_args()

    _validate_options(parser, options, args)
    _update_config_vars(options)

    return ResmokeConfig(
        list_suites=options.list_suites,
        find_suites=options.find_suites,
        dry_run=options.dry_run,
        suite_files=options.suite_files.split(","),
        test_files=args,
        include_with_any_tags=options.include_with_any_tags,
        exclude_with_any_tags=options.exclude_with_any_tags,
        logging_config=_get_logging_config(options.logger_file))


def _validate_options(parser, options, args):
    """
    Do preliminary validation on the options and error on any invalid options.
    """

    if options.shell_port is not None and options.shell_conn_string is not None:
        parser.error("Cannot specify both `shellPort` and `shellConnString`")

    if options.executor_file:
        parser.error("--executor is superseded by --suites; specify --suites={} {} to run the"
                     "test(s) under those suite configuration(s)"
                     .format(options.executor_file, " ".join(args)))


def _update_config_vars(values):
    # file name -> "options" section of the yaml file
    options = _get_options_config(values.options_file)

    config = _config.DEFAULTS.copy()
    config.update(options)

    values = vars(values)
    for dest in values:
        if dest not in DEST_TO_CONFIG:
            continue
        config_var = DEST_TO_CONFIG[dest]
        if values[dest] is not None:
            config[config_var] = values[dest]

    _config.BASE_PORT = int(config.pop("basePort"))
    _config.BUILDLOGGER_URL = config.pop("buildloggerUrl")
    _config.DBPATH_PREFIX = _expand_user(config.pop("dbpathPrefix"))
    _config.DBTEST_EXECUTABLE = _expand_user(config.pop("dbtest"))
    _config.DRY_RUN = config.pop("dryRun")
    _config.EVERGREEN_DISTRO_ID = config.pop("distroId")
    _config.EVERGREEN_PATCH_BUILD = config.pop("patchBuild")
    _config.EVERGREEN_TASK_ID = config.pop("taskId")
    _config.EVERGREEN_TASK_NAME = config.pop("taskName")
    _config.EVERGREEN_VARIANT_NAME = config.pop("variantName")
    _config.EXCLUDE_WITH_ANY_TAGS = _tags_from_list(config.pop("excludeWithAnyTags"))
    _config.FAIL_FAST = not config.pop("continueOnFailure")
    _config.INCLUDE_WITH_ANY_TAGS = _tags_from_list(config.pop("includeWithAnyTags"))
    _config.JOBS = config.pop("jobs")
    _config.MONGO_EXECUTABLE = _expand_user(config.pop("mongo"))
    _config.MONGOD_EXECUTABLE = _expand_user(config.pop("mongod"))
    _config.MONGOD_SET_PARAMETERS = config.pop("mongodSetParameters")
    _config.MONGOS_EXECUTABLE = _expand_user(config.pop("mongos"))
    _config.MONGOS_SET_PARAMETERS = config.pop("mongosSetParameters")
    _config.NO_JOURNAL = config.pop("nojournal")
    _config.NO_PREALLOC_JOURNAL = config.pop("preallocJournal") == "off"
    _config.NUM_CLIENTS_PER_FIXTURE = config.pop("numClientsPerFixture")
    _config.RANDOM_SEED = config.pop("seed")
    _config.REPEAT = config.pop("repeat")
    _config.REPORT_FAILURE_STATUS = config.pop("reportFailureStatus")
    _config.REPORT_FILE = config.pop("reportFile")
    _config.SERVICE_EXECUTOR = config.pop("serviceExecutor")
    _config.SHELL_READ_MODE = config.pop("shellReadMode")
    _config.SHELL_WRITE_MODE = config.pop("shellWriteMode")
    _config.STAGGER_JOBS = config.pop("staggerJobs") == "on"
    _config.STORAGE_ENGINE = config.pop("storageEngine")
    _config.STORAGE_ENGINE_CACHE_SIZE = config.pop("storageEngineCacheSizeGB")
    _config.TAG_FILE = config.pop("tagFile")
    _config.TRANSPORT_LAYER = config.pop("transportLayer")
    _config.WT_COLL_CONFIG = config.pop("wiredTigerCollectionConfigString")
    _config.WT_ENGINE_CONFIG = config.pop("wiredTigerEngineConfigString")
    _config.WT_INDEX_CONFIG = config.pop("wiredTigerIndexConfigString")

    shuffle = config.pop("shuffle")
    if shuffle == "auto":
        # If the user specified a value for --jobs > 1 (or -j > 1), then default to randomize
        # the order in which tests are executed. This is because with multiple threads the tests
        # wouldn't run in a deterministic order anyway.
        _config.SHUFFLE = _config.JOBS > 1
    else:
        _config.SHUFFLE = shuffle == "on"

    conn_string = config.pop("shellConnString")
    port = config.pop("shellPort")

    if port is not None:
        conn_string = "mongodb://localhost:" + port

    if conn_string is not None:
        _config.SHELL_CONN_STRING = conn_string

    if config:
        raise optparse.OptionValueError("Unknown option(s): %s" % (config.keys()))


def _get_logging_config(pathname):
    """
    Attempts to read a YAML configuration from 'pathname' that describes
    how resmoke.py should log the tests and fixtures.
    """

    # Named loggers are specified as the basename of the file, without the .yml extension.
    if not utils.is_yaml_file(pathname) and not os.path.dirname(pathname):
        if pathname not in resmokeconfig.NAMED_LOGGERS:
            raise optparse.OptionValueError("Unknown logger '%s'" % pathname)
        pathname = resmokeconfig.NAMED_LOGGERS[pathname]  # Expand 'pathname' to full path.

    if not utils.is_yaml_file(pathname) or not os.path.isfile(pathname):
        raise optparse.OptionValueError("Expected a logger YAML config, but got '%s'" % pathname)

    return utils.load_yaml_file(pathname).pop("logging")


def _get_options_config(pathname):
    """
    Attempts to read a YAML configuration from 'pathname' that describes
    any modifications to global options.
    """

    if pathname is None:
        return {}

    return utils.load_yaml_file(pathname).pop("options")


def _expand_user(pathname):
    """
    Wrapper around os.path.expanduser() to do nothing when given None.
    """
    if pathname is None:
        return None
    return os.path.expanduser(pathname)


def _tags_from_list(tags_list):
    """
    Returns the list of tags from a list of tag parameter values.

    Each parameter value in the list may be a list of comma separated tags, with empty strings
    ignored.
    """
    tags = []
    if tags_list is not None:
        for tag in tags_list:
            tags.extend([t for t in tag.split(",") if t != ""])
    return tags
