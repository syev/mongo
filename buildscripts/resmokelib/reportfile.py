"""
Manages interactions with the report.json file.
"""

from __future__ import absolute_import

import json

from . import config


def write_evergreen_report(resmoke_report):
    """Writes the report file for the resmoke execution if the --reportFile option was specified."""
    if config.REPORT_FILE is None:
        return
    combined_report_dict = resmoke_report.get_combined_report().as_dict()
    with open(config.REPORT_FILE, "w") as fp:
        json.dump(combined_report_dict, fp)
