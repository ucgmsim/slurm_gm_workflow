"""
Module which contains shared functions/values.

@date 8 April 2016
@author Viktor Polak
@contact viktor.polak@canterbury.ac.nz
"""
from __future__ import print_function

import glob
import os
import re
import shutil
import sys
from datetime import datetime
from logging import Logger, DEBUG, INFO

from qcore.qclogging import get_basic_logger


def dict_to_e3d_par(pyfile, vardict):
    with open(pyfile, "w") as fp:
        for (key, value) in vardict.items():
            if isinstance(value, str):
                fp.write('%s="%s"\n' % (key, value))
            else:
                fp.write("%s=%s\n" % (key, value))


def verify_strings(string_list):
    """Makes sure required string are not empty"""
    for variable in string_list:
        if variable == "":
            raise ValueError("Variable is empty: %s. Check " "params.py." % (variable))


def verify_user_dirs(dir_list):
    """Makes sure user dirs (ones that may be created if not existing)
    are ready
    """
    for dir_path in dir_list:
        os.makedirs(dir_path, exist_ok=True)


def get_hf_nt(params):
    return int(float(params["sim_duration"]) / float(params["hf"]["dt"]))


def get_site_specific_path(
    stat_file_path,
    site_v1d_dir=None,
    logger: Logger = get_basic_logger(),
):
    logger.info("Auto-detecting site-specific info")
    logger.info("- Station file path: %s" % stat_file_path)

    if site_v1d_dir is None:
        site_v1d_dir = os.path.join(os.path.dirname(stat_file_path), "1D")

    if os.path.exists(site_v1d_dir):
        logger.info("- 1D profiles found at {}".format(site_v1d_dir))
    else:
        logger.critical("Error: No such path exists: {}".format(site_v1d_dir))
        sys.exit()
    return site_v1d_dir
