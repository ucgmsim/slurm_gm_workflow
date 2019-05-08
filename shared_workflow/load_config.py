import os
import json
from shared_workflow.shared import log
from logging import DEBUG, CRITICAL


# Shouldn't this be in shared or qcore?
def load(
    directory=os.path.abspath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "../scripts")
    ),
    cfg_name="workflow_config.json",
    logger=None,
):
    config_file = os.path.join(directory, cfg_name)
    log(logger, DEBUG, "Attempting to load {}".format(config_file))
    try:
        with open(config_file) as f:
            config_dict = json.load(f)
        log(logger, DEBUG, "{} loaded successfully".format(config_file))
        return config_dict

    except IOError:
        log(logger, CRITICAL, "No {} available on {}. This is a fatal error. Please contact someone from the software team.".format(cfg_name, directory))
        exit(1)
