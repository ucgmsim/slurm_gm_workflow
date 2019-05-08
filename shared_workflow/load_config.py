import os
import json
from shared_workflow.shared import get_basic_logger
from logging import Logger


# Shouldn't this be in shared or qcore?
def load(
    directory=os.path.abspath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "../scripts")
    ),
    cfg_name="workflow_config.json",
    logger: Logger = get_basic_logger(),
):
    config_file = os.path.join(directory, cfg_name)
    logger.debug("Attempting to load {}".format(config_file))
    try:
        with open(config_file) as f:
            config_dict = json.load(f)
        logger.debug("{} loaded successfully".format(config_file))
        return config_dict

    except IOError:
        logger.critical("No {} available on {}. This is a fatal error. Please contact someone from the software team.".format(cfg_name, directory))
        exit(1)
