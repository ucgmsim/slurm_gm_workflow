import os
import json

# Shouldn't this be in shared or qcore?
def load(
    directory=os.path.dirname(os.path.abspath(__file__)),
    cfg_name="workflow_config.json",
):
    print("load", directory)
    config_file = os.path.join(directory, cfg_name)
    try:
        with open(config_file) as f:
            config_dict = json.load(f)
            return config_dict

    except IOError:
        print("No %s available on %s" % (cfg_name, directory))
        print(
            "This is a fatal error. Please contact someone " "from the software team."
        )
        exit(1)


def check_cfg_params_path(config_dict, *excludes):
    for param in config_dict.keys():
        if param not in excludes:
            if type(config_dict[param]) is str:
                assert(os.path.exists(config_dict[param]),"file/path no exist for {}.".format(param))
