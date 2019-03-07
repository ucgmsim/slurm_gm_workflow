import os
import json
import pickle

# Shouldn't this be in shared or qcore?

import pickle
import inspect

TEST_DATA_SAVE_DIR = "/nesi/nobackup/nesi00213/tmp/test_space/slurm_gm_workflow/pickled"
REALISATION = "PangopangoF29_HYP01-10_S1244"
DATA_TAKEN = {}
INPUT_DIR = 'input'
OUTPUT_DIR = 'output'


def load(
    directory=os.path.dirname(os.path.abspath(__file__)),
    cfg_name="workflow_config.json",
):
    frame = inspect.currentframe()
    args, _, _, values = inspect.getargvalues(frame)
    func_name = inspect.getframeinfo(frame)[2]
    if not DATA_TAKEN.get(func_name):
        for arg in args:
            with open(os.path.join(TEST_DATA_SAVE_DIR, REALISATION, INPUT_DIR, func_name + '_{}.P'.format(arg)),
                      'wb') as save_file:
                pickle.dump(values[arg], save_file)
    print("load", directory)
    config_file = os.path.join(directory, cfg_name)
    try:
        with open(config_file) as f:
            config_dict = json.load(f)
            if not DATA_TAKEN.get(func_name):
                with open(os.path.join(TEST_DATA_SAVE_DIR, REALISATION, OUTPUT_DIR, func_name + '_ret_val.P'),
                          'wb') as save_file:
                    pickle.dump(config_dict, save_file)
                DATA_TAKEN[func_name] = True
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
                assert os.path.exists(config_dict[param]), "file/path no exist for {}.".format(param)
