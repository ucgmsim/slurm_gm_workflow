# update adv_im models in root_params.yaml
# written for the run_adv_im.sh wrapper
import argparse

from qcore import constants as const
from qcore.utils import load_yaml, dump_yaml

def parse_args():
    parser = argparse.ArgumentParser()
    
    parser.add_argument("path_rootyaml", type=str, help='the path to root_params.yaml')
    parser.add_argument("models", nargs='+', help='list of models to run')

    args = parser.parse_args()

    return args

def main(path_rootyaml, models):
    
    root_params = load_yaml(path_rootyaml)
    
    # create key with empty value if key no exist
    if not (const.ProcessType.advanced_IM.str_value in root_params.keys()):
        root_params[const.ProcessType.advanced_IM.str_value] = {}
    # update value for models
    root_params[const.ProcessType.advanced_IM.str_value]['models'] = models
    # extra options for advanced_IM
    if not ('match_obs_stations' in root_params[const.ProcessType.advanced_IM.str_value]):
        root_params[const.ProcessType.advanced_IM.str_value]['match_obs_stations'] = True
    # dump updated dictionary, overwrites original    
    dump_yaml(root_params,path_rootyaml)    
    

if __name__ == "__main__":
    args = parse_args()
    main(args.path_rootyaml, args.models)
