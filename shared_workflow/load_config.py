
import sys
import os
import json

def load():
    directory = os.path.dirname(os.path.abspath(__file__))
    config_file = os.path.join(directory, "workflow_config.json")
    try:
        with open(config_file) as f:
            config_dict = json.load(f)
            return config_dict

    except IOError:
        print "No workflow_config.json available on %s" % directory
        print "This is a fatal error. Please contact someone from the software team."
        exit(1)



