import yaml
from collections import OrderedDict


def dump_yaml(input_dict, output_name):
    with open(output_name, 'w') as yaml_file:
        yaml.add_representer(OrderedDict, lambda dumper, data: dumper.represent_mapping('tag:yaml.org,2002:map', data.items()))
        yaml.dump(input_dict, stream=yaml_file, default_flow_style=False)


def create_dict(input_file):
    with open(input_file, 'r') as f:
        lines = f.readlines()
    d = {}
    for line in lines[:-6]:
        if line != '\n' and line != '' and line and not line.startswith("import") and not line.startswith("#") and not line.startswith("__") and not line.startswith("from"):
            print(line)
            if line.startswith('@'):
                k = line.strip().split('@')[-1]
                d[k]=OrderedDict()
            else:
            	p, v = line.split('=')[0].replace("'",'').strip(),line.split('=')[1].split("#")[0].replace("'",'').strip()
            	d[k][p] = v
    print(d)
    return d
  

d= create_dict('/home/melody.zhu/params.py')
d.update(create_dict('/home/melody.zhu/params_base.py'))
dump_yaml(d, "params.yaml")


