import yaml
import imp


def dump_yaml(input_dict, output_name):
    with open(output_name, 'w') as yaml_file:
        yaml.dump(input_dict, stream=yaml_file, default_flow_style=False)

def create_dict(input_file):
    with open(input_file, 'r') as f:
        lines = f.readlines()
    d = {}
    for line in lines[:-6]:
        if line != '\n' and line != '' and line and not line.startswith("import") and not line.startswith("#") and not line.startswith("__") and not line.startswith("from"):
            print(line)
            p, v = line.split('=')[0].replace("'",'').strip(),line.split('=')[1].split("#")[0].replace("'",'').strip()
            d[p] = v
    print(d)
    return d
  

d = load_py_cfg('/home/melody.zhu/params.py')
d= create_dict('/home/melody.zhu/params.py')
d.update(create_dict('/home/melody.zhu/params_base.py'))
dump_yaml(d, "params.yaml")


