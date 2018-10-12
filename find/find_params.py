import os
import subprocess
from qcore import utils
import imp
import glob
import csv


def get_scripts_imports(git_folder, params_dir):
    d = {}
    cmd = "find {} -name '*.py' | xargs grep 'from params'".format(git_folder)
    output = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True).communicate()[0].strip().split('\n')
    for line in output:
        script, imports = line.split(':')
        if not imports.startswith("#"):
            params_name = imports.split('from')[1].split('import')[0].strip()

            if '.py' not in params_name:
                params_name += '.py'

            if params_name in glob.glob1(params_dir, 'params*.py'):
                if d.get(script) is not None:
                    d[script].add(params_name)
                else:
                    d[script] = {params_name}
    return d


def get_params(params_template):
    params = []
    with open(params_template, 'r') as f:
        lines = f.readlines()
    for line in lines:
        if not line.startswith('#') and not line.startswith('if') and '=' in line:
            param = line.split('=')[0]
            params.append((param, 0))
    return params


def get_all_params_dict(params_dir):
    params_template_list = glob.glob1(params_dir, 'params*.py')
    params_dict = {}
    for params_template in params_template_list:
        #  print(params_template)
        params = load_py_cfg(os.path.join(params_dir, params_template))
        params_dict[params_template] = [[k, 0] for k in params.keys()]
    return params_dict


def load_py_cfg(f_path):
    with open(f_path) as f:
        module = imp.load_module('params', f, f_path, ('.py', 'r', imp.PY_SOURCE))
        cfg_dict = module.__dict__
    return cfg_dict


def get_scripts_context(git_dir, script, imports_dict, params_dict):
    log = open("params_usage_{}.txt".format(git_dir.split('/')[-1]), 'a')
    imported_params = imports_dict[script]
    log.write("@{}{}\n".format(script, '-' * 150))
    for params_file in imported_params:
        for i in range(len(params_dict[params_file])):
            param, _ = params_dict[params_file][i]
            if not param.startswith("__"):
                cmd = "cat {} | grep {}".format(script, param)
                output = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True).communicate()[0].strip()
                if output != '':
                    log.write("{}, {}\n".format(params_file, param))
                    params_dict[params_file][i][1] = 1
                    #        print("found", params_file,params_dict[params_file][i][0])
    log.write('\n\n')
    log.close()
    return params_dict


def write_param_dict(outname, header, param_dict):
    with open(outname, 'w') as c:
        cw = csv.writer(c, delimiter=',', quotechar='|')
        cw.writerow(header)
        for k, v in param_dict.items():
            for pa, bol in v:
                if bol == 0 and not pa.startswith('__'):
                    cw.writerow([k, pa])


# TODO params in qcore is: import params.....
p = get_all_params_dict('.')

gs = ['/home/melody.zhu/slurm_gm_workflow', '/home/melody.zhu/qcore', '/home/melody.zhu/visualization']
for g in gs:
    d = get_scripts_imports(g, '.')
    print("Afddsafs", d.keys())
    for s in d.keys():
        print(s)
        p = get_scripts_context(g, s, d, p)
write_param_dict("params_used.csv", ['params_script', 'used_param'], p)
write_param_dict("params_unused.csv", ['params_script', 'unused_param'], p)

