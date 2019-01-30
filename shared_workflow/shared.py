"""
Module which contains shared functions/values.

@date 8 April 2016
@author Viktor Polak
@contact viktor.polak@canterbury.ac.nz
"""
from __future__ import print_function

import os
import shutil
import subprocess
import sys
import re
import datetime

import estimation.estimate_wct as wc

from shared_workflow.shared_defaults import tools_dir

if sys.version_info.major == 3:
    basestring = str


def write_to_py(pyfile, vardict):
    with open(pyfile, 'w') as fp:
        for (key, value) in vardict.items():
            if isinstance(value, basestring):
                fp.write('%s="%s"\n' % (key, value))
            else:
                fp.write('%s=%s\n' % (key, value))


def par_value(variable):
    """reads a parameter from the parameters file (e3d.par)
    should not be necessary as you can just 'from params import *' (params.py)
    """
    result = ''
    par_handle = open('e3d.par', 'r')
    for line in par_handle:
        if line.startswith(variable + '='):
            # keep going and get the last result
            result = line
    par_handle.close()
    return ''.join(result.split('=')[1:]).rstrip('\n')


def get_stations(source_file, locations=False):
    """returns a list of stations
    sample line in source file:
    171.74765   -43.90236 ADCS
    """
    stations = []
    station_lats = []
    station_lons = []
    with open(source_file, 'r') as sp:
        for line in sp.readlines():
            if line[0] not in  ['#', '%']:
                info = line.split()
                stations.append(info[2])
                if locations:
                    station_lons.append(info[0])
                    station_lats.append(info[1])
    if not locations:
        return stations
    return (stations, station_lats, station_lons)


def get_corners(model_params, gmt_format = False):
    """Retrieve corners of simulation domain from model params file.
    model_params: file path to model params
    gmt_format: if True, also returns corners in GMT string format
    """
    # with -45 degree rotation:
    #   c2
    # c1  c3
    #   c4
    corners = []
    with open(model_params, 'r') as vmpf:
        lines = vmpf.readlines()
        # make sure they are read in the correct order at efficiency cost
        for corner in ['c1=', 'c2=', 'c3=', 'c4=']:
            for line in lines:
                if corner in line:
                    corners.append(list(map(float, line.split()[1:3])))
    if not gmt_format:
        return corners
    # corners in GMT format
    cnr_str = '\n'.join([' '.join(map(str, cnr)) for cnr in corners])
    return corners, cnr_str


def get_vs(source_file):
    """returns a dictionary of vrefs or vsites
    sample line in source file:
    SITE   VALUE
    """
    vs = {}
    with open(source_file, 'r') as sp:
        lines = sp.readlines()
        print(len(lines))
        for i,line in enumerate(lines):
            line = line.strip('\n')
            if line.startswith('#') or line.startswith('%'): #line is a comment
                continue
            info = line.split()
            if len(info)>=2: #if there are more than 2 columns
                vs[info[0]] = info[1]
            else:
                print("Check this line: %d %s" %(i,line), file=sys.stderr)
                
    return vs


################# Verify Section ###################
# verify functions make sure script resources exist before continuing to run.
# it also creates output directories if not existing.
# very important to prevent (for example) empty variables. 'rm -r emptyvar/*' == 'rm -r /*'
# these functions prevent a script malfunctioning and causing code to run dangerously

class ResourceError(Exception):
    """Exception to throw, prevents scripts from continuing"""
    pass


def verify_files(file_list):
    """Makes sure script file resources exist"""
    for file_path in file_list:
        if not os.path.isfile(file_path):
            raise ResourceError('File not found: %s. '
                                'Check params.py.' % (file_path))


def verify_logfiles(logfile_list):
    """Makes sure logfiles can be created, removes old ones"""
    for logfile in logfile_list:
        # reformat if just filename without path
        if os.path.dirname(logfile) == '':
            logfile = os.path.join(os.getcwd(), logfile)
        # is directory writable?
        if not os.access(os.path.dirname(logfile), os.W_OK):
            raise ResourceError('Can\'t write logfile: %s. '
                                'Check directory permissions.' % (logfile))
        if os.path.exists(logfile):
            os.remove(logfile)
        print("Logfile: %s" %logfile)


def verify_strings(string_list):
    """Makes sure required string are not empty"""
    for variable in string_list:
        if variable == '':
            raise ResourceError('Variable is empty: %s. Check '
                                'params.py.' % (variable))


def verify_lists(list_list):
    """<akes sure list inputs contain values"""
    for req_list in list_list:
        if len(req_list) < 1:
            raise ResourceError('List doesn\'t contain any values: %s. '
                                'Check params.py.' % (req_list))


def verify_dirs(dir_list):
    """Makes sure dirs which should already exist, do exist"""
    for dir_path in dir_list:
        if not os.path.isdir(dir_path):
            raise ResourceError('Directory doesn\'t exist: %s. '
                                'Check params.py' % (dir_path))


def verify_user_dirs(dir_list, reset=False):
    """Makes sure user dirs (ones that may be created if not existing)
    are ready
    """
    for dir_path in dir_list:
        if not os.path.isdir(dir_path):
            os.makedirs(dir_path)
        elif reset:
            # empty directory
            shutil.rmtree(dir_path)
            os.makedirs(dir_path)


def verify_binaries(bin_list):
    """Makes sure binary paths are valid binaries"""
    for bin_path in bin_list:
        if not os.path.isfile(bin_path):
            raise ResourceError('Binary not found: %s. '
                                'Check params.py.' % (bin_path))
        if not os.access(bin_path, os.X_OK):
            raise ResourceError('Binary not executable: %s. '
                                'Check file permissions.' % (bin_path))


def set_permission(dir_path, mode = 0o750, noexec = 0o640, debug=False):
    """Recursively sets permission. mode should be
    given in 0o777 format. eg. 0o750
    """

    print("Permission %s : %o" % (dir_path, mode))
    os.chmod(dir_path, mode)
    for root, dirs, files in os.walk(dir_path):
        for d in dirs:
            if os.path.islink(d):
                continue
            if debug:
                print("Permission %s : %o" % (os.path.join(root, d), mode))
            os.chmod(os.path.join(root, d), mode)
        for f in files:
            if os.path.islink(f):
                continue
            # please do not make every file executable (very bad)
            if f.split('.')[-1] in ['py', 'csh', 'sh']:
                file_mode = mode
            else:
                file_mode = noexec
            if debug:
                print("Permission %s : %o" % (os.path.join(root, f), file_mode))
            os.chmod(os.path.join(root, f), file_mode)


def user_select(options):
    """Gets the user selected number (not index)"""
    try:
        selected_number = input("Enter the number you "
                                "wish to select (1-%d):" %len(options))
    # when is this thrown?
    except NameError:
        print("Check your input.")
        selected_number = user_select(options)
    else:
        try:
            selected_number = int(selected_number)
        except ValueError:
            print("Input should be a number. And one number only.")
            selected_number = user_select(options)
        else:
            try:
                # Check that it is a valid option
                v = options[selected_number-1]
            except IndexError:
                print("Input should be a number in (1-%d)" %len(options))
                selected_number = user_select(options)
    return selected_number


def user_select_multi(options):
    user_inputs = input("Enter the numbers [1-%d] you wish to select "
                        "separated by space (eg. 1 3 4) "
                        "or a/A for All):" %len(options))
    user_inputs_raw = user_inputs.split(' ')
    if len(user_inputs_raw) == 1 and user_inputs_raw[0] in ['a','A']:
        print("You selected all")
        return list(range(1, len(options)+1))
    else:
        selected_numbers = []
        # check if all inputs are numeric
        for v in user_inputs_raw:
            try:
                selected_number = int(v)
            except ValueError:
                print("Value error: %s is not a valid number" %v)
                selected_numbers = user_select_multi(options)
                return selected_numbers
            else:
                if selected_number < 1 or selected_number > len(options):
                    print("Value error: %d is out of range" % selected_number)
                    selected_numbers = user_select_multi(options)
                    return selected_numbers
                selected_numbers.append(selected_number)
        selected_numbers.sort()

        return selected_numbers
         

def show_multiple_choice(options, singular=True):
    for i, option in enumerate(options):
        print("%2d. %s" % (i + 1, option))
    if singular:
        selected_number = user_select(options)
        return options[selected_number-1]
    else:
        selected_numbers = user_select_multi(options)
        selected_options = []
        for i in selected_numbers:
            selected_options.append(options[i-1])
        return selected_options


def show_yes_no_question():
    options = ["Yes", "No"]
    for i, option in enumerate(options):
        print("%2d. %s" % (i + 1, option))
    selected_number = user_select(options)

    # True if selected Yes
    return selected_number == 1


def show_horizontal_line(c='=', length=100):
    print(c*length)


def confirm(q):
    show_horizontal_line()
    print(q)
    return show_yes_no_question()


def confirm_name(name):
    show_horizontal_line()
    print("Automated Name: ", name)
    show_horizontal_line()
    print("Do you wish to proceed?")
    return show_yes_no_question()


def get_input_wc():
    show_horizontal_line()
    try:
        user_input_wc = datetime.datetime.strptime(str(input(
            "Enter the WallClock time limit you "
            "would like to use: ")), "%H:%M:%S").time()
    except ValueError:
        print(r'Input value error. Input does not match format : %H:%M:%S')
        print(r'Must not exceed the limit where: %H <= 23, %M <= 59, %S <= 59')
        user_input_wc = get_input_wc()
    show_horizontal_line()

    return user_input_wc


def set_wct(est_run_time, ncores, auto=False):
    print("Estimated time: {} with {} number of cores".format(
        wc.convert_to_wct(est_run_time), ncores))
    if not auto:
        print("Use the estimated wall clock time? (Minimum of 5 mins, "
              "otherwise adds a 10% overestimation to ensure "
              "the job completes)")
        use_estimation = show_yes_no_question()
    else:
        use_estimation = True

    if use_estimation:
        wct = wc.get_wct(est_run_time)
    else:
        wct = str(get_input_wc())
    print("WCT set to: %s" % wct)
    return wct


def add_name_suffix(name, yes):
    """Allow the user to add a suffix to the name"""
    new_name = name
    #print "Yes? ", yes
    while not yes:
        user_str = input("Add more text (will be appended to the name above)")
        user_str = user_str.replace(" ", "_")
        new_name = name + "_" + user_str
        yes = confirm_name(new_name)
    return new_name


def exe(cmd, debug=True,shell=False, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE):
    """cmd is either a str or a list. but it will be processed as a list.
    this is to accommodate the default shell=False. (for security reason)
    If we wish to support a simple shell command like "echo hello"
    without switching on shell=True, cmd should be given as a list.
    """

    if type(cmd)==str:
        cmd = cmd.split(' ')

    if debug:
        print(' '.join(cmd))

    p = subprocess.Popen(cmd, shell=shell, stdout=stdout, stderr=stderr, encoding='utf-8')
    out, err = p.communicate()
    if debug:
        if out:
            print(out)
        if err:
            print(err, file=sys.stderr)
            print(err) # also printing to stdout (syncing err msg to cmd executed)

    return out, err


def submit_sl_script(script, process, status, mgmt_db_loc, srf_name,
                     timestamp, submit_yes=False):
    """Submits the slurm script and udpates the management db"""
    if submit_yes:
        print("Submitting %s" % script)
        res = exe("sbatch %s" % script, debug=False)
        if len(res[1]) == 0:
            # no errors, return the job id
            jobid = res[0].split()[-1]
            try:
                int(jobid)
            except ValueError:
                print("{} is not a valid jobid. Submitting the "
                      "job most likely failed".format(jobid))
                sys.exit()

            update_db_cmd(process, status, mgmt_db_loc, srf_name, jobid, timestamp)
            return jobid
    else:
        print("User chose to submit the job manually")


def update_db_cmd(process, status, mgmt_db_loc, srf_name, jobid, timestamp):
    """Adds the command to update the mgmt db to the queue"""
    # Update the db
    if mgmt_db_loc is not None and os.path.isdir(mgmt_db_loc):
        db_queue_path = os.path.join(mgmt_db_loc, "mgmt_db_queue")
        cmd_name = os.path.join(db_queue_path, "%s_%s_q" % (timestamp, jobid))
        cmd = "python $gmsim/workflow/scripts/management/" \
              "update_mgmt_db.py {} {} {} --run_name {}  --job {}".format(
            mgmt_db_loc, process, status, srf_name, jobid)
        with open(cmd_name, 'w+') as f:
            f.write(cmd)
            f.close()
    else:
        print("{} is not a valid mgmt db location. No update cmd was created.".format(
            mgmt_db_loc))


### functions mostly used in regression_test
def get_list_of_files(folder_dir):
    # make sure folder_dir ends with /
    folder_dir = os.path.join(folder_dir, '')
    if os.path.isdir(folder_dir):
        list_of_files = os.listdir(folder_dir)
        return list_of_files
    return False


def filter_list_of_files(list_of_files, filter_list):
    for file in list_of_files:
        if file.rsplit('.', 1)[0] not in filter_list:
            list_of_files.remove(file)
    return list_of_files


def get_list_of_prefix(list_of_files):
    list_of_prefix = []
    for file in list_of_files:
        prefix = file.rsplit('.',1)[0]
        if prefix not in list_of_prefix:
            list_of_prefix.append(prefix)
    return list_of_prefix


def check_seismo_files(list_of_stations, list_of_files):
    """check if all station in list has .000 .090 .ver files
    """
    for station in list_of_stations:
        if station + ".000" not in list_of_files:
            return False
        if station + ".090" not in list_of_files:
            return False
        if station + ".ver" not in list_of_files:
            return False
    return True


def last_line(in_file, block_size=1024, ignore_ending_newline=True):
    suffix = ""
    in_file.seek(0, os.SEEK_END)
    in_file_length = in_file.tell()
    seek_offset = 0

    while -seek_offset < in_file_length:
        # Read from end.
        seek_offset -= block_size
        if -seek_offset > in_file_length:
            # Limit if we ran out of file (can't seek backward from start).
            block_size -= -seek_offset - in_file_length
            if block_size == 0:
                break
            seek_offset = -in_file_length
        in_file.seek(seek_offset, os.SEEK_END)
        buf = in_file.read(block_size)

        # Search for line end.
        if ignore_ending_newline and seek_offset == -block_size and buf[-1] == '\n':
            buf = buf[:-1]
        pos = buf.rfind('\n')
        if pos != -1:
            # Found line end.
            return buf[pos+1:] + suffix

        suffix = buf + suffix

    # One-line file.
    return suffix


def get_rlog_count(rlog_dir):
    list_of_rlogs = os.listdir(rlog_dir)
    for file in list_of_rlogs:
        # get the extension of the files
        extension = file.rsplit('.', 1)[1]
        # if it is not an rlog file,
        # remove from list(in case someone created non-rlog-files)
        if extension != "rlog":
            list_of_rlogs.remove(file)
    return len(list_of_rlogs)


def params_to_dict(params_base_path):
    with open(params_base_path, 'r') as f:
        lines = f.readlines()
    params_dict = {}

    for x in lines:
        x2 = x.replace('\n', '').replace('\t', '')
        comment_begin = x2.find('#')
        if comment_begin > -1:
            x2 = x2[:comment_begin]     # Remove comment

        x2 = x2.lstrip(' ').rstrip(' ')
        if x2:                          # Expected to have key=contents format
            try:
                key, contents = x2.split('=')
            except ValueError:
                print(x2)
                print("!!!!!!!!!")
            else:
                key = key.rstrip(' ').lstrip(' ')
                try:
                    contents = re.search('\[.*\]', contents).group(0)
                except AttributeError:
                    # this is no list
                    contents = contents.replace("'", "").replace('"', "")
                    contents = contents.rstrip(' ').lstrip(' ')
                else:  # this is a list
                    contents = contents.replace(']', '').replace('[',
                                                                 '').replace(
                        ' ', '').replace("'", "").replace('"', '')
                    contents = contents.split(',')
                params_dict[key] = contents

    return params_dict


def get_site_specific_path(stat_file_path, hf_stat_vs_ref=None, v1d_mod_dir=None):
    show_horizontal_line()
    print("Auto-detecting site-specific info")
    show_horizontal_line()
    print("- Station file path: %s" % stat_file_path)

    if v1d_mod_dir is not None:
        v_mod_1d_path = v1d_mod_dir
    else:
        v_mod_1d_path = os.path.join(os.path.dirname(stat_file_path), "1D")
    if os.path.exists(v_mod_1d_path):
        print("- 1D profiles found at %s" % v_mod_1d_path)
    else:
        print("Error: No such path exists: %s" % v_mod_1d_path)
        sys.exit()
    if hf_stat_vs_ref is None:
        hf_stat_vs_ref_options = glob.glob(
            os.path.join(stat_file_path, '*.hfvs30ref'))
        if len(hf_stat_vs_ref_options) == 0:
            print("Error: No HF Vsref file was found at %s" % stat_file_path)
            sys.exit()
        hf_stat_vs_ref_options.sort()

        show_horizontal_line()
        print("Select one of HF Vsref files")
        show_horizontal_line()
        hf_stat_vs_ref_selected = show_multiple_choice(hf_stat_vs_ref_options)
        print(" - HF Vsref tp be used: %s" % hf_stat_vs_ref_selected)
    else:
        hf_stat_vs_ref_selected = hf_stat_vs_ref
    return v_mod_1d_path, hf_stat_vs_ref_selected


def get_hf_run_name(v_mod_1d_name, srf, root_dict):
    hf_sim_bin = os.path.join(tools_dir, 'hb_high_v5.4.5_np2mm+')
    hfVString = 'hf' + os.path.basename(hf_sim_bin).split('_')[-1]
    hf_run_name = "{}_{}_rvf{}_sd{}_k{}".format(
        v_mod_1d_name, hfVString, str(root_dict['hf']['hf_rvfac']),
        str(root_dict['hf']['sdrop']), str(root_dict['hf']['kappa']))
    hf_run_name = hf_run_name.replace('.', 'p')
    show_horizontal_line()
    print("- Vel. Model 1D: %s" % v_mod_1d_name)
    print("- hf_sim_bin: %s" % os.path.basename(hf_sim_bin))
    print("- hf_rvfac: %s" % root_dict['hf']['hf_rvfac'])
    print("- hf_sdrop: %s" % root_dict['hf']['sdrop'])
    print("- hf_kappa: %s" % root_dict['hf']['kappa'])
    print("- srf file: %s" % srf)
    #    yes = confirm_name(hf_run_name)
    yes = True
    return yes, hf_run_name


