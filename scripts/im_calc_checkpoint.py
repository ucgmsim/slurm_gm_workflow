import os
import glob

IM_CLAC_DIR = 'IM_calc'
CSV_PATTERN = '*.csv'
CSV_SUFFIX = '.csv'
META_PATTERN = '*imcalc.info'
# Examples:
# sim_waveform_dirs =
# ['/nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6/Runs/test/Kelly/BB/Cant1D_v3-midQ_OneRay_hfnp2mm+_rvf0p8_sd50_k0p045/Kelly_HYP20-29_S1434',
# '/nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6/Runs/test/Kelly/BB/Cant1D_v3-midQ_OneRay_hfnp2mm+_rvf0p8_sd50_k0p045/Kelly_HYP29-29_S1524',
# '/nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6/Runs/test/Kelly/BB/Cant1D_v3-midQ_OneRay_hfnp2mm+_rvf0p8_sd50_k0p045/Kelly_HYP07-29_S1304']
# dire = '/nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6/Runs/test/Kelly/BB/Cant1D_v3-midQ_OneRay_hfnp2mm+_rvf0p8_sd50_k0p045/Kelly_HYP20-29_S1434'
# output_sim_dir = /nesi/nobackup/nesi00213/RunFolder/Cybershake/v18p6/Runs/test/Kelly/BB/Cant1D_v3-midQ_OneRay_hfnp2mm+_rvf0p8_sd50_k0p045/Kelly_HYP20-29_S1434/../../../IM_calc/Kelly_HYP20-29_S1434/


def checkpoint_single(run_dir, realization_name, sim_or_obs):
    if sim_or_obs == 's':
        fault_name = realization_name.split('_')[0]
        output_dir = os.path.join(run_dir, fault_name, IM_CLAC_DIR, realization_name)
    elif sim_or_obs == 'o':
        output_dir = os.path.join(run_dir, IM_CLAC_DIR, realization_name)

    if os.path.isdir(output_dir):  # if output dir exists
        sum_csv = glob.glob1(output_dir, CSV_PATTERN)
        meta = glob.glob1(output_dir, META_PATTERN)
        # if sum_csv and meta are not empty lists('.csv' and '_imcalc.info' files present)
        # then we think im calc on the corresponding dir is completed and hence remove
        return (sum_csv and meta) != []


def checkpoint_wrapper(run_dir, waveform_dirs, sim_or_obs):
    done = 0
    todo = 0
    for directory in waveform_dirs[:]:
        dir_name = directory.split('/')[-1]
        if dir_name == IM_CLAC_DIR:
            waveform_dirs.remove(directory)
        else:
            exits = checkpoint_single(run_dir, dir_name, sim_or_obs)
            if exits:
                waveform_dirs.remove(directory)
                done += 1
            else:
                todo += 1
    if waveform_dirs[:] != []:
        print("Inside {}, {} im_calc done, {} to do.".format(run_dir, done, todo)) 
    return waveform_dirs


def checkpoint_rrup(output_dir, srf_files):
    """
    Checkpoint for rrups
    :param output_dir: user input output dir to store computed rrups
    :param srf_files: a list of both completed and not completed srf files for computing rrups.
    :return: a list of not completed srf files
    """
    for srf in srf_files[:]:
        srf_name = srf.split('/')[-1].split('.')[0]
        output_path = os.path.join(output_dir, srf_name + CSV_SUFFIX)
        if os.path.isfile(output_path):
            srf_files.remove(srf)
    return srf_files

