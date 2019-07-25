#!/usr/bin/env python
"""
Generates 'e3d.par' from the default set, appending new key value pairs of parameters.
@author Viktor Polak, Sung Bae
@date 6 April 2016
Replaces set_runparams.csh. Converted to python, handles params set in separate file.
USAGE: edit params.py (not this file), execute this file from the same directory.
ISSUES: remove default values in e3d_default.par where not needed.
"""

from __future__ import print_function
import sys
import os.path
from logging import Logger
from os.path import basename

from shared_workflow import shared
from qcore import utils
from qcore import binary_version
from shared_workflow import load_config
from qcore.qclogging import get_basic_logger

sys.path.append(os.path.abspath(os.path.curdir))


def create_run_params(
    sim_dir,
    srf_name=None,
    workflow_config=None,
    steps_per_checkpoint=None,
    logger: Logger = get_basic_logger(),
):
    params = utils.load_sim_params(os.path.join(sim_dir, "sim_params.yaml"))

    if workflow_config is None:
        workflow_config = load_config.load(
            os.path.dirname(os.path.realpath(__file__)), "workflow_config.json"
        )
    global_root = workflow_config["global_root"]
    tools_dir = binary_version.get_lf_bin(params.emod3d.emod3d_version)
    emod3d_version = workflow_config["emod3d_version"]

    e3d_yaml = os.path.join(
        workflow_config["templates_dir"],
        "gmsim",
        params.version,
        "emod3d_defaults.yaml",
    )
    e3d_dict = utils.load_yaml(e3d_yaml)
    # skip all logic if a specific srf_name is provided
    if srf_name is None or srf_name == os.path.splitext(basename(params.srf_file))[0]:
        srf_file_basename = os.path.splitext(os.path.basename(params.srf_file))[0]
        e3d_dict["version"] = emod3d_version + "-mpi"

        e3d_dict["name"] = params.run_name
        e3d_dict["n_proc"] = 512

        e3d_dict["nx"] = params.nx
        e3d_dict["ny"] = params.ny
        e3d_dict["nz"] = params.nz
        e3d_dict["h"] = params.hh
        e3d_dict["dt"] = params.dt
        e3d_dict["nt"] = str(int(round(float(params.sim_duration) / float(params.dt))))
        e3d_dict["flo"] = float(params.flo)

        e3d_dict["faultfile"] = params.srf_file

        e3d_dict["vmoddir"] = params.vel_mod_dir

        e3d_dict["modellon"] = params.MODEL_LON
        e3d_dict["modellat"] = params.MODEL_LAT
        e3d_dict["modelrot"] = params.MODEL_ROT

        e3d_dict["main_dump_dir"] = os.path.join(params.sim_dir, "LF", "OutBin")
        e3d_dict["seiscords"] = params.stat_coords
        e3d_dict["user_scratch"] = os.path.join(params.user_root, "scratch")
        e3d_dict["seisdir"] = os.path.join(
            e3d_dict["user_scratch"], params.run_name, srf_file_basename, "SeismoBin"
        )

        e3d_dict["ts_total"] = str(
            int(
                float(params.sim_duration)
                / (float(e3d_dict["dt"]) * float(e3d_dict["dtts"]))
            )
        )
        e3d_dict["ts_file"] = os.path.join(
            e3d_dict["main_dump_dir"], params.run_name + "_xyts.e3d"
        )
        e3d_dict["ts_out_dir"] = os.path.join(params.sim_dir, "LF", "TSlice", "TSFiles")

        e3d_dict["restartdir"] = os.path.join(params.sim_dir, "LF", "Restart")
        if steps_per_checkpoint:
            e3d_dict["dump_itinc"] = e3d_dict["restart_itinc"] = int(steps_per_checkpoint)

        e3d_dict["restartname"] = params.run_name
        e3d_dict["logdir"] = os.path.join(params.sim_dir, "LF", "Rlog")
        e3d_dict["slipout"] = os.path.join(
            params.sim_dir, "LF", "SlipOut", "slipout-k2"
        )

        # other locations
        e3d_dict["wcc_prog_dir"] = tools_dir
        e3d_dict["vel_mod_params_dir"] = params.vel_mod_dir
        e3d_dict["global_root"] = global_root
        e3d_dict["sim_dir"] = params.sim_dir
        e3d_dict["stat_file"] = params.stat_file
        e3d_dict["grid_file"] = params.GRIDFILE
        e3d_dict["model_params"] = params.MODEL_PARAMS

        if params.emod3d:
            for key, value in params.emod3d.items():
                if key in e3d_dict:
                    e3d_dict[key] = value
                else:
                    logger.debug(
                        "{} not found as a key in e3d file. Ignoring variable. Value is {}.".format(
                            key, value
                        )
                    )

        shared.write_to_py(os.path.join(params.sim_dir, "LF", "e3d.par"), e3d_dict)


if __name__ == "__main__":
    sim_dir = os.getcwd()
    create_run_params(sim_dir)
