import argparse
import os
from logging import Logger
from pathlib import Path

from qcore import shared as qc_shared
from qcore import constants, qclogging, utils, simulation_structure, geo

from workflow.automation.lib import shared as wf_shared


def load_args():
    parser = argparse.ArgumentParser(allow_abbrev=False)
    parser.add_argument("cs_root", type=Path)
    parser.add_argument("fault")
    parser.add_argument("--log_dir", type=Path)
    args = parser.parse_args()
    return args


def main():
    args = load_args()

    fault_name = args.fault
    cybershake_root: Path = args.cs_root

    if args.log_dir is None:
        log_dir = Path(simulation_structure.get_sim_dir(cybershake_root, fault_name))
    else:
        log_dir: Path = args.log_dir

    logger = qclogging.get_logger(f"install_fault_{fault_name}")
    qclogging.add_general_file_handler(logger, log_dir / f"install_fault_{fault_name}")

    root_params_path = simulation_structure.get_root_yaml_path(cybershake_root)
    root_params = utils.load_yaml(root_params_path)

    vm_params_path = simulation_structure.get_vm_params_yaml(
        simulation_structure.get_fault_VM_dir(cybershake_root, fault_name)
    )
    vm_params_dict = utils.load_yaml(vm_params_path)

    fd_statcords, fd_statlist = generate_fd_files(
        simulation_structure.get_fault_dir(cybershake_root, fault_name),
        vm_params_dict,
        stat_file=root_params[constants.RootParams.stat_file.value],
        logger=logger,
        keep_dup_station=root_params[constants.RootParams.keep_dup_station.value],
    )

    fault_params_dict = generate_fault_params(
        cybershake_root, fault_name, fd_statcords, fd_statlist
    )

    runs_dir = simulation_structure.get_runs_dir(cybershake_root)
    fault_params_path = simulation_structure.get_fault_yaml_path(runs_dir, fault_name)
    utils.dump_yaml(fault_params_dict, fault_params_path)


def generate_fault_params(
    cybershake_root, fault_name, fd_statcords, fd_statlist
):
    runs_dir = simulation_structure.get_runs_dir(cybershake_root)
    root_params_yaml = simulation_structure.get_root_yaml_path(runs_dir)
    vel_mod_dir = simulation_structure.get_fault_VM_dir(cybershake_root, fault_name)
    fault_dir = Path(simulation_structure.get_fault_dir(cybershake_root, fault_name))
    fault_params_dict = {
        constants.FaultParams.root_yaml_path.value: root_params_yaml,
        constants.FaultParams.vel_mod_dir.value: vel_mod_dir,
        constants.FaultParams.stat_coords.value: str(fault_dir / fd_statcords),
        constants.FaultParams.FD_STATLIST.value: str(fault_dir / fd_statlist),
    }

    return fault_params_dict


def generate_fd_files(
    output_path,
    vm_params_dict,
    stat_file="default.ll",
    keep_dup_station=True,
    logger: Logger = qclogging.get_basic_logger(),
):
    MODEL_LAT = vm_params_dict["MODEL_LAT"]
    MODEL_LON = vm_params_dict["MODEL_LON"]
    MODEL_ROT = vm_params_dict["MODEL_ROT"]
    hh = vm_params_dict["hh"]
    nx = vm_params_dict["nx"]
    ny = vm_params_dict["ny"]
    sufx = vm_params_dict["sufx"]
    wf_shared.verify_strings([MODEL_LAT, MODEL_LON, MODEL_ROT, hh, nx, ny, sufx])

    filename = "fd{}".format(sufx)
    logger.info(stat_file)

    # arbitrary longlat station input
    ll_in = stat_file
    # where to save gridpoint and longlat station files
    gp_out = os.path.join(output_path, "{}.statcords".format(filename))
    ll_out = os.path.join(output_path, "{}.ll".format(filename))

    logger.info("From: {}. To: {}, {}".format(stat_file, gp_out, ll_out))

    # velocity model parameters
    nx = int(nx)
    ny = int(ny)
    mlat = float(MODEL_LAT)
    mlon = float(MODEL_LON)
    mrot = float(MODEL_ROT)
    hh = float(hh)

    # retrieve in station names, latitudes and longitudes
    sname, slat, slon = qc_shared.get_stations(ll_in, locations=True)
    slon = list(map(float, slon))
    slat = list(map(float, slat))

    # convert ll to grid points
    xy = geo.ll2gp_multi(
        list(map(list, zip(slon, slat))),
        mlon,
        mlat,
        mrot,
        nx,
        ny,
        hh,
        keep_outside=True,
    )

    # store gridpoints and names if unique position
    sxy = []
    suname = []
    for i in range(len(xy)):
        if xy[i] is None or xy[i][0] == nx - 1 or xy[i][1] == ny - 1:
            logger.log(
                qclogging.VERYVERBOSE, "Station outside domain: {}".format(sname[i])
            )
        elif xy[i] not in sxy:
            sxy.append(xy[i])
            suname.append(sname[i])
        elif keep_dup_station:
            # still adds in the station but raise a warning
            sxy.append(xy[i])
            suname.append(sname[i])
            logger.log(
                qclogging.NOPRINTWARNING,
                f"Duplicate Station added: {sname[i]} at {xy[i]}",
            )
        else:
            logger.log(
                qclogging.VERYVERBOSE, "Duplicate Station Ignored: {}".format(sname[i])
            )

    # create grid point file
    with open(gp_out, "w") as gpf:
        # file starts with number of entries
        gpf.write("%d\n" % (len(sxy)))
        # x, y, z, name
        for i, xy in enumerate(sxy):
            gpf.write("%5d %5d %5d %s\n" % (xy[0], xy[1], 1, suname[i]))

    # convert unique grid points back to ll
    # warning: modifies sxy
    ll = geo.gp2ll_multi(sxy, mlat, mlon, mrot, nx, ny, hh)

    # create ll file
    with open(ll_out, "w") as llf:
        # lon, lat, name
        for i, pos in enumerate(ll):
            llf.write("%11.5f %11.5f %s\n" % (pos[0], pos[1], suname[i]))

    return gp_out, ll_out


if __name__ == "__main__":
    main()
