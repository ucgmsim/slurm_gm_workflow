import os
from io import StringIO

from qcore.utils import load_sim_params as mocked_load_sim_params
from qcore.utils import load_yaml as mocked_load_yaml

from workflow.calculation import create_e3d
from workflow.automation.tests.test_common_set_up import get_fault_from_rel, set_up


EXPECTED_DATA = """all_in_one=1
bfilt=4
bforce=0
dampwidth=0
dblcpl=0
dmodfile="rho3dfile.d"
dtts=20
dump_itinc=186802
dxout=1
dxts=5
dyout=1
dyts=5
dzout=1
dzts=1
elas_only=0
enable_output_dump=1
enable_restart=1
ffault=2
fhi=0.0
fmax=25
fmin=0.01
freesurf=1
geoproj=1
intmem=1
ix_ts=99
ix_ys=100
ix_zs=100
iy_ts=2
iy_xs=60
iy_zs=50
iz_ts=1
iz_xs=1
iz_ys=1
lonlat_out=1
maxmem=1500
model_style=1
nseis=1
order=4
pertbfile="none.pertb"
pmodfile="vp3dfile.p"
pointmt=0
qbndmax=100.0
qpfile="qp.qp"
qpfrac=100
qpqs_factor=2.0
qsfile="qs.qs"
qsfrac=50
read_restart=0
report=100
restart_itinc=186802
scale=1
smodfile="vs3dfile.s"
span=1
stype="2tri-p10-h20"
swap_bytes=0
ts_inc=1
ts_start=0
ts_total="111"
ts_xy=1
ts_xz=0
ts_yz=0
tzero=0.6
useqsqp=0
vmodel_swapb=0
version="3.0.4-mpi"
name="PangopangoF29"
n_proc=512
nx="141"
ny="156"
nz="88"
h="0.4"
dt=0.02
nt="2228"
flo=0.25
faultfile="sample0/CSRoot/Data/Sources/PangopangoF29/Srf/PangopangoF29_HYP01-10_S1244.srf"
vmoddir="sample0/CSRoot/Data/VMs/PangopangoF29"
modellon="178.106395445"
modellat="-38.2530996198"
modelrot="0"
main_dump_dir="sample0/CSRoot/Runs/PangopangoF29/PangopangoF29_HYP01-10_S1244/LF/OutBin"
seiscords="sample0/CSRoot/Runs/PangopangoF29/fd_rt01-h0.400.statcords"
user_scratch="sample0/CSRoot/scratch"
seisdir="sample0/CSRoot/scratch/PangopangoF29/PangopangoF29_HYP01-10_S1244/SeismoBin"
ts_file="sample0/CSRoot/Runs/PangopangoF29/PangopangoF29_HYP01-10_S1244/LF/OutBin/PangopangoF29_xyts.e3d"
ts_out_dir="sample0/CSRoot/Runs/PangopangoF29/PangopangoF29_HYP01-10_S1244/LF/TSlice/TSFiles"
restartdir="sample0/CSRoot/Runs/PangopangoF29/PangopangoF29_HYP01-10_S1244/LF/Restart"
restartname="PangopangoF29"
logdir="sample0/CSRoot/Runs/PangopangoF29/PangopangoF29_HYP01-10_S1244/LF/Rlog"
slipout="sample0/CSRoot/Runs/PangopangoF29/PangopangoF29_HYP01-10_S1244/LF/SlipOut/slipout-k2"
wcc_prog_dir="/nesi/project/nesi00213/tools/emod3d-mpi_v3.0.4"
vel_mod_params_dir="sample0/CSRoot/Data/VMs/PangopangoF29"
sim_dir="sample0/CSRoot/Runs/PangopangoF29/PangopangoF29_HYP01-10_S1244"
stat_file="/nesi/project/nesi00213/StationInfo/non_uniform_whole_nz_with_real_stations-hh400_v18p6.ll"
grid_file="/nesi/nobackup/nesi00213/RunFolder/PangopangoF29/Data/VMs/PangopangoF29/gridfile_rt01-h0.400"
model_params="/nesi/nobackup/nesi00213/RunFolder/PangopangoF29/Data/VMs/PangopangoF29/model_params_rt01-h0.400"
"""


def test_create_run_params(set_up, mocker):
    for root_path, realisation in set_up:
        fault = get_fault_from_rel(realisation)
        get_mocked_sim_params = lambda x: mocked_load_sim_params(
            os.path.join(root_path, "CSRoot", "Runs", fault, x)
        )
        mocker.patch(
            "workflow.calculation.create_e3d.utils.load_sim_params",
            get_mocked_sim_params,
        )

        outp = StringIO()

        def write_e3d(vardict):
            for (key, value) in vardict.items():
                if isinstance(value, str):
                    outp.write('%s="%s"\n' % (key, value))
                else:
                    outp.write("%s=%s\n" % (key, value))

        mocker.patch(
            "workflow.calculation.create_e3d.shared.write_to_py",
            lambda x, y: write_e3d(y),
        )
        mocker.patch(
            "workflow.calculation.create_e3d.utils.load_yaml",
            lambda x: mocked_load_yaml(
                os.path.join(
                    os.path.dirname(os.path.realpath(__file__)),
                    "..",
                    "..",
                    "..",
                    "calculation",
                    "gmsim_templates",
                    "16.1",
                    "emod3d_defaults.yaml",
                )
            )
            if "emod3d_defaults.yaml" in x
            else mocked_load_yaml(x),
        )

        create_e3d.create_run_params("PangopangoF29_HYP01-10_S1244", None, 186802)

        expected_lines = EXPECTED_DATA.split()
        outp_lines = outp.getvalue().split()

        assert len(expected_lines) == len(outp_lines)
        for i in range(len(expected_lines)):
            assert outp_lines[i] == expected_lines[i], (
                i,
                outp_lines[i],
                expected_lines[i],
            )
        assert outp.getvalue() == EXPECTED_DATA
