import argparse
import subprocess
from dataclasses import dataclass
from pathlib import Path
from tempfile import TemporaryDirectory, NamedTemporaryFile
from typing import List

import numpy as np
from numpy.fft import rfft, irfft
from scipy.integrate._quadrature import tupleset

from qcore import qclogging
from qcore.config import qconfig
from qcore.constants import Components
from qcore.timeseries import transf, vel2acc, read_ascii
from qcore.utils import load_yaml
from qcore.siteamp_models import cb_amp

vs_ref = 500.0  # reference Vs at the ground surface in the HF GM simulation (m/s)
vp_ref = 1800.0  # reference Vp at the ground surface in the HF GM simulation (m/s)
rho_ref = 1810.0  # reference density at ground surface in the HF GM simulation (kg/m3)
vs_pga = vs_ref
vp_pga = vp_ref

# anelastic attenuation - according to GP2010
Qs = 50.0 * vs_ref / 1000.0  # quality factor - shear (dimensionless)
Qp = 2.0 * Qs  # quality factor - compression (dimensionless)
dampS_soil = 1.0 / (2.0 * Qs)  # damping ratio - shear (dimensionless)
dampP_soil = 1.0 / (2.0 * Qp)  # damping ratio - compression (dimensionless)

SITE_AMP_SCRIPT = (
    Path(__file__).parent / "run_site_amp.tcl"
)


@dataclass
class SiteProp:
    """
    Dataclass to hold the site properties for a location to run site response at a given location
    # TODO: Investigate use of dataframe for the list values instead
    """

    H_soil: float  # Depth to bottom of soil column
    dampS_base: float  # Vs damping ratio of layer to which you deconvolve

    numLayers: int  # number of soil layers (not counting layer 0 which is bedrock)
    waterTable: float  # if water not present set waterTable anywhere below depth of model

    # allow excess pore pressure generation? Yes or No
    # If No, permeability is automatically set very high for dynamic analysis
    allowPWP: bool

    gammaPeak: float  # peak shear strain

    # flags for water table and Vs inversion
    # set VsInvTopLayer to "Yes" if there is a velocity inversion immediately below upper layer (else "No")
    # set waterTopLayer to "Yes" if water table within upper most layer and layer was split in two (else "No")
    # if waterTopLayer == "Yes", should set refDepth(numLayers) = 1.0 and refDepth(numLayers-1) = 0.0
    VsInvTopLayer: bool
    waterTopLayer: bool

    layerThick: List[float]  # layer thicknesses

    # reference pressure
    # computed as mean confining pressure at refDepth for each layer (0 is ToL, 1 is BoL)
    refDepth: List[float]

    rho: List[float]  # soil mass density (Mg/m^3)
    Vs: List[float]  # soil shear wave velocity for each layer(m/s)
    phi: List[float]  # soil friction angle
    pressCoeff: List[float]  # pressure dependency coefficient
    voidR: List[float]  # void ratio (need it for layer 0 for element definition)

    phaseAng: List[float]  # phase transformation angle (not for layer 0)

    # contraction (not for layer 0)
    contract1: List[float]
    contract3: List[float]

    # dilation coefficients (not for layer 0)
    dilate1: List[float]
    dilate3: List[float]

    name: str = ""  # Name of the station

    @property
    def soilThick(self):
        return sum(self.layerThick)

    @property
    def rho_base(self):
        """Must convert from Mg/m^2 to kg/m^3"""
        return self.rho[0] * 1000

    @property
    def vs_base(self):
        return self.Vs[0]

    def __post_init__(self):
        assert len(self.rho) == self.numLayers + 1, f"{self.rho}, {self.numLayers + 1}"
        assert len(self.Vs) == self.numLayers + 1, f"{self.Vs}, {self.numLayers + 1}"
        assert len(self.phi) == self.numLayers + 1, f"{self.phi}, {self.numLayers + 1}"
        assert (
            len(self.pressCoeff) == self.numLayers + 1
        ), f"{self.pressCoeff}, {self.numLayers + 1}"
        assert (
            len(self.voidR) == self.numLayers + 1
        ), f"{self.voidR}, {self.numLayers + 1}"
        assert (
            len(self.phaseAng) == self.numLayers
        ), f"{self.phaseAng}, {self.numLayers}"
        assert (
            len(self.contract1) == self.numLayers
        ), f"{self.contract1}, {self.numLayers}"
        assert (
            len(self.contract3) == self.numLayers
        ), f"{self.contract3}, {self.numLayers}"
        assert len(self.dilate1) == self.numLayers, f"{self.dilate1}, {self.numLayers}"
        assert len(self.dilate3) == self.numLayers, f"{self.dilate3}, {self.numLayers}"

    @staticmethod
    def from_file(file_path):
        """Loads the station yaml file"""
        return SiteProp(**load_yaml(file_path), name=Path(file_path).stem)

    def to_tcl(self, file_path, nt=None):
        """
        Writes the site properties in tcl format, as required by the site amplification code
        :param file_path: The path to write the file to
        :param nt: The number of time steps. Added for convenience. If more non-site paramters are needed, make a new filek
        """

        def list_to_tcl(val, offset=0):
            """Converts a list into tcl compatible array list format"""
            return " ".join([f"{i+offset} {x}" for i, x in enumerate(val)])

        def bool_to_tcl(val):
            """Converts a bool into tcl compatible equivalent"""
            return "Yes" if val else "No"

        out_str = f"""
# Scalars
set numLayers {self.numLayers}
set soilThick {self.soilThick}
set waterTable {self.waterTable}
set gammaPeak {self.gammaPeak}

# Bools
set allowPWP {bool_to_tcl(self.allowPWP)}                
set VsInvTopLayer {bool_to_tcl(self.VsInvTopLayer)}
set waterTopLayer {bool_to_tcl(self.waterTopLayer)}

# Arrays
array set layerThick [list {list_to_tcl(self.layerThick)}]
array set refDepth [list {list_to_tcl(self.refDepth)}]
array set rho [list {list_to_tcl(self.rho)}]
array set Vs [list {list_to_tcl(self.Vs)}]
array set phi [list {list_to_tcl(self.phi)}]
array set pressCoeff [list {list_to_tcl(self.pressCoeff)}]
array set phaseAng [list {list_to_tcl(self.phaseAng, 1)}]
array set contract1 [list {list_to_tcl(self.contract1, 1)}]
array set contract3 [list {list_to_tcl(self.contract3, 1)}]
array set dilate1 [list {list_to_tcl(self.dilate1, 1)}]
array set dilate3 [list {list_to_tcl(self.dilate3, 1)}]
array set voidR [list {list_to_tcl(self.voidR)}]
"""

        if nt is not None:
            out_str += f"\nset motionSteps {nt}"

        with open(file_path, "w") as fp:
            fp.write(out_str)


class TimeOutError(Exception):
    pass


def cumulative_trapezoid(y, dx=1.0, initial=0):
    """
    Cumulatively integrate y(x) using the composite trapezoidal rule.

    Parameters
    ----------
    y : array_like
        Values to integrate.
    dx : float, optional
        Spacing between elements of `y`. Only used if `x` is None.
    initial : scalar, optional
        If given, insert this value at the beginning of the returned result.
        Typically this value should be 0. Default is None, which means no
        value at ``x[0]`` is returned and `res` has one element less than `y`
        along the axis of integration.

    Returns
    -------
    res : ndarray
        The result of cumulative integration of `y` along `axis`.
        If `initial` is None, the shape is such that the axis of integration
        has one less value than `y`. If `initial` is given, the shape is equal
        to that of `y`.

    """
    y = np.asarray(y)
    nd = len(y.shape)
    slice1 = tupleset((slice(None),) * nd, -1, slice(1, None))
    slice2 = tupleset((slice(None),) * nd, -1, slice(None, -1))
    res = np.cumsum(dx * (y[slice1] + y[slice2]) / 2.0, axis=-1)

    shape = list(res.shape)
    shape[-1] = 1
    res = np.concatenate([np.full(shape, initial, dtype=res.dtype), res], axis=-1)

    return res


def run_deamp(waveform, component: Components, dt, vs_site, HF_pga):
    """
    Performs Hanning tapering and applies a deamplification factor to
    :param waveform:
    :param component:
    :param dt:
    :param vs_site:
    :param HF_pga:
    :return:
    """
    npts = waveform.size
    # Fourier transform of the acceleration time history of the BBGM simulation
    # length for fft and Nyquist frequency
    ft_len = int(2.0 ** np.ceil(np.log2(npts)))
    # apply taper to BBGM simulation on 5% values on the right using the Hanning method
    ntap = int(npts * 0.05)
    waveform[npts - ntap :] *= np.hanning(ntap * 2 + 1)[ntap + 1 :]
    # extend with blanks for the fft
    waveform.resize(ft_len, refcheck=False)
    ft = rfft(waveform)

    # compute the amplification factor from CB14, take inverse, and...
    # "remove" it from the BBGM simulation
    # TODO: have multiple deamp functions
    if component in [Components.c000, Components.c090]:
        ampf = cb_amp(dt, ft_len, vs_ref, vs_site, vs_pga, HF_pga)
    else:  # TODO we should have different site amp for hor and ver components
        ampf = cb_amp(dt, ft_len, vs_ref, vs_site, vs_pga, HF_pga)
    ft[:-1] *= 1.0 / ampf
    return irfft(ft)[:npts]


def run_deconvolve_and_site_response(
    acceleration_waveform: np.ndarray,
    component: Components,
    site_properties: SiteProp,
    dt=0.005,
    logger=qclogging.get_basic_logger(),
):
    """
    Deconvolves a surface waveform to a waveform at a given depth
    No deamplification occurs here
    :param acceleration_waveform: a 1d array of the component velocities to be run in cm/s/s
    :param component: The name of the component. Used to determine the transfer function to use
    :param site_properties: A SiteProp object for the location
    :param dt: The timestep for the given waveform
    :return: A waveform with site specific amplification applied. Has the same shape as waveform and units of m/s/s
    """
    size = acceleration_waveform.size

    ntap = int(size * 0.05)
    acceleration_waveform[size - ntap :] *= np.hanning(ntap * 2 + 1)[ntap + 1 :]

    ft_len = int(2.0 ** np.ceil(np.log2(size)))
    acceleration_waveform = acceleration_waveform.copy()
    acceleration_waveform.resize(ft_len, refcheck=False)

    if component in [Components.c000, Components.c090]:
        transfer = transf(
            vs_ref,
            rho_ref,
            dampS_soil,
            site_properties.H_soil,
            site_properties.vs_base,
            site_properties.rho_base,
            site_properties.dampS_base,
            acceleration_waveform.size,
            dt,
        )
    else:
        # TODO: Get ver transfer function
        transfer = transf(
            vp_ref,
            rho_ref,
            dampP_soil,
            site_properties.H_soil,
            site_properties.vs_base,
            site_properties.rho_base,
            site_properties.dampS_base,
            acceleration_waveform.size,
            dt,
        )

    waveform_ft = np.fft.rfft(acceleration_waveform)
    deconv_ft = (1.0 / transfer) * waveform_ft
    bbgm_decon = np.fft.irfft(deconv_ft)[:size]

    # integrate acceleration to get velocity (in m/s) for input to OpenSees
    cm_to_m_multiplier = 1.0 / 100
    bbgm_decon_vel = (
        cumulative_trapezoid(bbgm_decon, dx=dt, initial=0) * cm_to_m_multiplier
    )

    with TemporaryDirectory() as td:
        td = Path(td)
        with NamedTemporaryFile(dir=td) as file_name:
            # File name doesn't matter for temp file
            np.savetxt(file_name, bbgm_decon_vel)
            params_path = td / "params.tcl"
            site_properties.to_tcl(params_path, nt=size)
            try:
                out_file = call_opensees(file_name.name, params_path, td, logger=logger)
            except RuntimeError as e:
                logger.error(f"This didn't work: {component}, {site_properties.name}")
                raise e
            else:
                # Acceleration in m/s/s
                out_waveform = np.loadtxt(out_file)

    return out_waveform


def check_status(component_outdir, check_fail=False):
    """
    check the status of a run by scanning Analysis_* for keyword: "Successful" / "Failed"
    check_fail: Bools. changes the keyword
    """

    analysis_files = list(Path(component_outdir).glob("Analysis_*"))

    if len(analysis_files) == 0:
        return False

    if check_fail:
        keyword = "Failed"
        result = True
        for f in analysis_files:
            contents = f.read_text()
            result = result and (contents.strip() == keyword)
    else:
        keyword = "Successful"
        result = False
        for f in analysis_files:
            contents = f.read_text()
            result = result or (contents.strip() == keyword)
    return result


def call_opensees(
    input_file,
    params_path,
    out_dir,
    timeout_threshold=600,
    logger=qclogging.get_basic_logger(),
):
    # print(qconfig)
    script = [
        qconfig["OpenSees"],
        SITE_AMP_SCRIPT,
        input_file,
        params_path,
        out_dir,
    ]

    # Cast to string then join for printing
    # print(" ".join(map(str, script)))

    try:
        subp = subprocess.run(
            script,
            timeout=timeout_threshold,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    except subprocess.TimeoutExpired as e:
        logger.error(str(e))
        raise e

    logger.info(f'got stdout line from subprocess: {subp.stdout.decode("utf-8")}')
    logger.info(f'got stderr line from subprocess: {subp.stderr.decode("utf-8")}')
    # TODO: Add debugging if it didn't work

    out_file_path = out_dir / "out.txt"
    return out_file_path


def load_args():
    parser = argparse.ArgumentParser(allow_abbrev=False)

    parser.add_argument("bb_bin", type=Path)
    parser.add_argument("station_folder", type=Path)
    parser.add_argument("output_dir")

    args = parser.parse_args()
    return args


def main():
    args = load_args()

    # bb = BBSeis(args.bb_bin)

    folder = args.bb_bin
    files = folder.glob("*.*")
    stations = {x.stem for x in files}

    # sites_in = [
    #     station.stem in stations
    #     for station in args.station_folder.iterdir()
    # ]

    sites = {
        station.stem: station
        for station in args.station_folder.iterdir()
        if station.stem in stations
    }

    components = [Components.c000, Components.c090, Components.cver]

    for site_name, site_path in sites.items():
        site_prop = SiteProp.from_file(site_path)
        for i, waveform_component in enumerate(components):
            vel_waveform, meta = read_ascii(
                folder / f"{site_name}.{waveform_component.str_value}", meta=True
            )
            acc_waveform = vel2acc(vel_waveform, meta["dt"]) / 980.665
            deamp_wave = run_deamp(
                acc_waveform, waveform_component, meta["dt"], 155.0, 0.06275231603044873
            )
            deconv = run_deconvolve_and_site_response(
                deamp_wave[:], waveform_component, site_prop, meta["dt"]
            )
            # print(deconv)
            return deconv


if __name__ == "__main__":
    test = main()
