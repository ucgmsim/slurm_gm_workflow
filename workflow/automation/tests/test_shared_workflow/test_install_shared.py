import inspect
import os

from workflow.automation.install_scripts import install_shared
from workflow.automation.tests.test_common_set_up import (
    get_bench_output,
    get_input_params,
    set_up,
)


def test_install_simulation(set_up):
    func_name = "install_simulation"
    params = inspect.getfullargspec(install_shared.install_simulation).args
    for root_path, _ in set_up:
        input_params = get_input_params(root_path, func_name, params)

        for i in range(len(input_params)):
            if isinstance(input_params[i], str) and input_params[i].startswith(
                ("CSRoot", "AdditionalData", "PangopangoF29/")
            ):
                input_params[i] = os.path.join(root_path, input_params[i])

        # An incorrect parameter is at index 9
        input_params[9] = True
        print(input_params)
        test_output = install_shared.install_simulation(*input_params)
        root_params_dict = test_output[0]

        # Accounting for removed parameters
        # Simpler solution than downloading, editing and re-uploading the test data
        root_params_dict["global_root"] = "/nesi/project/nesi00213"
        root_params_dict[
            "v_1d_mod"
        ] = "/nesi/project/nesi00213/VelocityModel/Mod-1D/Cant1D_v3-midQ_OneRay.1d"
        root_params_dict["bb"]["version"] = "3.0.4"
        root_params_dict["bb"]["site_specific"] = False
        del root_params_dict["hf"]["hf_vel_mod_1d"]
        root_params_dict[
            "v_mod_1d_name"
        ] = "/nesi/project/nesi00213/VelocityModel/Mod-1D/Cant1D_v2-midQ_leer.1d"

        bench_output = get_bench_output(root_path, func_name)[0]
        bench_output.pop("extended_period")
        bench_output.pop("stat_vs_ref")
        bench_output["ims"] = {
            "component": ["geom"],
            "extended_period": False,
            "pSA_periods": [
                0.02,
                0.05,
                0.1,
                0.2,
                0.3,
                0.4,
                0.5,
                0.75,
                1.0,
                2.0,
                3.0,
                4.0,
                5.0,
                7.5,
                10.0,
            ],
        }
        bench_output["bb"]["site_response_dir"] = False
        assert root_params_dict == bench_output
