import inspect
import os

from shared_workflow import install_shared
from testing.test_common_set_up import set_up, get_bench_output, get_input_params


def test_install_simulation(set_up):
    func_name = "install_simulation"
    params = inspect.getfullargspec(install_shared.install_simulation).args
    for root_path, _ in set_up:
        input_params = get_input_params(root_path, func_name, params)

        # Set to a directory that will definitely exist
        input_params[18] = "/etc"
        for i in range(len(input_params)):
            if isinstance(input_params[i], str) and input_params[i].startswith(
                ("CSRoot", "AdditionalData", "PangopangoF29/")
            ):
                input_params[i] = os.path.join(root_path, input_params[i])
        # Run/fault name was missing for some reason?
        input_params.insert(2, "PangopangoF29")
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
        bench_output["ims"] = {
            "component": ["comp"],
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
        assert root_params_dict == bench_output
