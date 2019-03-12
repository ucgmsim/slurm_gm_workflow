import os
import json
import shutil

from shared_workflow.shared import exe


class E2ETests(object):

    # Config keys
    cf_stage_dir_key = "stage_dir"
    cf_data_dir_key = "data_dir"
    cf_cybershake_config_key = "cybershake_config"
    cf_fault_list_key = "fault_list"

    # Output keys
    out_install_key = "install"

    out_output_key = "output"
    out_error_key = "error"

    # Error Keywords
    error_keywords = ["error", "traceback", "exception"]

    def __init__(self, config_file: str):

        with open(config_file, "r") as f:
            self.config_dict = json.load(f)

        self.stage_dir = self.config_dict[self.cf_stage_dir_key]

        self.output = {}

    def setup(self):
        """Setup for automatic workflow
        If this is ever used => change to use simulation structure!!
        """
        # Data
        shutil.copytree(self.config_dict[self.cf_data_dir_key], self.stage_dir)

        # Cybershake config
        shutil.copy(self.config_dict[self.cf_cybershake_config_key], self.stage_dir)

        # Fault list
        shutil.copy(self.config_dict[self.cf_fault_list_key], self.stage_dir)

        # Create runs folder
        os.mkdir("Runs")

        # Mgmt queue
        os.mkdir("mgmt_db_queue")

    def install(self):
        """Installs automated workflow"""

        # Why is this a script? Make it all python?
        cmd = "$gmsim/workflow/scripts/cybershake/install_cybershake.sh {} {} {}".format(
            self.stage_dir,
            os.path.join(
                self.stage_dir,
                os.path.basename(self.config_dict[self.cf_cybershake_config_key]),
            ),
            os.path.join(
                self.stage_dir,
                os.path.basename(self.config_dict[self.cf_fault_list_key]),
            ),
        )

        # Run install
        output, error = exe(cmd)


        # Check for errors?
        if any(cur_str in output.lower() for cur_str in self.error_keywords):
            print("There appears to be errors in the install. Error keyword found in stdout!")

        if any(cur_str in error.lower() for cur_str in self.error_keywords):
            print("There appears to be errors in the install. Error keyword found in stderr!")

        # Save the output
        self.output[self.out_install_key] = {
            self.out_output_key: output,
            self.out_error_key: error,
        }




if __name__ == '__main__':
    config_file = ""
    e2e_test = E2ETests(config_file)

    e2e_test.setup()
    e2e_test.install()