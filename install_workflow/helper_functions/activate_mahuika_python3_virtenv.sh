#!/usr/bin/env bash

source "${env_path}/workflow/install_workflow/helper_functions/activate_common_env.sh"

# Load python3, have to do this as virtualenv points to this python
# verions, which is not accessible without loading
module load Python/3.6.3-gimkl-2017a
