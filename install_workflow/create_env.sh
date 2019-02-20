#!/usr/bin/env bash

name=${1?Error: "A environment name has to be given"}
conf_file=${2?Error: "A config file path has to be provided"}

# Load all the required value from the json config
base_path=`jq -r '.base_environments_path' ${conf_file}`
virtenv_src=`jq -r '.clean_python3_virtualenv' ${conf_file}`
est_models_src=`jq -r '.estimation_model_src' ${conf_file}`
clean_workflow=`jq -r '.clean_workflow' ${conf_file}`

# Check that this name isn't already taken.
env_path=${base_path}/${name}
if [[ -d "${env_path}" ]]; then
    echo "Environment with name $name already exists. Quitting."
    exit
fi

# Create the directory
echo "Creating enviroment folder in $env_path"
mkdir ${env_path} || exit 1
cd ${env_path}

# Copy the base python3 virtual environment
mkdir python_virtenv
echo "Copying clean python3 virtual environment from $virtenv_src"
cp -r ${virtenv_src}/* ${env_path}/python_virtenv

# Setting up workfow, qcore and IM calc
echo "Cloning workflow"
#git clone git@github.com:ucgmsim/slurm_gm_workflow.git
#mv ./slurm_gm_workflow ./workflow

# TMP HACK, due to permission issue
cd ${clean_workflow}
git reset --hard
git checkout QSW_879

rsync -a $clean_workflow/ $env_path/workflow/
cd $env_path
echo `pwd`
# END OF HACK

# Load standard python3 virtual env
./workflow/install_workflow/helper_functions/activate_maui_python3_virtenv.sh ${virtenv_src}
echo `which python`

# Create workflow config
python ./workflow/install_workflow/create_config_file.py ./workflow/scripts/

# Copy the estimation models
echo "Copying estimation models from $est_models_src"
mkdir ./workflow/estimation/models/
cp -r ${est_models_src}/* ./workflow/estimation/models/

echo "Cloning qcore"
git clone git@github.com:ucgmsim/qcore.git

echo "Cloning IM_calculation"
git clone git@github.com:ucgmsim/IM_calculation.git

# Run setup for IM_calculation
echo "Running setup for IM_calculation"
cd IM_calculation
python setup_rspectra.py build_ext --inplace
cd ../

./workflow/install_workflow/helper_functions/deactivate_virtenv.sh
