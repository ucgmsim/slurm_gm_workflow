#!/usr/bin/env bash
# Deactivates the current active virtual python environment,
# resets the PYTHONPATH and
# reloads the shared bashrc

# Reset the pythonpath
export PYTHONPATH=''

# Remove environment
export CUR_ENV=''

deactivate
source ~/.bashrc