#!/usr/bin/env bash
# Activates the specified python3 virtual environment.
# Note: Resets the PYTHONPATH
# Created as a separate script to allow it to be called from scripts.
my_dir="$(dirname "$0")"
source "$my_dir/activate_common_env.sh"
