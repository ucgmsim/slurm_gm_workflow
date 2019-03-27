#!/usr/bin/env bash
# script version: slurm
#
# must be run with sbatch clean_up.sl [realisation directory]

#SBATCH --job-name=clean_up
#SBATCH --account=nesi00213
#SBATCH --partition=prepost
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=1

python $gmsim/workflow/scripts/clean_up.py $1