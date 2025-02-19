#!/bin/bash
#SBATCH --qos=QOS                # Quality of Service
#SBATCH --job-name=JOBNAME       # Job name
#SBATCH --account=ACCOUNT        # Account name
#SBATCH --output=%x-%j.out       # Standard output file
#SBATCH --error=%x-%j.err        # Standard error file
#SBATCH --chdir=BASEDIR          # Change to directory BASEDIR before starting the job
#SBATCH --cpus-per-task=CPUS     # Number of CPUs per task
#SBATCH --time=TIMELIMIT         # Time limit for the job
#SBATCH --export=ALL,OMP_PROC_BIND=FALSE  # Export all environment variables and set OMP_PROC_BIND to FALSE

# Set the number of OpenMP threads to the number of CPUs per task
export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK

# Run the ACCLIMATE_PATH script with the SETTINGS_FILE
# These variables are  replaced by submit_settings_list.py
ACCLIMATE_PATH SETTINGS_FILE