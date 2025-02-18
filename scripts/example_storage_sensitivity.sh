#!/bin/bash
#SBATCH --qos=priority
#SBATCH --job-name=postproc_storage_forcing_amplitude
#SBATCH --account=acclimat
#SBATCH --output=%x-%j.out
#SBATCH --error=%x-%j.err
#SBATCH --chdir=/p/projects/acclimate/projects/storage-sensitivity
#SBATCH --cpus-per-task=16
#SBATCH --ntasks=1
#SBATCH --time=0-01:59:00
#SBATCH --export=ALL,OMP_PROC_BIND=FALSE
export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK
source activate compacts-simulations
python /p/projects/acclimate/projects/post-proc-dev/post-processing/scripts/example_storage_sensitivity.py