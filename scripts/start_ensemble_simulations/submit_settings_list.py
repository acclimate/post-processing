"""
Simple script to submit a list of settings for acclimate ensemble to the cluster.

Usage:
    python submit_settings_list.py --acclimate acclimate_path --settings settings_list.yml --sbatch sbatch_script.sh
Optional arguments:
    --qos: SLURM QOS
    --jobname: job name
    --account: account name
    --basedir: base directory
    --cpus: number of CPUs
    --timelimit: time limit
    --test: test run, only submit the first two settings
"""

import os
import ruamel.yaml
import argparse

def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--acclimate", required=True, help="path to acclimate source code")
    parser.add_argument("--settings", required=True, help="path to settings list")
    parser.add_argument("--sbatch", required=True, help="path to base sbatch script")
    parser.add_argument("--qos", required=True, help="SLURM QOS")
    parser.add_argument("--jobname", help="job name", default="acclimate")
    parser.add_argument("--account", help="account name", default="acclimat")
    parser.add_argument("--basedir", required=True, help="base directory")
    parser.add_argument("--cpus", help="number of CPUs", default="128")
    parser.add_argument("--timelimit", help="time limit", default="3-12:00:00")
    parser.add_argument("--test", action="store_true", help="test run")
    return parser.parse_args()

def load_settings(settings_path):
    """Load the settings list from a YAML file."""
    yaml = ruamel.yaml.YAML()
    with open(settings_path, 'r') as stream:
        return yaml.load(stream)


args = parse_arguments()

# Load acclimate path
acclimate_path = args.acclimate

# Load the settings list as a list
list_of_settings = load_settings(args.settings)

if args.test:
    list_of_settings = list_of_settings[:2]

for setting in list_of_settings:
    # Load the sbatch script
    with open(args.sbatch) as f:
        sbatch_script = f.read()

    sbatch_script = sbatch_script.replace("ACCLIMATE_PATH", acclimate_path)
    sbatch_script = sbatch_script.replace("SETTINGS_FILE", setting)
    sbatch_script = sbatch_script.replace("QOS", args.qos)
    sbatch_script = sbatch_script.replace("JOBNAME", args.jobname)
    sbatch_script = sbatch_script.replace("ACCOUNT", args.account)
    sbatch_script = sbatch_script.replace("BASEDIR", args.basedir)
    sbatch_script = sbatch_script.replace("CPUS", args.cpus)
    sbatch_script = sbatch_script.replace("TIMELIMIT", args.timelimit)

    # Save the sbatch script
    sbatch_script_path = setting + ".sh"
    with open(sbatch_script_path, "w") as f:
        f.write(sbatch_script)

    print(sbatch_script)

    # Submit the job
    os.system("sbatch " + sbatch_script_path)

    # Remove the sbatch script
    os.remove(sbatch_script_path)