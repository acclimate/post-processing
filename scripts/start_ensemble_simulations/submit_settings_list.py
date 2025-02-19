"""
Script to submit a list of settings for acclimate ensemble to the cluster.

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
    parser = argparse.ArgumentParser()
    parser.add_argument("--acclimate", required=True, help="path to acclimate source code")
    parser.add_argument("--settings", required=True, help="path to settings list")
    parser.add_argument("--sbatch", required=True, help="path to base sbatch script")
    parser.add_argument("--qos", required=True, help="SLURM QOS")
    parser.add_argument("--jobname", default="acclimate", help="job name")
    parser.add_argument("--account", default="acclimat", help="account name")
    parser.add_argument("--basedir", required=True, help="base directory")
    parser.add_argument("--cpus", default=128, help="number of CPUs")
    parser.add_argument("--timelimit", default="3-12:00:00", help="time limit")
    parser.add_argument("--test", action="store_true", help="test run")
    parser.add_argument("--verbose", action="store_true", help="verbose output")
    return parser.parse_args()

def load_settings(settings_path):
    yaml = ruamel.yaml.YAML()
    with open(settings_path, 'r') as stream:
        return yaml.load(stream)

args = parse_arguments()
list_of_settings = load_settings(args.settings)[:2] if args.test else load_settings(args.settings)

for setting in list_of_settings:
    with open(args.sbatch) as f:
        sbatch_script = f.read()
        replacements = {
            "ACCLIMATE_PATH": args.acclimate,
            "SETTINGS_FILE": setting,
            "QOS": args.qos,
            "JOBNAME": args.jobname,
            "ACCOUNT": args.account,
            "BASEDIR": args.basedir,
            "CPUS": str(args.cpus),  # Convert to string
            "TIMELIMIT": args.timelimit
        }

        for key, value in replacements.items():
            sbatch_script = sbatch_script.replace(key, value)

    sbatch_script_path = f"{setting}.sh"
    with open(sbatch_script_path, "w") as f:
        f.write(sbatch_script)

    if args.verbose:
        print(sbatch_script)
    os.system(f"sbatch {sbatch_script_path}")
    os.remove(sbatch_script_path)
