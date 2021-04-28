import logging
import argparse
import yaml
from dask_jobqueue import SLURMCluster

def start_Dask_on_Slurm(config) : # or sherlock'
    with open('slurm_conf.yaml') as file :
        params = yaml.load(file, Loader=yaml.FullLoader)
    return SLURMCluster(**params[config]) # Section to use from jobqueue.yaml configuration file.

def main() :
	parser = argparse.ArgumentParser(description='Start SLURM cluster')
	parser.add_argument('-c', type=str, help='configuration', default='sherlock-hns')
	args = parser.parse_args()
	
	cluster = start_Dask_on_Slurm(args.c)
	logging.info(f" Started SLURM cluster at {cluster}")

if __name__ == '__main__':
	main()