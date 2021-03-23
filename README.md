# ParaRun -- Computational Experiment at Scale

## About
- ParaRun is a simple template for running a massive computational experiment 
using Dask or another scaling manager

## Install
- ``pip3 install -r requirements.txt''

## Instructions for using Dask ([https://dask.org/]):
 - install `requirements.txt'
 - install 'dask' and 'dask-distributed'
 - use flag ``--dask'' when running the file containing ``main()''

## Usage
 - Setup parameters in ``params.yaml''
 - Replace function ``evaluate_iteration''
 - Run main file (e.g.): ``python para_run.py --dask''



