# ParaRun -- Computational Experiment at Scale

## About
- ParaRun is a simple template for running massive computational experiments 

## Usage
 - install ``requirements.txt``
 - Setup parameters in ``params.yaml``
 - Setup experiment atomic function ``evaluate_iteration``
 - Run main file (e.g.): ``python para_run.py --dask``

### With Dask (https://dask.org/):
 - install 'dask' and 'dask-distributed'
 - use ``--dask`` when running the file containing ``main()``




