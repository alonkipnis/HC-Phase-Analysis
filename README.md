# Para-Run

## About
Run many atomic experiments from a single configuration file. Ideal for running
embarrassingly parallel massive computational experiments.

For example, the current implementation of `atomic_experiment.py` corresponds 
to an experiment anayzing the asymptotic performance in multiple hypothesis 
testing under sparse alternatives as in 
[Donoho and Kipnis (2021) ``Higher Criticism to Compare Two Large Frequency Tables, with Sensitivity to Possible Rare and Weak Differences''][https://arxiv.org/abs/2007.01958]

## Setup
 - Define parameters in parameters file (e.g., ``params.yaml``)
 - Define function `gen_func` to generate experiment configurations from the parameters in the parameters file
 - Define atomic experiment function `eval_func` that receives a set of configurations and outputs the result of an atomic experiment

## Running:

### Standard:
``$python para_run -p params.yml -o output_file.csv``

### With Dask (https://dask.org/):
``$python para_run -p params.yml -o output_file.csv --dask``



