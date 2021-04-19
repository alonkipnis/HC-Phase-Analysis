import numpy as np
import pandas as pd
import yaml
from datetime import datetime
from evaluate_iteration import evaluate_iteration

import logging
logging.basicConfig(level=logging.INFO)
import argparse

from evaluate_iteration import evaluate_iteration

import dask
import dask.dataframe as dd
from dask.distributed import Client
from dask_jobqueue import SLURMCluster

    
class ParaRun :

    def __init__(self, param_file='params.yaml', func) :
        with open(param_file) as file:
            self._params = yaml.load(file, Loader=yaml.FullLoader)
        logging.info(f" Reading parameters from {param_file}.")
        self._conf = pd.DataFrame(self._conf_generator())
        logging.info(f" {len(self._conf)} configurations generated.")

        self._out = pd.DataFrame()
        self._npartitions = 4
        self._func = func
        
    def _conf_generator(self) :
        def gen_series(var) :
            rec = self._params['variables'][var]
            tp = self._params['variables'][var].get('float')
            return np.linspace(rec['min'], rec['max'],
                         int(rec['length'])).astype(tp)

        rr = gen_series('r')
        bb = gen_series('beta')
        xx = self._params['variables']['Zipf_params']
        nMonte = self._params['nMonte']

        nn = self._params['variables']['n_samples']
        NN = self._params['variables']['n_features']
    
        for itr in range(nMonte) :
            for N in NN :
                ee = np.round(N ** (-bb), 6)
                mm = np.round(np.sqrt(2*np.log(N) * rr), 3)
                for n in nn :
                    for eps in ee :
                        for mu in mm :
                            for xi in xx :
                                yield {'itr' : itr, 'n' : n, 'N': N,
                                       'ep' : eps, 'mu' : mu, 'xi' : xi} 
        
    def run(self) :
        """
        Apply atomic expriment function to each row in configuration table

        Args:
        -----
        func    atomic experiment function
        """
        logging.info(f" Running...")
        y = self._conf.iloc[:,1:].apply(lambda row : func(*row), axis=1)
        
        #self._out = pd.json_normalize(y)
        self._out = y
        logging.info(f" Completed.")

    def Dask_run(self, client) :
        """
        Apply atomic expriment function to each row in configuration table

        Args:
        -----
        func    atomic experiment function

        """

        logging.info(f" Running on Dask...")
        ddf = dd.from_pandas(self._conf.iloc[:,1:], npartitions=self._npartitions)
        
        x = ddf.apply(lambda row : func(*row), axis=1, meta=dict)
        logging.info(" Sending futures...")
    
        self._out['func'] = str(func.__name__)
        self._out['time_start'] = str(datetime.now())
        y = x.compute()
        self._out = pd.DataFrame(y)
        self._out['time_end'] = str(datetime.now())
        
        logging.info(f" Completed.")

    def to_file(self, filename="results.csv") :
        if self._out.empty :
            logging.warning(" No output detected."
            "Did the experiment complete running?")
        if self._conf.empty :
            logging.warning(" No configuration talbe detected."
            "Did call gen_conf_table() ")

        logging.info(f" Saving results...")
        results = pd.concat([self._conf, self._out], axis=1)
        logging.info(f" Saved {len(results)} records in {filename}.")
        results.to_csv(filename)

def start_Dask_cluster(config='sherlock-hns') : # or sherlock'
    with open('slurm_conf.yaml') as file :
        params = yaml.load(file, Loader=yaml.FullLoader)
    return SLURMCluster(**params[config]) # Section to use from jobqueue.yaml configuration file.

def main() :
    parser = argparse.ArgumentParser(description='Launch experiment')
    parser.add_argument('-o', type=str, help='output file', default='results.csv')
    parser.add_argument('-p', type=str, help='yaml parameters file.', default='params.yaml')
    parser.add_argument('--dask', action='store_true')
    parser.add_argument('--slurm', action='store_true')
    args = parser.parse_args()
    #
    
    cluster=None
    client=None
    exper = ParaRun(args.p, evaluate_iteration)
    if args.dask :
        if args.slurm :
            logging.info(" Starting a Dask cluster on a SLURM cluster...")
            cluster = start_Dask_cluster()
            logging.info(" Connecting Dask client...")
            client = Client(cluster)
        else :
            logging.info(" Starting a local Dask cluster...")
            client = Client()
        
        logging.info(f" Client info:")
        logging.info(f"\t Services: {client.scheduler_info()['services']}")
        exper.Dask_run(client)
        exper.to_file(args.o)
        if cluster :
            cluster.close()
        if client :
            client.close()
    
    else :
        exper.run()
        exper.to_file(args.o)
    

if __name__ == '__main__':
    main()
