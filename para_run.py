import numpy as np
import pandas as pd
import yaml
from evaluate_iteration import evaluate_iteration
from tqdm import tqdm

import logging
logging.basicConfig(level=logging.INFO)
import argparse

import dask
from dask.distributed import Client, progress

    
class ParaRun :
    def __init__(self, func, param_file='params.yaml') :
        print(param_file)
        with open(param_file) as file:
            self._params = yaml.load(file, Loader=yaml.FullLoader)
        logging.info(f" Reading parameters from {param_file}.")

        self._out = pd.DataFrame()
        self._npartitions = 4
        self._func = func

        self._conf = pd.DataFrame(self._conf_generator())
        logging.info(f" {len(self._conf)} configurations generated.")
        
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
                for n in nn :
                    for beta in bb :
                        for r in rr :
                            for xi in xx :
                                yield {'itr' : itr, 'n' : n, 'N': N,
                                       'be' : beta, 'r' : r, 'xi' : xi} 
        
    def run(self) :
        """
        Apply atomic expriment function to each row in configuration table

        Args:
        -----
        func    atomic experiment function
        """
        logging.info(f" Running...")

        results = []
        job_ids = []
        itr = 0
        for params in tqdm(self._conf.iterrows()) :
            r = self._func(*params[1])
            results.append(r)
            itr += 1
            job_ids += [itr]
        
        self._out = self._conf.join(pd.DataFrame(results), how='left')
        logging.info(f" Completed.")

    def Dask_run(self, client) :
        """
        Apply atomic expriment function to each row in configuration table

        Args:
        -----
        func    atomic experiment function

        """

        logging.info(f" Running on Dask...")

        logging.info(" Mapping to futures...")

        variables = self._conf.columns.tolist()
        self._conf.loc[:,'job_id'] = 'null'
        futures=[]

        df_vars = self._conf.filter(items=variables)
        for r in df_vars.iterrows() :
            fut = client.submit(self._func, **r[1])
            self._conf.loc[r[0], 'job_id'] = fut.key
            futures += [fut]
        
        logging.info(" Sending futures...")
        progress(futures)
        
        keys = [fut.key for fut in futures]
        #results = pd.DataFrame([fut.result() for fut in futures])
        results = pd.DataFrame(client.gather(futures), index=keys)
        logging.info(" Terminating client...")
        client.close()
    
        self._out = self._conf.set_index('job_id').join(results, how='left')


        #ddf = dd.from_pandas(self._conf.iloc[:,1:], npartitions=self._npartitions)
        #x = ddf.apply(lambda row : self._func(*row), axis=1, meta=dict)    

        # fut = client.map(func, )
        # progress(fut)
        # res = client.gather(fut)
        # y = x.compute()
        # self._out = pd.DataFrame(y)
        

    def to_file(self, filename="results.csv") :
        if self._out.empty :
            logging.warning(" No output." 
            "Did the experiment complete running?")
        if self._conf.empty :
            logging.warning(" No configuration detected."
            "Did call gen_conf_table() ")

        logging.info(f" Saving results...")
        logging.info(f" Saved {len(self._out)} records in {filename}.")
        self._out.to_csv(filename)    

def main() :
    parser = argparse.ArgumentParser(description='Launch experiment')
    parser.add_argument('-o', type=str, help='output file', default='results.csv')
    parser.add_argument('-p', type=str, help='yaml parameters file.', default='params.yaml')
    parser.add_argument('--dask', action='store_true')
    parser.add_argument('--address', type=str, default="")
    args = parser.parse_args()
    #
    
    if args.dask :
        logging.info(f" Using Dask:")
        if args.address == "" :
            logging.info(f" Starting a local cluster")
            client = Client()
        else :
            logging.info(f" Connecting to existing cluster at {args.address}")
            client = Client(args.address)
        logging.info(f" Dashboard at {client.dashboard_link}")
        exper = ParaRun(evaluate_iteration, args.p)
        exper.Dask_run(client)
        exper.to_file(args.o)
        
    else :
        exper = ParaRun(evaluate_iteration, args.p)
        exper.run()
        exper.to_file(args.o)
    

if __name__ == '__main__':
    main()
