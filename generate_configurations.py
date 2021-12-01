import numpy as np

def generate(nMonte, params):
    return gen_normal(nMonte, params)

def gen_normal(nMonte, variables) :
    """
    Parameters for 1-sample normal means experiment

    """
    def gen_series(var) :
        rec = variables[var]
        if rec['type'] == 'range':
            tp = variables[var].get('float')
            return np.linspace(rec['min'], rec['max'],
                         int(rec['length'])).astype(tp)
        if rec['type'] == 'list':
            return rec['values']

    rr = gen_series('r')
    bb = gen_series('beta')
    ss = gen_series('sigma')
    nn = gen_series('n_samples')

    for itr in range(nMonte) :
        for n in nn :
            for beta in bb :
                for r in rr :
                    for sig in ss :
                        yield {'itr' : itr, 'n' : n, 
                               'beta' : beta, 'r' : r, 'sig' : sig} 