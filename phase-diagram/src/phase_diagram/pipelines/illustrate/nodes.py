"""
This is a boilerplate pipeline 'illustrate'
generated using Kedro 0.17.0

To do (11/29/2021):
 - improve HC curve
 - add illustration for a fixed x or y value
"""

import pandas as pd
import numpy as np
import scipy
import logging
from plotnine import *
from typing import Dict
from scipy.optimize import curve_fit

def prepare_results(df_res, params) -> pd.DataFrame:
    df_res = df_res[pd.to_numeric(df_res['HC'], errors='coerce').notnull()]
    df_res = df_res.drop(['metric'], axis=1)
    df_res = df_res.astype(float)

    # fixes:
    df_res = df_res.replace(to_replace=np.inf, value=1e6)

    # log transform
    if params['log_transform']:
        for var in params['variables']:
            vals = df_res[var].values
            vals[vals < 0] = .1
            df_res.loc[:, var] = np.log(vals)

    return df_res.melt(id_vars=["r", "beta", "n", "N", "nMonte", "a", "xi"],
                       value_vars=params['variables'],
                       value_name='value').drop_duplicates()


def _logit(x):
    return np.log(x / (1 - x + 1e-6))

def compute_success_probability(df, params):
    logging.info("Variables in evaluation results:")
    xname = params['x_name']
    yname = params['y_name']


    for var_name in [var_name for var_name in df.columns
                     if var_name not in [xname, yname, 'variable']]:
        no_uniques = df[var_name].unique()
        logging.info(f"`{var_name}`: {no_uniques}")
        if len(no_uniques) > 0:
            logging.warning(f"Number of values for `{var_name}` is not unique")
    logging.info(f"num. of unique x values: {len(df[params['x_name']].unique())}")
    logging.info(f"num. of unique y values: {len(df[params['y_name']].unique())}")

    df_prob = pd.DataFrame()
    for variable in params['variables']:
        dfr = df[df.variable == variable]

        # get test thresh corresponding to test size:
        if params['y_null'] == 'min':
            # (here we assume that min(y_values) corresponds to the null)
            y_null_values = dfr.loc[dfr[yname] <= dfr[yname].min(), 'value'].dropna()
            thr = np.quantile(y_null_values, params['size'])  # quantile `size` under the null
        else:
            raise ValueError("I do not understand how to obtain null behavior")

        nMonte = dfr.groupby([xname, yname]).agg('count').values[0][0]  # number of experiments

        dfr.loc[:, 'total_succ'] = dfr.groupby([xname, yname])['value'].transform(lambda x: (x > thr).sum())
        dfr.loc[:, 'prob'] = dfr['total_succ'] / nMonte
            #dfr.groupby([xname, yname])['value'].transform(lambda x: (x > thr).mean())

        dfr.loc[:, 'logit'] = dfr['prob'].apply(_logit)

        # num of successes needed to deem result significance at requested level
        no_success_needed = scipy.stats.binom.isf(params['level'],
                                                 n=nMonte, p=params['size'])
        dfr.loc[:, 'succ_sig'] = (dfr.total_succ > no_success_needed) + .0
        dfr.loc[:, 'variable'] = variable

        if params['interpolate']:
            success_rate_thr = no_success_needed / nMonte
            dfr = _interpolate(dfr, params, success_rate_thr)

        # add perfect success event
        dfs = pd.DataFrame({xname: dfr[xname].unique(),
                            yname: dfr[yname].max()})
        dfs.loc[:, 'prob'] = 1.0
        dfs.loc[:, 'succ_t'] = nMonte
        dfs.loc[:, 'succ_sig'] = 1.0
        dfs.loc[:, 'variable'] = variable
        dfr = dfr.append(dfs, ignore_index=True)

        dfr = add_critical_value(dfr, params['x_name'], params['y_name'], 'succ_sig')
        dfr = dfr.filter([xname, yname, 'prob', 'succ', 'succ_sig',
                          'variable', 'crit']).drop_duplicates()

        df_prob = df_prob.append(dfr, ignore_index=True)
    return df_prob


def _get_critical_value(xx, yy):
    """
    :param xx:
    :param yy:
    :return: intercept of sigmoid fitted to x and y values
    """

    def sigmoid(x, k, x0):
        return 1 / (1 + np.exp(-k * (x - x0)))

    opt, _ = curve_fit(sigmoid, xx, yy)
    return opt[1]

def add_critical_value(dfr, x_name, y_name, z_name):

    dft = pd.DataFrame()
    for c in dfr.sort_values(y_name).groupby(x_name):
        if c[1][z_name].max() > 0.5:
            crt = _get_critical_value(c[1][y_name], c[1][z_name])
            c[1].loc[:, 'crit'] = crt
        dft = dft.append(c[1], ignore_index=True)
    return dft


def illustrate_fixed_x(df: pd.DataFrame, params: Dict) -> Dict:
    """
    Illusstrate 'prob' versus y values for fixed values of x

    :param df:
    :param params:
    :param title:
    :return:
    """
    xval = params['x_value']
    yval = params['y_value']
    zval = params['z_value']

    ls_p = {}
    EPS = 1e-3
    for x in params['fixed_x_vals']:
        dfr = df[np.abs(df[xval] - x) < EPS]
        print(x)
        assert(len(dfr) > 0), "df for plotting is empty. Make sure x values exists."
        p = (ggplot(aes(x=yval, y=zval, color='variable'), data=dfr) + geom_tile()
             + geom_line(size=1)
             + ggtitle(f"{zval} versus {yval} for {xval}={x}")
             + xlim(params['xmin'], params['xmax'])
             + ylim(0, 1)
             )
        p.save(params['path_to_figs'] + f"/power_versus_y_x{x}.png")
        ls_p[x] = p
    return ls_p

def illustrate_phase_diagram(df: pd.DataFrame, params: Dict,
                             title="{} phase diagram") -> Dict:
    """
    Illustrate phase diagram of data in `df`

    :param df:      dataframe with at least three columns for variables
                    x, y, z
    :param params:  indicates names of x, y, z columns and axes limits
    :param title:   title of figure
    :return:        dictionary of pyplot object
    """

    xval = params['x_value']
    yval = params['y_value']
    zval = params['z_value']

    ls_p = {}
    for variable in params['variables']:
        dfr = df[df.variable == variable]
        p = (ggplot(aes(x=xval, y=yval, fill=zval),
                        data=dfr) + geom_tile()
             + theme(legend_position='none', legend_key_width=100,
                     #legend_title=element_blank()
                     )
             + geom_line(aes(x=xval, y='crit'), color='red', size=1)
             + ggtitle(title.format(variable))
             + scale_fill_gradient(low='darkblue', high='yellow', na_value='salmon')
             + xlim(params['xmin'], params['xmax'])
             + ylim(params['ymin'], params['ymax'])
             )
        p.save(params['path_to_figs'] + f"/phase_diagram_{variable}")
        ls_p[variable] = p
    return ls_p


def _interpolate(dfr, params, success_rate_thr):
    """
    interpolate evaluation in dfr on a grid provided in params['interpolation_grid']

    :param dfr:
    :param params:
    :param success_rate_thr: The rate above which we report success
    :return:
        interpolated
    """

    def _inv_logit(x):
        return 1 / (1 + np.exp(-x))

    xname = params['xname']
    yname = params['yname']

    gp = params['interpolation_grid']

    grid_x, grid_y = np.mgrid[gp['xmin']:gp['xmax']:gp['dx'],
                     hp['ymin']:hp['ymax']:gp['dy']]
    dfr = dfr.sort_values(yname).reset_index()
    func_gr = scipy.interpolate.griddata((dfr.beta, dfr.r),
                                         dfr.logit.values,
                                         (grid_x, grid_y), method='linear')
    df_interp = pd.DataFrame()
    df_interp.loc[:, xname] = grid_x.ravel()
    df_interp.loc[:, yname] = grid_y.ravel()
    df_interp.loc[:, 'prob'] = _inv_logit(func_gr).ravel()

    df_interp.loc[:, 'succ_sig'] = (df_interp['prob'] > success_rate_thr) + 0.0
    return df_interp
