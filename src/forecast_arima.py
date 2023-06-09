#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Oct  2 14:41:47 2021

@author: faarrosp
"""

import os
import pandas as pd
import pmdarima as pm
from pmdarima import model_selection
import matplotlib.pyplot as plt
import numpy as np
from pmdarima.arima import ndiffs

def compute_NSE_series(ts_sim,ts_obs):
    '''
    

    Parameters
    ----------
    ts1 : TYPE
        DESCRIPTION.
    ts2 : TYPE
        DESCRIPTION.

    Returns
    -------
    idx_both : TYPE
        DESCRIPTION.

    '''
    idx_both = set(ts_sim.index).intersection(set(ts_obs.index))
    x = ts_sim.loc[sorted(idx_both)].values
    y = ts_obs.loc[sorted(idx_both)].values
    my_nse = evaluator(nse, x, y, axis=1)
    
    return round(my_nse[0],3)

def inverse_diff(differentiated, original, interval=1, unit = 'D'):
    val = []
    idx = []
    for t in differentiated.index:
        # print(t)
        dt = pd.Timedelta(value = interval, unit = unit)
        idx.append(t)
        # print(t-dt)
        val.append(differentiated.loc[t] + original.loc[t-dt])
    inverted_series = pd.Series(data=val,index = idx)
    return inverted_series


def forecast_method_1(ts, order=(7,0,1)):
    train, test = model_selection.train_test_split(ts, train_size=0.8)
    trainD = train.diff(365).dropna()
    testD = test.diff(365).dropna()
    
    # Fit with some validation (cv) samples
    arima = pm.ARIMA(order=order)
    arima.fit(trainD)
    
    # Now plot the results and the forecast for the test set
    preds, conf_int = arima.predict(n_periods=test.shape[0],
                                    return_conf_int=True)
    
    # generate predictions as Pandas Series
    preds = pd.Series(preds, index=test.index)
    conf_int_lower = pd.Series(conf_int[:,0], index = test.index)
    conf_int_upper = pd.Series(conf_int[:,1], index = test.index) 
    
    # apply inverse difference to get real values of prediction
    predsINV = inverse_diff(preds,ts,interval=365)
    
    arima.update(testD)

    new_preds, new_conf_int = arima.predict(n_periods=365, return_conf_int=True)
    new_preds_index = pd.date_range(start=test.index[-1], periods = 366, 
                                    closed='right')
    new_preds = pd.Series(new_preds, index=new_preds_index)
    new_predsINV = inverse_diff(new_preds,ts,interval=365)
    
    ts_updated = pd.concat([ts,new_predsINV], axis=0)
    return ts_updated

def forecast_method_2(ts, order=(1,1,1), sorder = (0,1,1,12)):
    train, test = model_selection.train_test_split(ts, train_size=0.8)
    # trainD = train.diff(365).dropna()
    # testD = test.diff(365).dropna()
    
    # Fit with some validation (cv) samples
    arima = pm.ARIMA(order=order, seasonal_order = sorder)
    arima.fit(train)
    
    # Now plot the results and the forecast for the test set
    preds, conf_int = arima.predict(n_periods=test.shape[0],
                                    return_conf_int=True)
    
    # generate predictions as Pandas Series
    preds = pd.Series(preds, index=test.index)
    conf_int_lower = pd.Series(conf_int[:,0], index = test.index)
    conf_int_upper = pd.Series(conf_int[:,1], index = test.index) 
    
    # apply inverse difference to get real values of prediction
    # predsINV = inverse_diff(preds,ts,interval=365)
    
    # arima.update(testD)

    new_preds, new_conf_int = arima.predict(n_periods=365, return_conf_int=True)
    new_preds_index = pd.date_range(start=test.index[-1], periods = 366,
                                    closed='right')
    new_preds = pd.Series(new_preds, index=new_preds_index)
    # new_predsINV = inverse_diff(new_preds,ts,interval=365)
    
    ts_updated = pd.concat([ts,new_preds], axis=0)
    return ts_updated
    
    
def compute_ndiffs(ts):
    kpss_diffs = ndiffs(ts, alpha=0.05, test='kpss', max_d=6)
    adf_diffs = ndiffs(ts, alpha=0.05, test='adf', max_d=6)
    n_diffs = max(adf_diffs, kpss_diffs)    
    print(f"Estimated differencing term: {n_diffs}")
    
    
def forecast_dataframe_file(path):
    df = pd.read_csv(path, index_col=0, parse_dates=True)
    forecasted_series = []
    for col in df.columns:
        ts = df[col]
        ts_forecasted = forecast_method_1(ts)
        forecasted_series.append(ts_forecasted)
    df_forecasted = pd.concat(forecasted_series, axis=1)
    df_forecasted.to_csv(path.replace('.csv','_forecast.csv'))
        
        
    
if __name__ == '__main__':    
    # path = ['/Users', 'felipearrospide','Documents','GitHub',
    #         'Demo-SRM','04_Maule','01_RMEA',
    #         'Temperatura', 'temperatura_actual.csv']
    
    # path_dataset = r'G:\Demo-SRM\04_Maule\01_RMEA\Temperatura\temperatura_actual.csv'
    # path_dataset=r'G:\Demo-SRM\01_Maipo\02_RMEEM\Temperatura\temperatura_actual.csv'
    datasets=['tminERA5.csv','tmaxERA5.csv','SWE_basin_79_23.csv',
    'SfPress_basin_79_23.csv','SCA_basin_79_23.csv','SAlbedo_basin_79_23.csv',
    'pp_basin_79_23.csv','PET_basin_79_23.csv']
    root=r'G:\pangeo_lstm_example\modelos\combarbala'
    for set in datasets:
        path_dataset=os.path.join(root,set)

    # path_dataset = os.path.join(*path)
    
        forecast_dataframe_file(path_dataset)
    
    # df = pd.read_csv(path_dataset, index_col=0, parse_dates=(True))
    
    # ts = df['0']
    
    # ts_new = forecast_method_2(ts)
    
    
    # train, test = model_selection.train_test_split(ts, train_size=0.95)
    
    # compute_ndiffs(train.values)
