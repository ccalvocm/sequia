# -*- coding: utf-8 -*-
"""
Created on Wed Jul 14 14:23:20 2021

@author: Carlos
"""
#========================================================================== 
#                           cargar librerias
#==========================================================================
import pandas as pd
import datetime
from statsmodels.tsa.arima.model import ARIMA
from dateutil.relativedelta import relativedelta
import os
import numpy as np
import pmdarima as pm
import warnings
warnings.filterwarnings('ignore')

# funciones

def difference(dataset, interval=1):
    diff = list()
    for i in range(interval, len(dataset)):
        value = dataset[i] - dataset[i-interval]
        diff.append(value)
    return np.array(diff)

def inverse_difference(history, yhat, interval=1):
	return yhat + history.iloc[:-interval]

def inverse_diff(differentiated, original, interval=1, unit = 'D'):
    val = []
    idx = []
    for t in differentiated.index:
        dt = pd.Timedelta(value = interval, unit = unit)
        idx.append(t)
        val.append(differentiated.loc[t] + original.loc[t-dt])
    inverted_series = pd.Series(data=val,index = idx)
    return inverted_series

def matchSnow(master_, last_date, df_h, cols):
    """
    

    Parameters
    ----------
    df : TYPE
        DESCRIPTION.
    master_ : TYPE
        DESCRIPTION.
    last_date : TYPE
        DESCRIPTION.

    Returns
    -------
    None.

    """
    
    # año de pronóstico
    forecast_year = master_.index[-1].year-1
    
    # seleccionar sólo los datos medidos    
    master_ = master_.loc[master_.index <= last_date]
    
    # calcular el área cubierta de nieve de la cuenca
    wSCAhi = pd.DataFrame(np.sum(master_[cols].mul(df_h['area'].values),
                                 axis = 1) / df_h['area'].sum())
    
    first_date = pd.to_datetime(str(forecast_year)+'-04-01')        
    last_CDC = wSCAhi.loc[(wSCAhi.index >= first_date ) & (wSCAhi.index <= last_date)]
    len_last_cover = len(last_CDC.index)
    
    # años anteriores
    previous_years = list(dict.fromkeys(wSCAhi.index.year))[:-2]
    
    # fig, ax = plt.subplots(1)
    # comparar las curvas de agotamiento acumuladas CDC de los años anteriores
    
    rmse = np.inf
    best_rmse = np.inf
    best_year = 2019
    
    # iterar sobre los años
    for yr in previous_years:
        start =  pd.to_datetime(str(yr)+'-04-01')    
        end = start + datetime.timedelta(days = len_last_cover-1)   
        historical_CDC =  wSCAhi.loc[pd.date_range(start,end)]
        rmse = funcRMSE(historical_CDC, last_CDC)
        if best_rmse > rmse:
            best_rmse = rmse
            best_year = yr
    
    # devolver el mejor año
    return best_year

def funcRMSE(df1,df2):
    #return root mean square error between df1 and df2
    return np.sqrt(np.mean((df1.values-df2.values)**2))
        
# def pronostico_ARMA(df, orden, lastdate, end_date):

#     # Parameters
#     # ----------
#     # df : Pandas DataFrame
#     #     dataframe de nieve medida.
#     # dias : int
#     #     dias del periodo predictivo.
#     # orden : tuple
#     #     orden del ARMA.

#     # Returns
#     # -------
#     # predictions : Pandas DataFrame
#     #     cubierta nival pronosticada.
                
#     train, test = pm.model_selection.train_test_split(df, train_size=(0.79))
    
#     # días a pronosticar
#     p_length = pd.date_range(lastdate-datetime.timedelta(days = len(test)-1), end_date,freq = '1d')           

#     # it is required to drop the na
#     train_differentiated = train.diff(365*5).dropna()
#     # train_differentiated2 = difference(train.values,1)
#     model = ARIMA(train_differentiated, order=(7,1,0))
#     model_fit = model.fit(method='innovations_mle', low_memory=True, cov_type='none')
    
#     # train_dif_inverse = inverse_diff(train_differentiated, train, interval=365)
                            
#     forecast = model_fit.forecast(steps=len(p_length))
#     forecast = inverse_diff(forecast, df, interval=len(p_length))
#     forecast = forecast.loc[forecast.index > lastdate]
    
#     return forecast

def pronostico_ARMA(df, dias, orden):

    # Parameters
    # ----------
    # df : Pandas DataFrame
    #     dataframe de nieve medida.
    # dias : int
    #     dias del periodo predictivo.
    # orden : tuple
    #     orden del ARMA.

    # Returns
    # -------
    # predictions : Pandas DataFrame
    #     cubierta nival pronosticada.
    
    model = ARIMA(df.astype(float),order=orden,seasonal_order=(0,0,0,365))
    # fit model
    try:
        result = model.fit(method='statespace',low_memory=True,cov_type='none')

        # make prediction
        predictions = result.predict(1, dias,
                                 typ = 'levels')
    
    except ZeroDivisionError:
        predictions=df.iloc[1:]

    return predictions

def snow_forecast(root):

    # Parameters
    # ----------
    # years : int
    #     años a modelar, ejemplo 20002020.
    # root : str
    #     directorio raíz.
    # Returns
    # -------
    # None.    

    #construir las coberturas del predictivo
    years_train =  10
    
    # leer el arhcivo master del periodo de pronostico
    
    try:
        #caudal pronosticado en m3/s
        master = pd.read_csv(os.path.join(root,'Master.csv'), 
                             index_col = 0, parse_dates = True)
    except FileNotFoundError:
        print("Wrong file or file path")
     
    # cargar la curva hipsométrica
    df_hypso = pd.read_csv(os.path.join(root,'bands_mean_area.csv'),
                           index_col = 0)
    
    # leer última fecha de las imágenes modis
    last_date = pd.read_csv(os.path.join(root,'LastDateVal.csv'),index_col=0,
                             parse_dates = True).index[-1]
        
    # asignar las fechas
    master = master.loc[master.index <= last_date]

    last_snow = master[[x for x in master.columns if ('Zone' in x) & ('.' not in x)]]
    last_snow.dropna(inplace = True)

    iDate=last_date+datetime.timedelta(days = 1)   
    idx=pd.date_range(iDate, iDate+np.timedelta64(4, 'M'), freq = '1d')      
    
    # extender el archivo master hasta el pronóstico
    complemento = pd.DataFrame([], index = idx, columns = master.columns)
    master = master.append(complemento)
    
    # cargar coberturas de nieve y glaciares
    cols_SCA = [x for x in master.columns if ('Zone' in x) & ('.' not in x)]
    SCA = master[cols_SCA]         # Snow Covered Area (%) 
    cols_GCA = [x for x in master.columns if ('Zone' in x) & ('.' in x)]
    GCA = master[cols_GCA]     # Glacier Covered Area (%)

    # año que mejor se ajusta a la nieve acumulada
    best_year_s = matchSnow(master.loc[master.index.year >= last_date.year-years_train], 
                            last_date, df_hypso, cols_SCA)      
  
    for ind, col in enumerate(SCA.columns):
        
        # coberturas de nieve para el periodo de entrenamiento
        idx_s=pd.date_range(last_date-relativedelta(years=(last_date.year-best_year_s)), 
                            last_date,freq = '1d')
        
        if (SCA.loc[idx_s,col] > 0).any():
            
            # realizar el pronóstico
            pronostico_nieve=pronostico_ARMA(SCA.loc[idx_s,col],last_date,
                                             (2,1,0))
            pronostico_nieve[pronostico_nieve < 0] = 0
            pronostico_nieve[pronostico_nieve > 1] = 1
            
            # guardar el pronóstico
            idx2 = pd.date_range(pd.to_datetime(pd.to_datetime(last_date)+datetime.timedelta(days = 1)),
                                 master.index[-1], freq = '1d')
            master.loc[idx2,col] = pronostico_nieve.iloc[:len(idx2)].values
        
        # coberturas de glaciares
        idx_g = pd.date_range(pd.to_datetime(last_date)-relativedelta(years=(last_date.year - best_year_s)),
                              pd.to_datetime(last_date),freq = '1d')
        if (GCA.loc[idx_g,col+'.1'] > 0).any():
            
            # realizar el pronóstico
            pronostico_glaciares = pronostico_ARMA(GCA.loc[idx_g,col+'.1'],
                                                   last_date,(2,1,0))
            pronostico_glaciares[pronostico_glaciares < 0] = 0
            pronostico_glaciares[pronostico_glaciares > 1] = 1
            
            # guardar el pronóstico
            master.loc[idx2,col+'.1'] = pronostico_glaciares.iloc[:len(idx2)].values
    
    # # completar los parametros que faltan del predictivo
    # par = [x for x in master.columns if ('Zone' not in x) & ('Pp_' not in x) & ('T_' not in x) & ('Measured Discharge' not in x)]
    # last_par = master['Recess_X']
    # last_par.dropna(inplace = True)
    
    # # completar los parametros del predictivo junto a los dias de años bisiestos
    # idx_missing = pd.date_range(last_par.index[-1]+datetime.timedelta(days = 1), master.index[-1], freq = '1d' )
    # feb_bisiesto = [f for f in idx_missing if (f.month == 2) & (f.day == 29)]
    # for date in idx_missing:
    #     master.loc[date, par] = master.loc[pd.to_datetime(str(date.year-1)+'-'+str(date.month)+'-'+str(date.day)), par].values
    # for feb in feb_bisiesto:
    #     master.loc[feb, par] = master.loc[feb-datetime.timedelta(days = 1), par]
    
    # master = master.reset_index(drop = True)
    # master.index = master.index+1
    master[cols_SCA] = master[cols_SCA].fillna(0)
    master[cols_GCA] = master[cols_GCA].fillna(0)
   
    return master
