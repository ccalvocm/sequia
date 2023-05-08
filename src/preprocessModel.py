# -*- coding: utf-8 -*-
"""
Created on Tue May 18 15:28:01 2021

@author: Carlos
"""

import pandas as pd
import os
import fiscalyear
import numpy as np
import datetime
import os
import csv
import geopandas as gpd
fiscalyear.START_MONTH = 4    

# funciones

def fixMaster(path):
    pathMaster=os.path.join(path,'Master.csv')
    pathBands=os.path.join(path,'bands_mean_area.csv')
    master=pd.read_csv(pathMaster,index_col=0,parse_dates=True)
    # master.iloc[1:,:]=np.nan
    nBands=len(pd.read_csv(pathBands))
    masterOut=master[[x for x in master.columns if str(nBands+1) not in x]]
    masterOut.to_csv(os.path.join(path,'Master.csv'))
    return None

def agnohidrologico(date):
    year_ = date.year
    month_ = date.month
    cur_dt = fiscalyear.FiscalDate(year_, month_, 1) 
    retornar = cur_dt.fiscal_year - 1
    return int(retornar)

def by_yr(df,est):
    df = df[est]
    df = df.loc[df.index.year >= 2000]
    yrs = df.index.year
    yrs_dr = list(dict.fromkeys(yrs))
    
    for yr in yrs_dr:
        df_yr = df[df.index.year == yr]
        df_yr = df_yr.reset_index(drop = True)
        df_yr.index = df_yr.index + 1
        df_yr.to_excel('TotalDischarge'+str(yr)+'.xls',columns=None,header=True)

def ordenarDGA(df,df_DGA):
    output=pd.DataFrame([],index=pd.date_range(start='1900-01-01',
                                               end='2020-12-31',closed=None))

    for ind, est in enumerate(df):
        q_est = df_DGA[df_DGA.iloc[:,0] == df[ind]]
        fechas = pd.to_datetime(q_est.iloc[:,2], dayfirst = True)
        caudal = q_est.iloc[:,3]
        flags = q_est.iloc[:,4]
        
        q_est_df=pd.DataFrame(caudal.values,index=fechas,columns=[est])
        flags_est=pd.DataFrame(flags.values,index=fechas,columns=['flags'])
            
        output.loc[output.index,est] = q_est_df[est]
        output.loc[output.index,'flag'] = flags_est['flags']

    return output        

def borrar_comillas(file):
    # borrar las comillas dobles
    reading_file = open(file, "r")
    
    new_file_content = "\n"
    for line in reading_file:
      new_line = line.replace("\"", "")
      new_file_content += new_line
    reading_file.close()
    
    writing_file = open(file, "w")
    writing_file.write(new_file_content)
    writing_file.close()         

def extendMaster(df):
    dates=pd.date_range(df.index[0],pd.to_datetime('2023-02-26'))
    dfOut=pd.DataFrame(index=dates,columns=df.columns)
    dfOut.loc[df.index,df.columns]=df.values
    return dfOut
    
def resampleMon(df,how='mean'):
    if how=='mean':
        return df.resample('MS').mean()
    elif how=='sum':
        return df.reample('MS').sum()
    else:
        return None

def autocompleteCol(df):
    colsNotna=df.dropna(how='all',axis=1).columns
    colsNa=[x for x in df.columns if x not in colsNotna]
    dfOut=df[:]
    if len(colsNa)>0:
        for col in colsNa:
            if col<colsNotna.min():
                dfOut[col]=df[colsNotna[colsNotna>col].min()]
            else:
                dfOut[col]=df[colsNotna[colsNotna<col].max()]
    return dfOut
           
def fixSWE(df):
    df[df < 1e-5] = 0.0
    return df
 
def loadArea(ruta):
    rutaArea=os.path.join(ruta,'bands_mean_area.csv')
    df=pd.read_csv(rutaArea)
    return df['area']

def createSweObsIns(df_swe_mon,est,swe_wgt,root,cuenca):
    # crear el obsgroup del pst
    swe_stack = pd.DataFrame(df_swe_mon.stack().reset_index(drop = True))
    swe_stack.columns = [est]
    indices = swe_stack.index+1
    swe_stack['ins'] = ['swe'+str(x) for x in indices]
    swe_stack['obsgroup'] = 'swe'
    swe_stack['wgt'] =  pd.DataFrame(swe_wgt.stack().reset_index(drop = True))
    swe_stack = swe_stack[['ins', est, 'wgt', 'obsgroup']]
    folderOut=os.path.join(root,cuenca)
    swe_stack.to_csv(os.path.join(folderOut,'swe_observation_group.txt'),
                     index = False, header = None, mode='a', sep = ' ')

    def createSweIns(swe_stack,df_swe_mon,folderOut):
        #################################################
        #         escribir el .ins del SWE              #
        #################################################
        
        swe_ins = ['l1 #,#!'+str(x)+'!' for x in swe_stack['ins']]
        result = pd.DataFrame([w.replace('l1',
            '') if i%df_swe_mon.shape[1] else w for i,w in enumerate(swe_ins)])
    
        ins_df_swe = df_swe_mon.copy()
        ins_df_swe[:] = result.values.reshape(df_swe_mon.shape[0],
                                              df_swe_mon.shape[1])
        ins_df_swe.to_csv(os.path.join(folderOut,'swe.ins'),index=False,
                          header=None,mode='a',sep=' ')
        borrar_comillas(os.path.join(folderOut,'swe.ins'))

    return createSweIns(swe_stack,df_swe_mon,folderOut)

def swe(cuenca,root,dictCuenca):
    
    rutaSwe=os.path.join(root,cuenca,'SWE_bands_ERA5.csv')
    # df SWE
    df_swe = pd.read_csv(rutaSwe, index_col = 0, parse_dates = True)
    df_swe=df_swe.loc[df_swe.index<=fecha]
    
    # resamplear mensual
    df_swe_mon = resampleMon(df_swe)
    
    # completar columnas faltantes
    df_swe_mon=autocompleteCol(df_swe_mon)
    
    # valores negativos y cero
    df_swe_mon=fixSWE(df_swe_mon)
    
    # pesos proporcionales al area
    swe_wgt = df_swe_mon[:]
    
    # cargar las areas de la cuenca
    dfArea=loadArea(os.path.join(root,cuenca))
    
    # calcular pesos según el área
    swe_areas = dfArea/np.sum(dfArea)
    swe_areas = swe_areas/np.max(swe_areas)
    
    for i,col in enumerate(swe_wgt.columns):
        swe_wgt[col] = swe_areas[i]
    
    # escribir los inputs de PEST
    est=dictCuenca[cuenca][1]
    createSweObsIns(df_swe_mon,dictCuenca[cuenca][1],swe_wgt,root,cuenca)
    return None
    
def addCaudales(root,cuenca,master,dictCuenca):
    """
    

    Returns
    -------
    None.

    """
    #################################################
    #                   caudales                    #
    #################################################
               
    # df caudales
    master=master[:]
    ruta=os.path.join(root,cuenca,'qDayNat.csv')
    df_qaux = pd.read_csv(ruta, index_col = 0, parse_dates = True)
    df_qaux=pd.DataFrame(df_qaux.loc[(df_qaux.index.year>=2000) & (df_qaux.index<=fecha)])
    df_q=pd.DataFrame([],index=pd.date_range('2000-01-01',fecha,
                                             freq='1d'))
    df_qaux['wgt'] = 1e-1
    col=df_qaux.columns[0]
    df_qaux['wgt'][df_qaux[col]<=df_qaux[col].quantile(0.5)]=1
    est=df_qaux.columns[0]
    df_q.loc[df_qaux.index, [est, 'wgt']] = df_qaux.values
                   
    # guardar en el master
    idx = master.index.intersection(df_qaux.index)
    master['Measured Discharge'] = np.nan
    master.loc[idx, 'Measured Discharge'] = df_qaux.loc[idx,est].values
    
    def insCaudales(df_q,root):
        #################################################
        #       escribir el .ins de caudales            #
        #################################################
        
        df_q.reset_index(inplace = True)
        ins = list(df_q.index+1)
        df_q['ins'] = ['l1 #,#!q'+str(x)+'!' for x in ins]
        df_q.loc[df_q[df_q[est].isnull()].index,'ins'] = 'l1 #,#!dum!'
        df_q.loc[df_q[df_q[est].isnull()].index,est] = 'NaN'
        df_qpst = df_q.copy()[df_q[est].copy() != 'NaN']
        for ind,row in df_qpst.iterrows():
            pst = df_qpst.loc[ind, 'ins'].split('!')
            df_qpst.loc[ind, 'ins'] = pst[1]
        df_qpst[est] = df_qpst[est]*1e3
        df_qpst['obsgroup'] = 'q'
        del df_qpst['index']
        df_qpst = df_qpst[['ins', est, 'wgt', 'obsgroup']]
        df_qpst.to_csv(os.path.join(root,cuenca,'Master_obervation_data.pst'),
                        index = False, sep = ' ', header = None)
        ins = pd.DataFrame(df_q['ins'])
        ins.to_csv(os.path.join(root,cuenca,'q.ins'),index=False,header=None)
        
    insCaudales(df_q,root)
    return master

def addPp(rootSIG,cuenca,master):
    master=master[:]
    rutaPp=os.path.join(rootSIG,cuenca,'pr_Bands_ERA5.csv')

    # df precipitaciones
    df_pp = pd.read_csv(rutaPp, index_col = 0, parse_dates = True)

    # completar las columnas si es que faltan
    dfPp=autocompleteCol(df_pp)
    
    # asignar precipitaciones al master
    cols_pp = [x for x in master if 'Pp_z' in x]
    idx = master.index.intersection(dfPp.index)
    master.loc[idx, cols_pp] = dfPp.loc[idx].values
    return master
    
def addT(root,cuenca,master):
    master=master[:]
    ruta_t=os.path.join(root,cuenca,'t2m_bands_ERA5.csv')
    
    df_t = pd.read_csv(ruta_t, index_col = 0, parse_dates = True)
    df_t = df_t.resample('D').mean()-273.15

    # completar las columnas si es que faltan
    dfT=autocompleteCol(df_t)    

    # asignar temperaturas al master
    cols_t = [x for x in master if 'T_z' in x]
    
    # guardar temperatura en el master
    idx_t = master.index.intersection(dfT.index)
    master.loc[idx_t, cols_t] = df_t.loc[idx_t].values
    
    return master

def addRho(root,cuenca,master):
    master=master[:]
    rutaR=os.path.join(root,cuenca,'rhoSd.csv')
    
    dfR=pd.read_csv(rutaR, index_col = 0, parse_dates = True)

    # completar las columnas si es que faltan
    dfR=autocompleteCol(dfR)    

    # guardar temperatura en el master
    idxR = master.index.intersection(dfR.index)
    master.loc[idxR, ['DegDaySnow']]=1.1*dfR.loc[idxR].values/999.87 

    return master
    
def addSummerDays(master):
           
    # --------------------dias de verano
    summer_d=pd.DataFrame([],index=pd.date_range('2000-01-01',
                                            master.index[-1]),columns=['day'])
    summer_d['day'] = 0
    summer_d.loc[summer_d.index.month.isin([1,2,3,12]),'day'] = 1
    marzo = summer_d.loc[summer_d.index.month == 3]
    for ind,row in marzo.iterrows():
        if ind.day > 21:
            summer_d.loc[ind,'day'] = 0
    diciembre = summer_d.loc[summer_d.index.month == 12]
    for ind,row in diciembre.iterrows():
        if ind.day < 21:
            summer_d.loc[ind,'day'] = 0

    # guardar temperatura en el master
    master.loc[master.index, 'summer'] = summer_d.values[:,0]
    
    return master

def addSnowGlacier(root,cuenca,master):
    master=master[:]
    ruta_n=os.path.join(root,cuenca,'snowCover.csv')
    ruta_g=os.path.join(root,cuenca,'glacierCover.csv')

    # lerr df
    dfSnow=pd.read_csv(ruta_n,index_col=0,parse_dates=True)
    dfGlacier=pd.read_csv(ruta_g,index_col=0,parse_dates=True)

    cols_snow = [x for x in master.columns if ("Zone" in x) & ('.' not in x)]
    cols_g = [x for x in master.columns if ("Zone" in x) & ('.' in x)]
    
    idx = master.index.intersection(dfSnow.index)
    master.loc[idx,cols_snow] = dfSnow.loc[idx].values
    
    idx = master.index.intersection(dfGlacier.index)

    master.loc[idx,
        list(np.array(cols_g)[dfGlacier.columns.astype(int)])] = dfGlacier.loc[idx].values
    master[cols_g]=master[cols_g].fillna(1)

    # agregar densidad de la nieve
    master=addRho(root,cuenca,master)
    
    return master

def addSnow(root,cuenca,master):
    ruta_n=os.path.join(root,cuenca,'Nieve')
    # iterar sobre coberturas nivales
    df_n=pd.read_csv(os.path.join(root,cuenca,
                                'snowCover.csv'),
                                index_col=0,parse_dates=True)
    df_g=pd.read_csv(os.path.join(root,cuenca,
                                'glacierCover.csv'),
                                index_col=0,parse_dates=True)
    cols_snow = [x for x in master.columns if ("Zone" in x) & ('.' not in x)]
    cols_g = [x for x in master.columns if ("Zone" in x) & ('.' in x)]
    
    idx = master.index.intersection(df_n.index)
    master.loc[idx,cols_snow] = df_n.loc[idx].values
    
    idx = master.index.intersection(df_g.index)
    master.loc[idx,cols_g] = df_g.loc[idx].values
    
    return master
    
def exportMaster(master,root,cuenca):
    import shutil
    master=master[:]
    rutaMasterout=os.path.join(root,cuenca,'Master.tpl')
    shutil.copyfile(rutaMasterout, rutaMasterout.replace('.tpl',
                                '.tpl.back'), follow_symlinks=True)
    
    master.to_csv(rutaMasterout)
    return None    

def interSectGlaciers(pathBands,pathGlaciers):
    gdfBands=gpd.read_file(pathBands)
    gdfGlaciers=gpd.read_file(pathGlaciers)

    gdfInter=gpd.overlay(gdfBands,gdfGlaciers)
    gdfInter=gdfInter.dissolve(by='FID')

    return gdfInter

def matchPp(master,lday, df_h):
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

    # seleccionar sólo los datos medidos
    pp_=master[[x for x in master.columns if 'Pp_' in x]]
    pp_ = pp_.loc[pp_.index <= lday]

    # año de pronóstico
    if lday.strftime('%m-%d') > '03-31':
        forecast_year = pp_.index[-1].year
    else:
        forecast_year = pp_.index[-1].year-1

    # calcular la precipitación media de la cuenca
    pp_mean = pd.DataFrame(
        np.sum(pp_.mul(df_h['area'].values), axis=1) / df_h['area'].sum())

    first_date = pd.to_datetime(str(forecast_year)+'-04-01')
    last_ppmean = pp_mean.loc[(pp_mean.index >= first_date)
                              & (pp_mean.index <= lday)]
    len_last_ppmean = len(last_ppmean.index)

    # años anteriores
    previous_years = list(dict.fromkeys(pp_mean.index.year))[:-1]

    # calcular la precipitación acumulada de los años anteriores
    ppmean_registry = pd.DataFrame([], index=previous_years, columns=['ppm'])

    # iterar sobre los años
    for yr in previous_years:
        start = pd.to_datetime(str(yr)+'-04-01')
        end = start + datetime.timedelta(days=len_last_ppmean-1)
        ppmean_registry.loc[yr, 'ppm'] = float(
            pp_mean.loc[pd.date_range(start, end)].sum().values[0])

    df_pp_30 = ppmean_registry['ppm'].quantile(1-0.3)

    # if (last_ppmean.sum().values >= df_pp_30) | (forecast_year in [2020,2021]):
    if last_ppmean.sum().values >= df_pp_30:
        return 2000
    else:
        return 2019

def completarMaster(root,cuenca,master,df_h):
    
    # extraer la precipitacion
    pp = master[[x for x in master.columns if 'Pp_' in x]]

    # completar los parámetros
    par = [x for x in master.columns if ('Zone' not in x) & (
        'Pp_' not in x) & ('T_' not in x) & ('Measured Discharge' not in x)]
    last_par = master['Recess_X']
    last_par.dropna(inplace=True)

    # completar los parametros del predictivo junto a los dias de años bisiestos
    idx_missing = pd.date_range(
        last_par.index[-1]+datetime.timedelta(days=1), master.index[-1], freq='1d')
    yrs_missing = list(dict.fromkeys(idx_missing.year))

    # iterar sobre los años hidrologicos
    for yr in yrs_missing:

        # mínimo entre el ultimo día de los parámetros faltantes y el último día del año hidrológico
        last_day_yr = min(pd.to_datetime(str(yr+1)+'-03-31'), idx_missing[-1])

        # determinar si el año hasta las observaciones es seco o húmedo
        yr_param = matchPp(pp, last_day_yr, df_h)

        # tengo que hacer el for por si hay años bisiestos
        if min(idx_missing).strftime('%m-%d') > '03-31':
            idx_date = idx_missing[(idx_missing <= last_day_yr) & (
                idx_missing >= pd.to_datetime(str(yr)+'-04-01'))]
        else:
            idx_date = idx_missing[(idx_missing <= last_day_yr) & (
                idx_missing >= pd.to_datetime(str(yr-1)+'-04-01'))]
        for date in idx_date:
            yr_delta = date.year-idx_date.year[0]

            master.loc[date, par] = master.loc[pd.to_datetime(
                str(yr_param+yr_delta)+'-'+str(date.month)+'-'+str(date.day)), par].values

        # identificar los días bisiestos a completar
        feb_bisiesto = [f for f in idx_date if (f.month == 2) & (f.day == 29)]

        for feb in feb_bisiesto:
            yr_delta = feb.year-idx_date.year[0]
            if yr_param < 2018:
                master.loc[feb, par] = master.loc[pd.to_datetime(
                    str(2000+yr_delta)+'-02-29'), par].values
            else:
                master.loc[feb, par] = master.loc[pd.to_datetime(
                    str(2019+yr_delta)+'-02-29'), par].values

    return master

def agnohidrologico(date):
    year_ = date.year
    month_ = date.month
    cur_dt = fiscalyear.FiscalDate(year_, month_, 1) 
    retornar = cur_dt.fiscal_year - 1
    return int(retornar)

def createTPL(root,cuenca,master):
    pathq=os.path.join(root,cuenca,'qDayNat.csv')
    df_q=pd.read_csv(pathq,index_col=0,
                     parse_dates=True)
    df_k = pd.DataFrame([],index = df_q.index, columns = df_q.columns)
    y = pd.DataFrame([],index = df_q.index, columns = df_q.columns)
    x = pd.DataFrame([],index = df_q.index, columns = df_q.columns)
    
    # calcular los coeficientes k
    for ind in df_q.index[:-1]:
        df_k.loc[ind] = df_q.loc[ind+datetime.timedelta(days=1)].values/df_q.loc[ind].values
    
    df_k = df_k.astype(float)
    # calcular los coeficientes x e y
    for ind in df_q.index[:-2]:
        y.loc[ind] = -np.log(df_k.loc[ind+datetime.timedelta(days=2)].values/df_k.loc[ind+datetime.timedelta(days=1)].values)/np.log(df_q.loc[ind+datetime.timedelta(days=1)].values/df_q.loc[ind].values)
        # if np.abs(y.loc[ind].values) > 100:
        #     x.loc[ind] = 0
        # else:
        #     x.loc[ind] = df_k.loc[ind+timedelta(days=1)].values/(df_q.loc[ind].values**(-y.loc[ind].values))
        x.loc[ind] = np.exp((np.log(df_q.loc[ind].values)*np.log(df_k.loc[ind+datetime.timedelta(days=2)])-np.log(df_q.loc[ind+datetime.timedelta(days=1)].values)*np.log(df_k.loc[ind+datetime.timedelta(days=1)]))/(np.log(df_q.loc[ind].values)-np.log(df_q.loc[ind+datetime.timedelta(days=1)].values)))
       
    df_x = df_k.loc[df_k.index.year >= 2000].copy()
    df_x.loc[df_x.index.month.isin(list(range(6,9)))] = '#xw               #'
    df_x.loc[df_x.index.month.isin(list(range(1,6)))] = '#x               #'
    df_x.loc[df_x.index.month.isin(list(range(9,13)))] = '#x               #'
    df_y =  df_k.loc[df_k.index.year >= 2000].copy()
    df_y.loc[df_y.index.month.isin(list(range(6,9)))] = '#yw               #'
    df_y.loc[df_y.index.month.isin(list(range(1,6)))] = '#y               #'
    df_y.loc[df_y.index.month.isin(list(range(9,13)))] = '#y               #'
    
    # calcular las probabilidades de excedencia por año hidrológico
    df_q['hidroyear'] = ''

    for ind, col in df_q.iterrows():
        hidro_yr = agnohidrologico(ind)
        df_q.loc[ind, 'hidroyear'] = hidro_yr
    df_q.index.names = ['']
    df_q = df_q.groupby('hidroyear').mean()
    df_q50 = df_q.quantile(1-0.5)
    
    df_q['régimen'] = 'seco'
    df_q.loc[(df_q[df_q.columns[0]] >= df_q50.values[0]),'régimen'] = 'húmedo'
        
    # años húmedos y secos
    wet_years = df_q[df_q['régimen'] == 'húmedo'].index
    dry_years = df_q[df_q['régimen'] == 'seco'].index
    df_k['hidroyear'] = ''
    idx = df_k.index.intersection(master.index)
    df_k = df_k.loc[idx]

    df_k['hidroyear']=''
    for ind, col in df_k.iterrows():
      df_k.loc[ind, 'hidroyear'] = agnohidrologico(ind)
      
    df_RCS = df_k.copy()
  
    # RCS
    df_RCS['id'] = ''
    df_RCS.loc[df_RCS[df_RCS['hidroyear'].isin(dry_years)].index,'id'] = '#RCSd          #'
    df_RCS.loc[df_RCS[df_RCS['hidroyear'].isin(wet_years)].index,'id'] = '#RCSw          #'
    df_RCS.loc[df_RCS[df_RCS['hidroyear'].isin([1999])].index,'id'] = '#RCSw          #'
    
    # DDS
    df_DDS = df_RCS.copy()
    df_DDS['id'].replace('#RCS','#DDS', regex=True, inplace = True)  
    
    # DDG
    df_DDG = df_RCS.copy()
    df_DDG['id'].replace('#RCS','#DDG', regex=True, inplace = True)      
    
    # RCG
    df_RCG = df_RCS.copy()
    df_RCG['id'].replace('#RCS','#RCG', regex=True, inplace = True)    
        
    df_RCR = df_k.loc[df_k.index.year >= 2000].copy()
    df_RCR['id'] = ''
    df_RCR.loc[df_RCR[df_RCR['hidroyear'].isin(dry_years)].index,'id'] = '#RCRd          #'
    df_RCR.loc[df_RCR[df_RCR['hidroyear'].isin(wet_years)].index,'id'] = '#RCRw          #'
    df_RCR.loc[df_RCR[df_RCR['hidroyear'].isin([1999])].index,'id'] = '#RCRw          #'
    
    df_DDS = df_k.loc[df_k.index.year >= 2000].copy()
    df_DDS['id'] = ''
    df_DDS.loc[df_DDS[df_DDS['hidroyear'].isin(dry_years)].index,'id'] = '#DDSd          #'
    df_DDS.loc[df_DDS[df_DDS['hidroyear'].isin(wet_years)].index,'id'] = '#DDSw          #'
    df_DDS.loc[df_DDS[df_DDS['hidroyear'].isin([1999])].index,'id'] = '#DDSw          #'
    
    df_DDG = df_k.loc[df_k.index.year >= 2000].copy()
    df_DDG['id'] = ''
    df_DDG.loc[df_DDG[df_DDG['hidroyear'].isin(dry_years)].index,'id'] = '#DDGd          #'
    df_DDG.loc[df_DDG[df_DDG['hidroyear'].isin(wet_years)].index,'id'] = '#DDGw          #'
    df_DDG.loc[df_DDG[df_DDG['hidroyear'].isin([1999])].index,'id'] = '#DDGw          #'
    
    # fechas diarias
    dates = pd.date_range('2000-01-01','2021-05-29',freq = '1d')
    df_RCS = df_RCS.reindex(dates, method='ffill')
    df_RCR = df_RCR.reindex(dates, method='ffill')
    df_DDS = df_DDS.reindex(dates, method='ffill')
    df_DDG = df_DDG.reindex(dates, method='ffill')
    
    # flag de año seco y humedo
    flag_w = df_RCS.copy()
    flag_w['id'].replace('#RCSw          #','w', regex=True, inplace = True)  
    flag_w['id'].replace('#RCSd          #','d', regex=True, inplace = True)  
    
    # guardar flags
    idx = master.index.intersection(flag_w.index)
    master.loc[idx,'flag_wd'] = flag_w.loc[idx,'id'].values

    # crear el tpl
    master_tpl = master.copy()
    
    # guardar parametros
    # RCs
    cols_rcs = [x for x in master.columns if 'RC_S' in x]
    # RCr
    cols_rcp = [x for x in master.columns if 'RC_P' in x]
    
    dict_params = {'df_RCS' : cols_rcs, 'df_DDG' : ['DegDayGlacier'],
                   'df_RCG' : ['RC_g'], 'df_RCR' : cols_rcp }

    def create_tpl(df,df_param, cols_param):
        for indice, col in enumerate(cols_param):
            df_copy = df_param.copy()
            if col in ['DegDaySnow','DegDayGlacier','RC_g','DegDaySnow']:
                df_copy['id'].replace('d          #','d          #', 
                                      regex=True, inplace = True)
                df_copy['id'].replace('w          #','w          #',
                                       regex=True, inplace = True)
            else:
                df_copy['id'].replace('d          #',str(indice+1)+'d          #', regex=True, inplace = True)
                df_copy['id'].replace('w          #',str(indice+1)+'w          #', regex=True, inplace = True)
            df.loc[df_copy.index, col] = df_copy['id'].values
        return df
    
    for par in dict_params.keys():
        df_par = globals()[par]
        master_tpl = create_tpl(master_tpl, df_par, dict_params[par])

    # timelapse y factores de recesion X e Y
    idx = df_x.index.intersection(master.index)
    master_tpl['Tlapse'] = "#TL                #"
    master_tpl.loc[idx, 'Recess_X'] = df_x.loc[idx].values
    master_tpl.loc[idx, 'Recess_Y'] = df_y.loc[idx].values
    
    # extender los parámetros hasta 2021
    cols_param = [x for x in master_tpl.columns if ('DegDayGlacier' in x) | ('RC_S' in x) | ('RC_P' in x) | ('RC_g' in x) | ('Tlapse' in x) | ('Recess_X' in x) | ('Recess_Y' in x)]
    idx = master_tpl.loc[master_tpl.index >= '2021-01-01'].index
    idx2 = idx-datetime.timedelta(days = len(idx))
    
    # guardar los parámetros
    master_tpl.loc[idx, cols_param] = master_tpl.loc[idx2, cols_param].values
    
    # trim lineas del tpl
    cols_pp = [x for x in master_tpl.columns if 'Pp' in x]
    master_tpl[cols_pp] = master_tpl[cols_pp].round(8)
    
    return master_tpl

def parseMOP(df,path=r'G:\Downloads\TNsDO2Lu.xls'):
    qNew=pd.read_html(path)[0]
    colDate=qNew.columns[1]
    qNew.index=pd.to_datetime(qNew[colDate],dayfirst=True)
    return pd.DataFrame(qNew.iloc[:,-1].values,columns=df.columns,
                        index=qNew.index)

def main():
    
    #################################################
    #                    Master                     #
    #################################################
    global fecha
    fecha=pd.to_datetime('2023-02-17')
    root=r'G:\sequia\data'
    os.chdir(root)

    dictCuenca={'Combarbala_Ramadillas':['Rio Combarbala En Ramadillas',
'04532001-4'],'Cogoti_Embalse_Cogoti':['Rio Cogoti Entrada Embalse Cogoti',
'04531002-7'],'Grande_Cuyano':['Rio Grande En Cuyano','04513001-0'],
'Hurtado_San_Agustin':['Rio Hurtado En San Agustin','04501001-5'],
'Illapel_Las_Burras':['Rio Illapel En Las Burras','04721001-1'],
'Mostazal_Cuestecita':['Rio Mostazal En Cuestecita','04514001-6'],
'Choapa_Cuncumen':['Rio Choapa en Cuncumen','04703002-1'],
'Chalinga_Palmilla':['Rio Chalinga En La Palmilla','04712001-2'],
'Grande_Las_Ramadas':['Rio Grande En Las Ramadas','04511002-8'],
'Tascadero_Desembocadura': ['Rio Tascadero en Desembocadura','4512001-5'],
'Ponio': ['NA','NA'],
'Los_Molles': ['NA','NA'],
'La_Higuera': ['NA','NA'],
'Pama_Valle_Hermoso': ['Río Pama en Valle Hermoso','04533002-8']}
    
    # seleccionar la subcuenca
    cuenca=list(dictCuenca.keys())[7]

    # processGlaciers
    pathBands=os.path.join(root,cuenca,'bands.shp')
    pathGlaciers=r'G:\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\SIG\IPG2022\IPG2022.shp'
    if os.path.exists(pathGlaciers):    
        glacierBands=interSectGlaciers(pathBands,pathGlaciers)
        glacierBands.to_file(os.path.join(root,cuenca,'glacierBands.shp'))
    
    # leer master
    master=pd.read_csv(os.path.join(root,cuenca,
                                'Master.csv'),index_col=0,parse_dates = True)

    # leer areas
    dfArea=pd.read_csv(os.path.join(root,cuenca,'bands_mean_area.csv'))

    # extender master
    masterExt=extendMaster(master)
    
    # agregar los caudales al master y generar los INS de caudal
    masterExtQ=addCaudales(root,cuenca,masterExt,dictCuenca)
    
    # generar los archivos del SWE
    swe(cuenca,root,dictCuenca)

    # agregar las precipitaciones al master
    masterExtQP=addPp(root,cuenca,masterExtQ)

    # agregar las temperaturas
    masterExtQPT=addT(root,cuenca,masterExtQP)
    
    # agregar los dias de verano
    masterExtQPT=addSummerDays(masterExtQPT)
    
    # agregar la nieve
    masterExtQPTS=addSnowGlacier(root,cuenca,masterExtQPT)

    # compeltar el master
    masterExtQPTSvf=completarMaster(root,cuenca,masterExtQPTS,dfArea)

    # crear el .tpl
    dfTpl=createTPL(root,cuenca,masterExtQPTSvf)

    # guardar el master
    exportMaster(dfTpl,root,cuenca)

    def writeTPL(root,cuenca):
        path=os.path.join(root,cuenca,'Master.tpl')
        f = open(path,'r+')
        lines = f.readlines() # read old content
        f.seek(0) # go back to the beginning of the file
        f.write('ptf #'+'\n') # write new content at the beginning
        for line in lines: # write old content after new
            f.write(line)
        f.close()

    def writeIF(root,cuenca):
        path1=os.path.join(root,cuenca,'q.ins')
        path2=os.path.join(root,cuenca,'swe.ins')

        for file in [path1,path2]:
            f = open(file,'r+')
            lines = f.readlines() # read old content
            f.seek(0) # go back to the beginning of the file
            f.write('pif #'+'\n') # write new content at the beginning
            for line in lines: # write old content after new
                f.write(line)
            f.close()

    writeTPL(root,cuenca)
    writeIF(root,cuenca)    

if __name__ == '__main__':
    main()
