import os
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from sklearn.ensemble import IsolationForest
import numpy as np

def filterML(df,cont=1e-20):

    #% remover outliers con ML
    print(cont)
    iso = IsolationForest(contamination=cont)
    outliers = df.copy()
    outliers.dropna(inplace = True)
    yhat = iso.fit_predict(outliers.values.reshape(-1, 1))
    
    # select all rows that are outliers
    mask = yhat != 1
    outliers = outliers[mask]
    
    idx_outliers = outliers.index
   
    q_day_fildered = df.copy()
    q_day_fildered.loc[idx_outliers] = np.nan
    return q_day_fildered

def loadBasin(root,sb):
    basin=gpd.read_file(os.path.join(root,sb,'basin4callypso.shp'))
    basin.set_crs(epsg='4326',inplace=True,allow_override=True)
    basin.to_crs(epsg='32719',inplace=True)
    return basin.area.values[0]

def DaytoMonth(df):
    subCuencas=['ChalingaPalmilla','CombarbalaEnRamadillas',
                'CogotiEntradaEmbalseCogoti','IllapelEnLasBurras',
                'ChoapaCuncumen','HurtadoSanAgustin']
    subCuencas=subCuencas+['Ponio','Pama_Valle_Hermoso','Los_Molles','La_Higuera',
                'El_Ingenio']
    root=r'G:\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Modelos\insumosWEAP'
    for sb in subCuencas:
        files=os.listdir(os.path.join(root,sb))
        file=[x for x in files if 'mon' in x][0]
        # df=pd.read_csv(os.path.join(root,sb,file),index_col=0,parse_dates=True)
        dfMon=pd.read_csv(os.path.join(root,sb,file),index_col=0,parse_dates=True)

        # dfMon=df.loc[(df.index>='2000-01-01') & (df.index<='2022-03-31')].resample('MS').mean()
        # dfMon.to_csv(os.path.join(root,sb,'Qmon_'+sb+'.csv'))
        # df.plot()
        dfMon.plot()

def main():
    root=r'G:\sequia\data'
    subBasin=['Ponio','La_Higuera','Los_Molles','Pama_Valle_Hermoso',
              'El_Ingenio']
    subBasin=['CL24','CL23','CL15','CL14','CL13','CL12','CL08','CL07','CL06',
              'CL052','CL051','CL050','CL04','CL03','CL02']+['Rio_Tencadan',
            'Estero_Camisas','Estero_Canela','Rio_Cuncumen','Rio_Tencadan']
    subBasin=['CL05','AN10']
    subBasin=['AN-05']
    dataset='IDAHO_EPSCOR_TERRACLIMATE_ro.csv'
    for sb in subBasin:
        # leer Q en m3/mes
        Vmon=pd.read_csv(os.path.join(root,sb,dataset),index_col=0,
            parse_dates=True).dropna().astype(float)*loadBasin(root,sb)/1e3

        # Qmon=Qmon.divide(list(Qmon.index.daysinmonth),axis=0)

        # pasar a q m3/s
        Qmon=Vmon.dropna().div(list(Vmon.index.daysinmonth),axis=0)/86400

        # caudal medio mensual interpolado
        # indice
        idx=pd.date_range(Vmon.index[0],Vmon.index[-1],freq='D')
        qMonInter=Qmon.reindex(idx).interpolate('pad')
        Qday=Qmon.reindex(idx).interpolate('time')
        qSumInter=Qday.resample('MS').sum().reindex(idx).interpolate('pad')
        qm3s=Qday.mul(Qday.index.daysinmonth.values,axis=0).mul(qMonInter.values).div(qSumInter.values)
        qm3s[qm3s[qm3s.columns[0]].isna()]=0
        qm3sF=filterML(qm3s)
        qm3sF=qm3sF.fillna(qm3sF[qm3sF.columns].rolling(30,center=False,
                                                        min_periods=1).mean())
        qm3sF.index.name='fecha'
        qm3sF.columns=['q (m3/s)']
        qm3sF.resample('MS').mean().to_csv(os.path.join(root,sb,
                                                        'Qmon_'+sb+'.csv'))
        fig,ax=plt.subplots()
        qm3s.resample('MS').mean().plot(ax=ax,label='original')
        qm3sF.resample('MS').mean().plot(ax=ax,label='filtrado')
        plt.legend([sb])
        plt.show()

def parseDates():
    root=r'G:\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Modelos\insumosWEAP'
    folders=[ f.path for f in os.scandir(root) if f.is_dir() ]
    folders=['CL24','CL23','CL15','CL14','CL13','CL12','CL08','CL07','CL06',
              'CL052','CL051','CL050','CL04','CL03','CL02']+['Rio_Tencadan',
            'Estero_Camisas','Estero_Canela','Rio_Cuncumen','Rio_Tencadan']
    folders=['CL05','AN10']
    for folder in folders:
        files=os.listdir(os.path.join(root,folder))
        csvs=[x for x in files if ('.csv' in x) & ('20212022' not in x)]
        for csv in csvs:
            df=pd.read_csv(os.path.join(root,folder,csv),index_col=0,
                           parse_dates=True)
            df_2021_2022=df.loc[df.index<='2022-03-31']
            df_2021_2022.plot()
            df_2021_2022.to_csv(os.path.join(root,folder,csv.replace('.csv',
                                                           '20212022.csv')))
            
def dfUnion():
    root=r'G:\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Modelos\insumosWEAP'
    root=r'G:\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Modelos\insumosWEAP\LimariCL'
    folders=[ f.path for f in os.scandir(root) if f.is_dir() ]
    idx=pd.date_range('2000-01-01','2022-03-01',freq='MS')
    dfAll=pd.DataFrame(index=idx)
    for folder in folders:
        files=os.listdir(folder)
        csvs=[x for x in files if ('Qmon' in x) & ('20212022' in x)]
        csvs=[x for x in files if 'Qmon' in x]
        df=pd.read_csv(os.path.join(folder,csvs[0]),index_col=0,parse_dates=True)
        print(csvs)
        colName=subBasin=folder.split('\\')[-1]+' (m3/s)'
        dfAll[colName]=''
        dfAll[colName]=df.loc[dfAll.index].values
    dfAll.to_csv(os.path.join(root,'EntradasLimariCL.csv'))

    subBasinsLimari=['CogotiEntradaEmbalseCogoti','CombarbalaEnRamadillas',
                     'El_Ingenio','GrandeLasRamadas','HurtadoSanAgustin',
                     'La_Higuera','LosMollesOjosDeAgua','MostazalEnCuestecita',
                     'PamaValleHermoso','Ponio','TascaderoDesembocadura']
    subBasinsLimari=[x+' (m3/s)' for x in subBasinsLimari]
    dfLimari=dfAll[subBasinsLimari]
    dfChoapa=dfAll[[x for x in dfAll.columns if x not in subBasinsLimari]]
    dfLimari.to_csv(os.path.join(root,'EntradasLimari.csv'))
    dfChoapa.to_csv(os.path.join(root,'EntradasChoapa.csv'))

if __name__ == '__main__':
    main()