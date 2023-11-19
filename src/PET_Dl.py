import os
import PET_ERA5
import geopandas as gpd
import pandas as pd

def loadGdf():
    path=r'G:\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Modelos\insumosWEAP\rioPonio.shp'
    subcuencasCL=gpd.read_file(path)
    return subcuencasCL

def gdfToFolder(gdf,root):
    for idx in gdf.index:
        geoS=gpd.GeoDataFrame(pd.DataFrame(gdf.loc[idx]).T)
        pathDir=gdf.loc[idx,'COD_SUBC']
        try:
            os.mkdir(os.path.join(root,pathDir))
        except:
            'directorio ya existe'

        path=os.path.join('..','data',pathDir)
        folder = os.path.abspath(path)
        geoS.set_crs(crs='epsg:32719',inplace=True)
        geoS.to_file(os.path.join(folder,'basin4callypso.shp'))
        terraClimate.main(folder)

def gdfUTM24326(subcuenca):
    root=os.path.join('..','data',subcuenca)
    file=list(filter(lambda x: x.endswith('.shp'),os.listdir(root)))
    file=[x for x in file if 'basin' in x][0]
    gdf=gpd.read_file(os.path.join(root,file))
    gdf['area']=gdf.area
    gdf['area']=gdf['area'].astype(str)
    gdf.set_geometry(col='geometry', inplace=True) # optional
    gdf=gdf.sort_values(by='area',ascending=False)
    gdf=gpd.GeoDataFrame(pd.DataFrame(gdf.iloc[0]).T)
    gdf.set_crs(epsg='4326',inplace=True)
    gdf.to_crs(epsg='4326',inplace=True)
    cols=gdf.columns[~gdf.columns.str.contains('geometry')]
    gdf[cols]=gdf[cols].astype(str)
    gdf=gpd.GeoDataFrame(gdf,geometry=gdf.buffer(0.001))
    gdf.to_file(os.path.join(root,'basin4callypso.shp'))
    return gdf

def changeIndex(df):
    # change df format to m-Y month starting in 1
    # df.index = pd.to_datetime(df.index).strftime('%d-%m-%Y')
    cols=list(df.columns)
    df['Yr']=df.index.year
    df['Mon']=df.index.month
    dfRet=df[['Yr','Mon']+cols]
    return dfRet
    
def day2mon(lista):
    root=r'G:\sequia\data'
    for sb in lista:
        path=os.path.join(root,sb)
        folder = os.path.abspath(path)
        pp=pd.read_csv(os.path.join(folder,'Precipitacion',
        'PrecipitacionActualizada.csv'),index_col=0,parse_dates=True)
        t=pd.read_csv(os.path.join(folder,'Temperatura',
        'TemperaturaActualizada.csv'),index_col=0,parse_dates=True)
        ppmon=pp.resample('MS').sum()*1000
        ppmon=ppmon.astype(float)
        tmon=t.resample('MS').mean()-273.15
        tmon=tmon.astype(float)

        # change df index to dd-mm-yyyy format

        ppmon=changeIndex(ppmon)
        tmon=changeIndex(tmon)
        ppmon.to_csv(os.path.join(folder,'Precipitacion',
        'ppMon'+sb+'.csv'),index=None)
        tmon.to_csv(os.path.join(folder,'Temperatura',
        'tMon'+sb+'.csv'),index=None)
    return None

def download():
    lista=['Ponio','La_Higuera','Los_Molles','Pama_Valle_Hermoso','El_Ingenio']
    lista=['Estero_Canela','Estero_Camisas','Rio_Cuncumen','Rio_Tencadan']
    lista=['CL08','CL12','CL13','CL14','CL15','CL23','CL24']
    lista=['AN-05']
    lista=['EP']
    lista=['estChacay','estSanVicente','estTemuco','estElQuemado',
           'rioPalPal','rioRelbun']
    lista=['estMeco','estColton','estPitipiti','estEspinal','estGallipavo',
           'estPalPal']
    lista=['19','20','21','22','23','24']
    lista=['LarquiA','LarquiB','LarquiC','LarquiD','ChacayA',
'ChacayB','ChacayC','SanVicenteA','SanVicenteB','SanVicenteC',
'PalPalA','PalPalB','PalPalC','PalPalD']

    root=r'G:\sequia\data'
    # gdf=loadGdf()
    # gdfToFolder(gdf,root)

    for subcuenca in lista:
        gdf=gdfUTM24326(subcuenca)
        path=os.path.join('..','data',subcuenca)
        folder = os.path.abspath(path)
        PET_ERA5.main(subcuenca)

    day2mon(lista)
# def 