import os
import terraClimate
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
    file=[x for x in file if 'basin' not in x][0]
    gdf=gpd.read_file(os.path.join(root,file))
    gdf['area']=gdf.area
    gdf=gdf.sort_values(by='area',ascending=False)
    gdf=gpd.GeoDataFrame(pd.DataFrame(gdf.iloc[0]).T)
    gdf.set_crs(epsg='32719',inplace=True)
    gdf.to_crs(epsg='4326',inplace=True)
    del gdf['DN']
    gdf.to_file(os.path.join(root,'basin4callypso.shp'))
    return gdf

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


    root=r'G:\sequia\data'
    gdf=loadGdf()
    gdfToFolder(gdf,root)

    for subcuenca in lista:
        gdf=gdfUTM24326(subcuenca)
        path=os.path.join('..','data',subcuenca)
        folder = os.path.abspath(path)
        terraClimate.main(folder)

# def 