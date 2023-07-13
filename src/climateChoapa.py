import os
import ee
import geopandas as gpd

service_account = 'srmearthenginelogin@srmlogin.iam.gserviceaccount.com'
folder_json = os.path.join('.','auth',
                           'srmlogin-175106b08655.json')
credentials = ee.ServiceAccountCredentials(service_account, folder_json)
ee.Initialize(credentials)

#Pandas modules to interact data
import numpy as np
import pandas as pd

#%%
# manage the date formating as per your requirements
# Mine is in format of YYYYMMdd
def addDate(image):
    img_date = ee.Date(image.date())
    img_date = ee.Number.parse(img_date.format('YYYYMMdd'))
    return image.addBands(ee.Image(img_date).rename('date').toInt())

def rasterExtraction(image):
    feature = image.sampleRegions(
        collection = ee_fc, # feature collection here
        scale = 4638.3 # Cell size of raster
    )
    return feature

def partitionDates():
    datei=pd.to_datetime('1989-01-01')
    datef=pd.to_datetime('2021-04-01')
    months=round((datef-datei)/np.timedelta64(5, 'M'))+1
    return list(pd.date_range(start=datei,end=datef,periods=months))

path_df=r'E:\CIREN\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\SIG\ZR_ANID_20230707_CHOAPA_CATCHMENT.shp'
path_df=r'E:\CIREN\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\SIG\ElBato_ANID.shp'

plot_df = gpd.read_file(path_df)

plot_df['longitude']=plot_df.geometry.x
plot_df['latitude']=plot_df.geometry.y

del plot_df['geometry']

features=[]
for index, row in plot_df.iterrows():
#     print(dict(row))
#     construct the geometry from dataframe
    poi_geometry = ee.Geometry.Point([row['longitude'], row['latitude']])
#     print(poi_geometry)
#     construct the attributes (properties) for each point 
    poi_properties = dict(row)
#     construct feature combining geometry and properties
    poi_feature = ee.Feature(poi_geometry, poi_properties)
#     print(poi_feature)
    features.append(poi_feature)

# final Feature collection assembly
ee_fc = ee.FeatureCollection(features) 
ee_fc.getInfo()

scale=4638.3
dataset='IDAHO_EPSCOR/TERRACLIMATE'
dataset='ECMWF/ERA5_LAND/MONTHLY_AGGR'
band='pr'
band='evaporation_from_open_water_surfaces_excluding_oceans_sum'

def dl():
    listPeriods=partitionDates()

    idx=pd.date_range(listPeriods[0],listPeriods[-1])
    dfRet=pd.DataFrame(index=idx,columns=list(plot_df['WEAP_CATCH']))
    dset=ee.ImageCollection(dataset)

    lista=[]

    for ind,date in enumerate(listPeriods[:-1]):

        res=dset.filterBounds(ee_fc).select(band).map(addDate)
        results=res.filterDate(ee.Date(date),
        ee.Date(listPeriods[ind+1])).map(rasterExtraction).flatten()
    
        # Order data column as per sample data
        # You can modify this for better optimization
        column_df = list(plot_df.columns)
        column_df.extend([band,'date'])

        nested_list = results.reduceColumns(ee.Reducer.toList(len(column_df)),
                                             column_df).values().get(0)
        data = nested_list.getInfo()
        df = pd.DataFrame(data, columns=column_df)
        df['date']=pd.to_datetime(df['date'],format="%Y%m%d")
        dfLista=df.pivot_table(index=df['date'],columns=df['WEAP_CATCH'],
                          values=band)
        lista.append(dfLista)

    dfDate=pd.concat(lista, axis=0, ignore_index=False)

    return dfDate

df=dl()
            
