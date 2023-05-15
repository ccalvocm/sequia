import ee
import os
import pandas as pd
import datetime
import geopandas as gpd
import json
import numpy as np

service_account = 'srmearthenginelogin@srmlogin.iam.gserviceaccount.com'
folder_json = os.path.join('.','auth',
                           'srmlogin-175106b08655.json')
credentials = ee.ServiceAccountCredentials(service_account, folder_json)
ee.Initialize(credentials)

class dsetEE(object):
    def __init__(self,product):
        self.product=product

    def getDate(self):
        collection = ee.ImageCollection(self.product)
        date_range = collection.reduceColumns(ee.Reducer.minMax(),
                                            ['system:time_start'])
        jsondate1 = ee.Date(date_range.get('min'))
        jsondate2 = ee.Date(date_range.get('max'))
        return (jsondate1,jsondate2)

class polyEE(dsetEE):
    def __init__(self,name,gdf,product,band,idate,fdate):
        self.name=name
        self.gdf=gdf.to_crs(epsg=4326)
        self.product=product
        self.ee_fc=None
        self.band=band
        self.idate=idate
        self.fdate=fdate
        self.scale=None

    def copy(self):
        import copy
        return copy.deepcopy(self)

    def addDate(self,image):
        img_date = ee.Date(image.date())
        img_date = ee.Number.parse(img_date.format('YYYYMMdd'))
        return image.addBands(ee.Image(img_date).rename('date').toInt())
    
    def fixMultipoly(self,geo):
        if 'MultiPolygon' in geo.geometry.iloc[0].geom_type:
            geo2=geo.explode()
            geo3=gpd.GeoDataFrame([],geometry=geo2.geometry,crs='4326')
            geo3['area']=''
            geo3['area']=geo3.to_crs(epsg='32719').apply(lambda x: x['geometry'].area,
                                                         axis=1)
            geo3=geo3.sort_values('area',ascending=False)
            geo3=geo3.iloc[0].drop('area')
            geo3=gpd.GeoDataFrame(pd.DataFrame(geo3).T)
            return geo3.buffer(0)
        else:
            return geo.buffer(0)
        
    def gdf2FeatureCollection(self,gs):
        features = []
        for i in range(gs.shape[0]):
            geom = gs.iloc[i:i+1,:] 
            geom=self.fixMultipoly(geom)
            jsonDict = json.loads(geom.to_json())
            x=np.array([x[0] for x in jsonDict['features'][0]['geometry']['coordinates'][0]])
            y=np.array([x[1] for x in jsonDict['features'][0]['geometry']['coordinates'][0]])
            cords = np.dstack((x[:],y[:])).tolist()
            # g=ee.Geometry.Polygon(cords).bounds()
            g=ee.Geometry.Polygon(cords)
            # feature = ee.Feature(g,{'name':self.gdf.loc[i,col].astype(str)})
            feature = ee.Feature(g)
            features.append(feature)
        return ee.FeatureCollection(features)

    def rasterExtraction(self,image):
        feature = image.sampleRegions(**{'collection':self.ee_fc,
                                         'scale':self.scale})
        return feature
    
    def rasterExtracion2(self,image):
        mean = image.reduceRegion(reducer=ee.Reducer.mean(),
            geometry=self.ee_fc.geometry(),
            scale=self.scale,
            bestEffort=True)
        return image.set('date', image.date().format()).set(mean)
    
    def filterQA(self,image):
        qa = image.select('NDSI_Snow_Cover_Basic_QA')
        goodQA = qa.eq(0)
        return image.updateMask(goodQA)
    
    def filterCouds(self,image):
        clouds=image.select('Snow_Albedo_Daily_Tile_Class')
        noClouds = clouds.neq(150)
        return image.updateMask(noClouds)

    def filterMax(self,image):
        filterMask=image.lt(100)
        return image.updateMask(filterMask)
    
    def geoSeries2GeoDataFrame(self,gs):
        temp=gpd.GeoDataFrame(pd.DataFrame(gs).T)
        if 'geometry' in temp.columns:
            return temp
        else:
            return gpd.GeoDataFrame(pd.DataFrame(gs)) 

    def num2str(self,gs):
        col=[x for x in gs.columns if x!='geometry'][0]
        gs[col]=gs[col].astype(str)
        return gs	

    def partitionDates(self):
        datei=self.idate
        datef=self.fdate
        months=round(((datef - datei)/np.timedelta64(1, 'M')))+1
        return list(pd.date_range(start=datei,end=datef,periods=months))

    def ImagesToDataFrame(self,images,band):
        column_df=['date',band]
        nested_list = images.reduceColumns(ee.Reducer.toList(len(column_df)),
                                            column_df).values().get(0)
        data = nested_list.getInfo()
        df = pd.DataFrame(data, columns=column_df)
        df.index=pd.to_datetime(df['date'])
        df=df[[x for x in df.columns if x!='date']]
        return df
    
    def QA(self,df_,dfQA_):
        df_=df_[:]
        dfQA_=dfQA_.loc[df_.index]
        treshold=dfQA_.loc[df_.index].quantile(.75)[0]
        df_=df_[dfQA_[dfQA_.columns[0]]<=treshold*0]
        return df_
    
    def spatialFill(self,image):
        #function to fill spatial gaps in an image using the nearest pixels in
        #google earth engine
        temp=ee.Image().clip(self.ee_fc.geometry())
        unmasked=image.unmask(temp)
        filled=image.focalMean(self.scale,'square','meters', 1)
        join=filled.copyProperties(image, ['system:time_start'])
        return join
    
    def countImage(self,image):
        count=image.reduceRegion(ee.Reducer.count(),self.ee_fc.geometry(),
                                  self.scale,bestEffort=True)
        return image.set('date', image.date().format()).set(count)

    def dl(self):
        listPeriods=self.partitionDates()

        idx=pd.date_range(listPeriods[0],listPeriods[-1])
        dfRet=pd.DataFrame(index=idx,columns=list(self.gdf.index))
        dfRetC=pd.DataFrame(index=idx,columns=list(self.gdf.index))
        dset=ee.ImageCollection(self.product)
        # set scale
        self.scale=dset.select(self.band).first().projection().nominalScale().getInfo()
               
        for ind,date in enumerate(listPeriods[:-1]):

            dl=False
            while ~dl:
            
                try:
                    lista=[]
                    listaCount=[]
                    for index in self.gdf.index:
                        # preprocesar los FeatureCollection
                        gdfTemp=self.geoSeries2GeoDataFrame(self.gdf.loc[index])
                        gdfTemp=self.num2str(gdfTemp).set_crs(epsg='4326')
                        self.ee_fc=self.gdf2FeatureCollection(gdfTemp)
                        
                        if '061' in self.product:
                            # minimo de imagenes
                            resCount=dset.filterBounds(self.ee_fc.geometry()).select(self.band)
                            resDatesC=resCount.filterDate(ee.Date(date),
                            ee.Date(listPeriods[ind+1])).map(self.countImage)
                            dfCount=self.ImagesToDataFrame(resDatesC,self.band)
                            listaCount.append(dfCount)

                            # NDSI to snow cover
                            res=dset.filterBounds(self.ee_fc.geometry())
                            resDates=res.filterDate(ee.Date(date),
        ee.Date(listPeriods[ind+1])).map(self.calcNDSI).map(self.calcSnow).select('NDSI')\
            .map(self.rasterExtracion2)
                            df=self.ImagesToDataFrame(resDates,'NDSI')
                            lista.append(df)

                        else:
                            res=dset.filterBounds(self.ee_fc.geometry()).select(self.band)
                            resDates=res.filterDate(ee.Date(date),
                            ee.Date(listPeriods[ind+1])).map(self.rasterExtracion2)
                            df=self.ImagesToDataFrame(resDates,self.band)
                            lista.append(df)
                    dl = True
                    break
                except:
                    dl = False

            lista2=self.fixColumns(lista)
            lista3=self.fixColumns(listaCount)

            dfDate=pd.concat(lista2, axis=1, ignore_index=False)
            dfRet.loc[dfDate.index,:]=dfDate.values

            if len(lista3):
                dfDateCount=pd.concat(lista3, axis=1, ignore_index=False)
                dfRetC.loc[dfDateCount.index,:]=dfDateCount.values
                
        # filtrar las imagenes sin datos
        if '061' in self.product:
            dfRet=self.filterCount(dfRet,dfRetC.astype(float))

        # completar las columnas que no extrajo GEE
        dfRet=self.autocompleteCol(dfRet)

        # completar las filas que no descargó GEE
        dfRet=self.fillColumns(dfRet)

        return dfRet
    
    def fixColumns(self,lista):
        lista2=[]
        for ind,df in enumerate(lista):
            df.columns=[str(ind)]
            df = df.loc[~df.index.duplicated(keep='first')]
            lista2.append(df)
        return lista2

    def autocompleteCol(self,df):
        colsNotna=df.dropna(how='all',axis=1).columns
        colsNa=[x for x in df.columns if x not in colsNotna]
        dfOut=df[:]
        if (len(colsNa)>0) & (len(colsNa)<len(df.columns)):
            for col in colsNa:
                if col<colsNotna.min():
                    dfOut[col]=df[colsNotna[colsNotna>col].min()]
                else:
                    dfOut[col]=df[colsNotna[colsNotna<col].max()]
        return dfOut
    
    def fillColumns(self,df):
        df=df.fillna(method='ffill').fillna(method='bfill')
        return df

    def filterCount(self,df1,df2):
        mask=df2>df2.astype(float).describe().loc['mean']-1
        return df1[mask]

    def calcNDSI(self,img):
        ndsi = img.normalizedDifference(['sur_refl_b04',
                            'sur_refl_b06']).rename('NDSI')
        return img.addBands(ndsi)
    
    def calcSnow(self,img):
        maskNDSI =img.select('NDSI').gte(0.4)
        mask=img.updateMask(maskNDSI).unmask(0)
        return mask

def main(name='Hurtado_San_Agustin'):

    def getLastDate(name):
        pathMaster=os.path.join('..',name,'Master.csv')
        pathPp=os.path.join('..',name,'Precipitacion','precipitacion_actual.csv')
        pathT=os.path.join('..',name,'Temperatura','temperatura_actual.csv')
        master=pd.read_csv(pathMaster,index_col=0,parse_dates=True)
        pp=pd.read_csv(pathPp,index_col=0,parse_dates=True)
        t=pd.read_csv(pathT,index_col=0,parse_dates=True)
        lastDate=master[[x for x in master.columns if 'Pp_z']].dropna(how='all').index[-1]
        return min(lastDate,pp.dropna().index[-1],t.dropna().index[-1])

    def getMinDate():
        dsets={'ECMWF/ERA5_LAND/DAILY_RAW':['total_precipitation_sum',
        'temperature_2m'],'MODIS/061/MOD09GA':['sur_refl_b04'],
        'MODIS/061/MYD09GA':['sur_refl_b04']
        }
        mindate=pd.to_datetime(datetime.date.today())

        for data in list(dsets.keys()):
            dataset=dsetEE(data)
            mindate=min(pd.to_datetime(dataset.getDate()[1].format('YYYY-MM-dd').getInfo()),
            mindate)
        return mindate,dsets

    def loadGdf(name,shpStr):
        gdfRet=gpd.read_file(os.path.join('..','data',name,shpStr+'.shp'))
        gdfRet.set_index(gdfRet.columns[0],drop=False,inplace=True)
        return gdfRet

    def postProcess(polygon,df,fillValue=0):
        # poblar el df
        dfAll=pd.DataFrame(np.nan,index=pd.date_range(df.index.min(),
                                df.index.max(),freq='D'),
                                columns=df.columns)
        dfAll.loc[df.index,df.columns]=df.values

        dfAll=polygon.fillColumns(df)
        dfOut=dfAll.fillna(fillValue)
        dfOut=dfOut*1.21
        dfOut=dfOut.applymap(lambda x: min(x,1))
        dfOut=dfOut.applymap(lambda x: max(x,0))
        return dfOut

    def getDatesDatasets(name='Hurtado_San_Agustin'):
        lastDate=getLastDate(name)
        print(lastDate)

        mindate,dsets=getMinDate()

        # crear df de nieve
        dfTerra=pd.DataFrame()
        if mindate>lastDate:
            gdfCuenca=loadGdf(name,'bands')
            for data in list(dsets.keys()):
                for band in dsets[data]:
                    if 'precipitation' in band:
                        pathOut=os.path.join('..',name,'Precipitacion',
                        'PrecipitacionActualizada.csv')
                        polygon=polyEE(name,gdfCuenca,data,band,idate=lastDate,
                    fdate=mindate+pd.DateOffset(1))
                        df=polygon.dl().iloc[:-1,:]
                        df=polygon.fillColumns(df)
                        df.to_csv(pathOut)

                    if 'temperature' in band:
                        pathOut=os.path.join('..',name,'Temperatura',
                        'TemperaturaActualizada.csv')
                        polygon=polyEE(name,gdfCuenca,data,band,idate=lastDate,
                    fdate=mindate+pd.DateOffset(1))
                        df=polygon.dl().iloc[:-1,:]
                        df=polygon.fillColumns(df)
                        df.to_csv(pathOut)

                    if data.find('MOD09GA')>0:
                        polygon=polyEE(name,gdfCuenca,data,band,idate=lastDate,
                    fdate=mindate)
                        df=polygon.dl()
                        dfTerra=df[:]
                        continue
                        
                    if data.find('MYD09GA')>0:
                        polygon=polyEE(name,gdfCuenca,data,band,idate=lastDate,
                    fdate=mindate)
                        dfAqua=polygon.dl()
                        # resultados
                        dfOut=dfTerra.combine_first(dfAqua)
                        # postprocesar
                        dfOut=postProcess(polygon,dfOut)
                        dfOut.to_csv(os.path.join('..',name,'Nieve',
                                                'snowCoverActualizada.csv'))
            
            # ahora bajar cobertura de glaciares
            gdfCuenca=loadGdf(name,'glacierBands')
            for data in list(dsets.keys()):
                if data.find('MOD09GA')>0:
                    polygon=polyEE(name,gdfCuenca,data,dsets[data][0],
                                       idate=lastDate,fdate=mindate)
                    dfTerra=polygon.dl()
                elif data.find('MYD09GA')>0:
                    polygon=polyEE(name,gdfCuenca,data,dsets[data][0],
                                       idate=lastDate,fdate=mindate)
                    dfAqua=polygon.dl()
                    dfOut=dfTerra.combine_first(dfAqua)
                    # postprocesar
                    dfOut=postProcess(polygon,dfOut,fillValue=1.)
                    dfOut.to_csv(os.path.join('..',name,'Nieve',
                    'glacierCoverActualizada.csv' ))
            print('actualizacion de datasets finalizada')                    
        return None

    getDatesDatasets(name)

if __name__=='__main__':
     main()
