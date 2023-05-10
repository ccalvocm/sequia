import forecast_arima
import pandas as pd
import os

class dataset(object):
    import pandas as pd
    def __init__(self,path):
        self.path=path
    
    def autocompleteCol(self,df):
        colsNotna=df.dropna(how='all',axis=1).columns
        colsNa=[str(x) for x in df.columns if x not in colsNotna]
        dfOut=df[:]
        if (len(colsNa)>0) & (len(colsNa)<len(df.columns)):
            for col in colsNa:
                if str(col)<str(colsNotna.min()):
                    dfOut[col]=df[colsNotna[colsNotna>col].min()]
                else:
                    dfOut[col]=df[colsNotna[colsNotna<col].max()]
        
        colsNotna=df.dropna(axis=0).columns
        colsNa=[x for x in df.columns if x not in colsNotna]
        dfOut=df[:]
        if (len(colsNa)>0) & (len(colsNa)<len(df.columns)):
            for col in colsNa:
                if str(col)<str(colsNotna.min()):
                    dfOut[col]=df[colsNotna[colsNotna>col].min()]
                else:
                    dfOut[col]=df[colsNotna[colsNotna<col].max()]
        return dfOut

    def postProcessPp(self,path_df):
        df=pd.read_csv(path_df,index_col=0,parse_dates=True)
        df=df.applymap(lambda x: x if x>=0 else 0)
        df.to_csv(path_df)
        return None

    def fillPp(self):
        df2023=pd.read_csv(os.path.join('..','data',self.path,'Precipitacion',
                                    'PrecipitacionActualizada.csv'),
                                    index_col=0,parse_dates=True)
        dfActual=pd.read_csv(os.path.join('..','data',self.path,'Precipitacion',
                                    'precipitacion_actual.csv'),
                                    index_col=0,parse_dates=True)
        firstD=df2023.index[0]
        lastD=max(df2023.index[-1],dfActual.index[-1])
        df=pd.DataFrame(index=pd.date_range(dfActual.index[0],lastD,freq='D'),
    	columns=dfActual.columns)
        df.loc[dfActual.index,:]=dfActual.values
        df.loc[df2023.index,:]=df2023.values
        df=self.autocompleteCol(df)
        path_dataset=os.path.join('..','data',self.path,'Precipitacion',
                                    'precipitacion_actual.csv')
        df.to_csv(path_dataset)
        forecast_arima.forecast_dataframe_file(path_dataset)

        # post procesar las precipitaciones pronosticadas
        self.postProcessPp(path_dataset.replace('.csv',
        '_forecast.csv'))
        return None
    
    def resampleT(self,df):
        df=df.resample('D').mean()-273.15
        df=df.interpolate(method='linear')
        return df

    def fillTemp(self):
        df2023=pd.read_csv(os.path.join('..','data',self.path,'Temperatura',
                                    'TemperaturaActualizada.csv'),
                                    index_col=0,parse_dates=True)
        df2023=self.resampleT(df2023)
        dfActual=pd.read_csv(os.path.join('..','data',self.path,'Temperatura',
                                    'temperatura_actual.csv'),
                                    index_col=0,parse_dates=True)
        firstD=df2023.index[0]
        lastD=max(df2023.index[-1],dfActual.index[-1])
        df=pd.DataFrame(index=pd.date_range(dfActual.index[0],lastD,freq='D'),
    	columns=dfActual.columns)
        df.loc[dfActual.index,:]=dfActual.values
        df.loc[df2023.index,:]=df2023.values
        df=self.autocompleteCol(df)
        path_dataset=os.path.join('..','data',self.path,'Temperatura',
                                    'temperatura_actual.csv')
        df.to_csv(path_dataset)
        forecast_arima.forecast_dataframe_file(path_dataset)
        return None
    
    def completeDf(self,df2023,dfActual):
        firstD=df2023.index[0]
        lastD=max(df2023.index[-1],dfActual.index[-1])
        df=pd.DataFrame(index=pd.date_range(dfActual.index[0],lastD,freq='D'),
    	columns=dfActual.columns)
        df.loc[dfActual.index,:]=dfActual.values
        df.loc[df2023.index,:]=df2023.values
        return df

    def fillSnow(self):
        df2023=pd.read_csv(os.path.join('..','data',self.path,'Nieve',
                                    'snowCoverActualizada.csv'),
                                    index_col=0,parse_dates=True)
        dfActual=pd.read_csv(os.path.join('..','data',self.path,'Nieve',
                                    'snowCover.csv'),
                                    index_col=0,parse_dates=True)

        df=self.completeDf(df2023,dfActual)
        df=self.autocompleteCol(df)
        path_dataset=os.path.join('..','data',self.path,'Nieve',
                                    'snowCover.csv')
        df.to_csv(path_dataset)

        df2023=pd.read_csv(os.path.join('..','data',self.path,'Nieve',
                                    'glacierCoverActualizada.csv'),
                                    index_col=0,parse_dates=True)
        dfActual=pd.read_csv(os.path.join('..','data',self.path,'Nieve',
                                    'glacierCover.csv'),
                                    index_col=0,parse_dates=True)

        df=self.completeDf(df2023,dfActual)
        df=self.autocompleteCol(df)
        path_dataset=os.path.join('..','data',self.path,'Nieve',
                                    'glacierCover.csv')
        df.to_csv(path_dataset)
        return None

def main():
    name='Hurtado_San_Agustin'
    dataSet=dataset(name)
    dataSet.fillPp()
    dataSet.fillTemp()
    dataSet.fillSnow()
    
if __name__=='__main__':
    main()
