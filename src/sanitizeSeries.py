import pandas as pd
import os

def loadMaster(root):
    return pd.read_csv(os.path.join(root,'Master.csv'),index_col=0,
                       parse_dates=True)

def loadPp(root):
    return pd.read_csv(os.path.join(root,'Precipitacion',
'precipitacion_actual.csv'),index_col=0,parse_dates=True)

def loadT(root):
    return pd.read_csv(os.path.join(root,'Temperatura',
                                    'temperatura_actual.csv'),index_col=0,
                                    parse_dates=True)

def loadSnow(root):
    return pd.read_csv(os.path.join(root,'Nieve','snowCover.csv'),
                       index_col=0,parse_dates=True)

def loadGlacier(root):
    return pd.read_csv(os.path.join(root,'Nieve','glacierCover.csv'),
                       index_col=0,parse_dates=True)

def changeDatesDfs(df1,df2,df3,df4,df5):

    dt=pd.Timedelta(value=1,unit='D')

    date1=df1.index[-1]
    date2=df2.index[-1]
    date3=df3.index[-1]
    date4=df4.index[-1]
    date5=df5.index[-1]

    date1=date1-3*dt
    date2=date2-4*dt
    date3=date3-5*dt
    date4=date4-2*dt
    date5=date5-6*dt

    date1=pd.to_datetime('2023-02-26')
    date2=date1
    date3=date1
    date4=date1
    date5=date1

    return df1.loc[df1.index<=date1],df2.loc[df2.index<=date2],df3.loc[df3.index<=date3],df4.loc[df4.index<=date4],df5.loc[df5.index<=date5]

def saveDf(m,pp,t,snow,glacier,root):
    m.to_csv(os.path.join(root,'Master.csv'))
    pp.to_csv(os.path.join(root,'Precipitacion','precipitacion_actual.csv'))
    t.to_csv(os.path.join(root,'Temperatura','temperatura_actual.csv'))
    snow.to_csv(os.path.join(root,'Nieve','snowCover.csv'))
    glacier.to_csv(os.path.join(root,'Nieve','glacierCover.csv'))
    return None

def main():
    root=r'G:\sequia\data'

    subBasins=os.listdir(root)
    for sb in subBasins:
        if os.path.isdir(os.path.join(root,sb)):
            print(sb)

            path=os.path.join(root,sb)
            master=loadMaster(path)
            pp=loadPp(path)
            t=loadT(path)
            snow=loadSnow(path)
            glacier=loadGlacier(path)

            # alterar fechas
            masterA,ppA,tA,snowA,glacierA=changeDatesDfs(master,pp,t,snow,
                                                         glacier)

            # guardar fechas alteradas
            saveDf(masterA,ppA,tA,snowA,glacierA,path)

def otros():
    lastDate=master.index[-1]
    pp=pp.loc[pp.index<=lastDate]
    t=t.loc[t.index<=lastDate]
    snow=snow.loc[snow.index<=lastDate]
    glacier=glacier.loc[glacier.index<=lastDate]    

    pp.to_csv(os.path.join(root,'Precipitacion',
'precipitacion_actual.csv'))
    t.to_csv(os.path.join(root,'Temperatura','temperatura_actual.csv'))
    snow.to_csv(os.path.join(root,'Nieve','snowCover.csv'))
    glacier.to_csv(os.path.join(root,'Nieve','glacierCover.csv'))

