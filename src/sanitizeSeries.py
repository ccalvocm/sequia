import pandas as pd

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

def main():
    root=r'G:\sequia\data\Chalinga_Palmilla'

    master=loadMaster(root)
    pp=loadPp(root)
    t=loadT(root)
    snow=loadSnow(root)
    glacier=loadGlacier(root)

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

