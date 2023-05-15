import os

def run_pySRM(name,subcuenca, tipo = 'P'):
    import pyCSRM
    import snowCover61
    import addClimate
    import create_master_SRM
    import forecast_arima

    path=os.path.join('..','data',name)
    folder = os.path.abspath(path)

    # bajar la pp, t y nieve
    snowCover61.main(name)

    # agregar la pp, t y nieve al master
    # hacer el pronóstico de pp y t
    dataSet=addClimate.dataset(name)
    dataSet.fillPp()
    dataSet.fillTemp()
    dataSet.fillSnow()

    # forecast de nieve
    print('Realizando forecast de nieve')
    create_master_SRM.SRM_master(folder)

    print('Iniciando simulacion')
    # correr el modelo
    pyCSRM.DEVELOP_SRM(os.path.join('..','data',name),subcuenca)

    print('Simulacion finalizada exitosamente')
    import pandas as pd
    df=pd.read_csv(os.path.join('..','data',name,'Qsim.csv'))
    print(df.tail())

def dfsToDf(root_sb):
    folders=os.listdir(root_sb)
    
    # get sub directories of root_sb
    folders=[f for f in folders if os.path.isdir(os.path.join(root_sb,f))]

    import pandas as pd
    dfAll=pd.DataFrame()

    for sb in folders:
        path=os.path.join(root_sb,sb)
        folder = os.path.abspath(path)
        df=pd.read_csv(os.path.join(folder,'Qsim.csv'),index_col=0,
                       parse_dates=True)
        df.columns=[sb]
        try:
            dfAll=pd.concat([dfAll,df],axis=1)
        except:
            break
    
    return dfAll

def main():
    pth=os.path.join('.','sequia','src')
    os.chdir(pth)

    names=['Hurtado_San_Agustin','Illapel_Las_Burras','Mostazal_Cuestecita',
'Tascadero_Desembocadura','Chalinga_Palmilla','Choapa_Cuncumen',
'Cogoti_Embalse_Cogoti','Combarbala_Ramadillas','Grande_Las_Ramadas']
    
    dictNames= {'Hurtado_San_Agustin':'Rio_Hurtado_en_San_Agustin',
                'Illapel_Las_Burras': 'Rio_Illapel_en_Las_Burras',
                'Mostazal_Cuestecita': 'Rio_Mostazal_en_Cuestecita',
                'Tascadero_Desembocadura': 'Rio_Tascadero_en_Desembocadura',
                'Chalinga_Palmilla':'Rio_Chalinga_en_la_Palmilla',
                'Choapa_Cuncumen': 'Rio_Choapa_en_Cuncumen',
                'Cogoti_Embalse_Cogoti':'Rio_Cogoti_Entrada_Embalse_Cogoti',
                'Combarbala_Ramadillas':'Rio_Cobmarbala_en_Ramadillas',
                'Grande_Las_Ramadas':'Rio_Grande_en_Las_Ramadas'}

    # forecast actualizado
    for name in names:
        try:
            run_pySRM(name,dictNames[name])
        except Exception as e:
            print('Error '+str(e)+' en '+name)

    # compilar todo en una sola tabla
    dfOut=dfsToDf(os.path.join('..','data'))
    dfOut.to_csv(os.path.join('..','data','StreamflowAll.csv'))

if __name__=='__main__':
    main()
