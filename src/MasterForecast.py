import os

def run_pySRM(name, tipo = 'P'):
    import pyCSRM
    import snowCover61
    import addClimate
    import create_master_SRM
    import forecast_arima

    path=os.path.join('..','data',name)
    folder = os.path.abspath(path)

    # bajar la pp, t y nieve
    snowCover61.main(folder)

    # agregar la pp, t y nieve al master
    # hacer el pronóstico de pp y t
    dataSet=addClimate.dataset(name)
    dataSet.fillPp()
    dataSet.fillTemp()
    dataSet.fillSnow()

    # forecast de nieve
    print('Realizando forecast hidrometeorologico')
    create_master_SRM.SRM_master(folder)

    print('Iniciando simulacion')
    # correr el modelo
    pyCSRM.DEVELOP_SRM(os.path.join('..','data',name),name)

    print('Simulacion finalizada exitosamente')
    import pandas as pd
    df=pd.read_csv(os.path.join('..','data',name,'Qsim.csv'))
    print(df.tail())

def main():
    pth=os.path.join('.','sequia','src')
    os.chdir(pth)

    name='Illapel_Las_Burras'
    run_pySRM(name)

if __name__=='__main__':
    main()
