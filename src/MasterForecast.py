import pyCSRM
import GEEdl
import addClimate
import create_master_SRM
import forecast_arima

def run_pySRM(name, tipo = 'P'):
    path=os.path.join('..',name)
    folder = os.path.abspath(path)

    # bajar la pp, t y nieve
    GEEdl.getDatesDatasets(path)

    # agregar la pp, t y nieve al master
    # hacer el pronóstico de pp y t
    dataSet=addClimate.dataset(name)
    dataSet.fillPp()
    dataSet.fillTemp()

    # forecast de nieve
    create_master_SRM.SRM_master(folder)

    # correr el modelo
    pyCSRM.DEVELOP_SRM('..',name)

    print('Simulacion finalizada exitosamente')

def main():
    name='Hurtado_San_Agustin'
    run_pySRM(name)

if __name__=='__main__':
    main()