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
    folders=sorted(folders)

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
    
    return dfAll.dropna(axis=0)

def commitChanges():
    import csv
    import psycopg2
    import datetime
    import os

    # Conexión a postgresql
    conn = psycopg2.connect(database="geonode", user="geonode",
                             password="geonode", host="192.10.10.140",
                               port="5432")

    # Crea un cursor para la conexión
    cur = conn.cursor()

    #Elimina los datos existentes
    cur.execute("TRUNCATE TABLE sequia_bi.datos")

    # Lee el archivo CSV e inserta los datos en la base de datos
    filePath=os.path.join('..','data','StreamflowAll.csv')

    with open(filePath, 'r') as file:
    
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            date = row[0]
            Chalinga_Palmilla = row[1]
            Choapa_Cuncumen = row[2]
            Cogoti_Embalse_Cogoti = row[3]
            Combarbala_Ramadillas = row[4]
            Grande_Las_Ramadas = row[5]
            Hurtado_San_Agustin = row[6]
            Illapel_Las_Burras = row[7]
            Mostazal_Cuestecita = row[8]
            Tascadero_Desembocadura = row[9]
            date_actual = datetime.date.today()

            # Genero insert a la tabla
            sql = "INSERT INTO sequia_bi.datos(date, chalinga_palmilla, choapa_cuncumen, cogoti_embalse_cogoti, combarbala_ramadillas, grande_las_ramadas, hurtado_san_agustin, illapel_las_burras, mostazal_cuestecita, tascadero_desembocadura, date_insert) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            values = (date, Chalinga_Palmilla, Choapa_Cuncumen, 
                      Cogoti_Embalse_Cogoti, Combarbala_Ramadillas, 
                      Grande_Las_Ramadas,Hurtado_San_Agustin,Illapel_Las_Burras,
                        Mostazal_Cuestecita,Tascadero_Desembocadura,date_actual)

            # Ejecuta la consulta SQL
            cur.execute(sql, values)

    conn.commit()
    cur.close()
    conn.close()

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
                'Combarbala_Ramadillas':'Rio_Combarbala_en_Ramadillas',
                'Grande_Las_Ramadas':'Rio_Grande_en_Las_Ramadas'}

    # forecast actualizado
    for name in names:
        try:
            run_pySRM(name,dictNames[name])
        except Exception as e:
            print('Error '+str(e)+' en '+name)

    # compilar todo en una sola tabla
    try:
        dfOut=dfsToDf(os.path.join('..','data'))
        dfOut.to_csv(os.path.join('..','data','StreamflowAll.csv'))
    except:
        print('archivo no encontrado')
        
    # realizar commit a la BBDD de geonode
    try:
        commitChanges()
        print('Commit realizado exitosamente')
    except:
        print('Commit no realizado')

if __name__=='__main__':
    main()

