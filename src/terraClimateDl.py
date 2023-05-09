import os
import terraClimate

def download():
    lista=['Ponio','La_Higuera','Los_Molles','Pama_Valle_Hermoso','El_Ingenio']
    lista=['Estero_Canela','Estero_Camisas','Rio_Cuncumen','Rio_Tencadan']
    lista=['CL02','CL03','CL04','CL050','CL051','CL052','CL06','CL07',
           'CL08','CL12','CL13','CL14','CL15','CL23','CL24']
    for subcuenca in lista:
        path=os.path.join('..','data',subcuenca)
        folder = os.path.abspath(path)
        terraClimate.main(folder)

# def 