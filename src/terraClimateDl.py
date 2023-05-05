import os
import terraClimate

def download():
    lista=['Ponio','La_Higuera','Los_Molles','Pama_Valle_Hermoso','El_Ingenio']
    for subcuenca in lista:
        path=os.path.join('..','data',subcuenca)
        folder = os.path.abspath(path)
        terraClimate.main(folder)

# def 