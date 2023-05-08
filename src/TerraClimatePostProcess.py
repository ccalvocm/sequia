import os
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt


def main():
    root=r'G:\sequia\data'
    subBasin=['Ponio','La_Higuera','Los_Molles','Pama_Valle_Hermoso',
              'El_Ingenio']
    dataset='IDAHO_EPSCOR_TERRACLIMATE_ro.csv'
    for sb in subBasin:
        Qmon=pd.read_csv(os.path.join(root,sb,dataset),index_col=0,
                         parse_dates=True).astype(float)/1e3/86400
        Qmon=Qmon.divide(list(Qmon.index.daysinmonth),axis=0)
        idx=pd.date_range(Qmon.index[0],Qmon.index[-1],freq='MS')
        Qday=Qmon.reindex(idx).resample('MS').mean().interpolate('time')
        basin=gpd.read_file(os.path.join(root,sb,'basin4callypso.shp'))
        basin.to_crs(epsg='32719',inplace=True)
        qm3s=Qday*basin.area.values[0]
        qm3s.index.name='fecha'
        qm3s.columns=['q (m3/s)']
        qm3s.to_csv(os.path.join(root,sb,'Q_'+sb+'.csv'))
        # qm3s.plot()
        # plt.legend([sb])
        # plt.show()

if __name__ == '__main__':
    main()