#%%

import pandas as pd
import os

def index():
    return pd.date_range('2000-05-01','2022-04-01',freq='MS')

def headflows(root):
    path=os.path.join(root,'hf.csv')
    df=pd.read_csv(path,index_col=0,encoding='ISO-8859-1')

    df.index=index()
    return df

def rio(root):
    path=os.path.join(root,'limari.csv')
    df=pd.read_csv(path,index_col=0)

    df.index=index()
    return df

def rioChoapa(root):
    path=os.path.join(root,'choapa.csv')
    df=pd.read_csv(path,index_col=0)
    df=pd.DataFrame(df.loc[df.index[:-1]])

    df.index=index()
    return df

def rioIllapel(root):
    path=os.path.join(root,'illapel.csv')
    df=pd.read_csv(path,index_col=0)

    df.index=index()
    return df

def runoffPp(root):
    path=os.path.join(root,'ROppCatchments.csv')
    df=pd.read_csv(path,index_col=0)

    df.index=index()
    return df

def recupGW(root):
    path=os.path.join(root,'TL.csv')
    df=pd.read_csv(path,index_col=0)
    df.index=index()

    df=filterGWChoapa(df)
    return df

def demandasChoapa(root):
    path=os.path.join(root,'demandas.csv')
    df=pd.read_csv(path,index_col=0)

    df.index=index()
    return df

def corrales(root):
    path=os.path.join(root,'CORRALES.csv')
    df=pd.read_csv(path,index_col=0)
    df=df.apply(lambda x: pd.to_numeric(x, errors='coerce'))
    df.index=index()
    df=df[[x for x in df.columns if x not in ['Sum']]]

    entradas=df[df.columns[(df < 0).any()]]
    entradas=entradas.applymap(lambda x: abs(x))
    salidas=df[df.columns[(df > 0).any()]]

    entradas=df[['Decrease in Storage for EMB_CORRALES']].applymap(lambda x: abs(x))
    salidas=df[['Increase in Storage for EMB_CORRALES']].applymap(lambda x: abs(x))
    
    entradas=df[df.columns[df.columns.str.contains('Outflow ')]].applymap(lambda x: abs(x))
    salidas=df[df.columns[df.columns.str.contains('Inflow ')]].applymap(lambda x: abs(x))

    return entradas.fillna(0),salidas.fillna(0)


def elbato(root):
    path=os.path.join(root,'ELBATO.csv')
    df=pd.read_csv(path,index_col=0)
    df=df.apply(lambda x: pd.to_numeric(x, errors='coerce'))
    df.index=index()
    df=df[[x for x in df.columns if x not in ['Sum']]]

    entradas=df[df.columns[(df < 0).any()]]
    entradas=entradas.applymap(lambda x: abs(x))
    salidas=df[df.columns[(df > 0).any()]]

    # entradas=df[['Decrease in Storage for Embalse El Bato']]
    # salidas=df[['Increase in Storage for Embalse El Bato']]
    
    entradas=df[df.columns[df.columns.str.contains('Outflow ')]].applymap(lambda x: abs(x))
    entradas=entradas[[x for x in entradas.columns if 'Net Evap' not in x]]
    salidas=df[df.columns[df.columns.str.contains('Inflow ')]].applymap(lambda x: abs(x))

    return entradas.fillna(0),salidas.fillna(0)

def GW(root):
    path=os.path.join(root,'GW.csv')
    df=pd.read_csv(path,index_col=0)
    df=df.apply(lambda x: pd.to_numeric(x, errors='coerce'))
    df.index=index()
    df=df[[x for x in df.columns if x not in ['Sum']]]

    entradas=df[df.columns[(df < 0).any()]]
    salidas=df[df.columns[(df > 0).any()]]
    # salidas['Overflow']=entradas['Overflow']
    # del entradas['Overflow']
    return entradas,salidas

def whilelistGWChoapa():
    whitelist=['Transmission Link from AC_']
    return whitelist

def filterGWChoapa(df):
    wl=whilelistGWChoapa()
    cols=df.columns[df.columns.str.contains('|'.join(wl))]
    return pd.DataFrame(df[cols])

def filterHFChoapa(df):
    wl=whitelistChoapa()
    cols=df.columns[df.columns.str.contains('|'.join(wl))]

    return pd.DataFrame(df[cols])

def filterHFIllapel(df):
    wl=['Estero','Rio','CL_']
    cols=df.columns[df.columns.str.contains('|'.join(wl))]

    return pd.DataFrame(df[cols])

def whitelistChoapa():
    whitelist=['AN_','CL_','EST.','RIO']
    return whitelist

def filterDda(df):
    wl=['ZR_','NOZR']
    cols=df.columns[df.columns.str.contains('|'.join(wl))]

    return pd.DataFrame(df[cols])

def filterAP(df):
    wl=['AP_']
    cols=df.columns[df.columns.str.contains('|'.join(wl))]

    return pd.DataFrame(df[cols])

def filterMIN(df):
    wl=['MIN_']
    cols=df.columns[df.columns.str.contains('|'.join(wl))]

    return pd.DataFrame(df[cols])

def filterIND(df):
    wl=['IND_']
    cols=df.columns[df.columns.str.contains('|'.join(wl))]

    return pd.DataFrame(df[cols])

def filterETacuiferos(df):
    wl=['ET_']
    cols=df.columns[df.columns.str.contains('|'.join(wl))]

    return pd.DataFrame(df[cols])

def filterEVAPcorrales(df):
    wl=['EMB_CORRALES_EVAP']
    cols=df.columns[df.columns.str.contains('|'.join(wl))]

    return pd.DataFrame(df[cols])

def ofertaCorrales(df,hf):
    outflow=-df[df.columns[df.columns.str.contains('Outflow')]].sum(axis=1)
    outflow=pd.DataFrame(outflow)
    inflow=hf[hf.columns[hf.columns.str.contains('AN_07')]]
    inflow=pd.DataFrame(inflow)

    return pd.DataFrame(outflow.values-inflow.values,index=outflow.index)

def infGW(root):
    path=os.path.join(root,'infGW.csv')
    df=pd.read_csv(path,index_col=0)
    df.index=index()

    df=df[[x for x in df.columns if 'to AC_' in x]]

    return df

def TL(root):
    path=os.path.join(root,'tl.csv')
    df=pd.read_csv(path,index_col=0)
    df.index=index()

    return df

def riegoTL(df):
    wl=['to ZR','to NOZR']

    cols=df.columns[df.columns.str.contains('|'.join(wl))]

    return pd.DataFrame(df[cols])

def returnRiver(root):
    path=os.path.join(root,'ril.csv')
    df=pd.read_csv(path,index_col=0)
    df.index=index()

    wl=['to RIO']

    cols=df.columns[df.columns.str.contains('|'.join(wl))]

    return pd.DataFrame(df[cols])

def returnAq(root):
    path=os.path.join(root,'ril.csv')
    df=pd.read_csv(path,index_col=0)
    df.index=index()

    wl=['to AC_']

    cols=df.columns[df.columns.str.contains('|'.join(wl))]

    return pd.DataFrame(df[cols])


    # balance
def suma(lista):
    dfRet=pd.DataFrame(0,index=index(),columns=['suma'])
    for df in lista:
        dfTemp=pd.DataFrame(df.sum(axis=1))
        dfTemp.columns=['suma']
        dfRet=dfRet+dfTemp

    return dfRet

def dfTocol(df):
    return df.sum(axis=1)
#%%
def main():

    #%%
    import matplotlib.pyplot as plt
    root=r'G:\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Resultados'
    root=os.path.join(root,'Mostazal')
    # entradas
    hfF=pd.DataFrame(headflows(root)).astype(float)
    hfF.index=index()

    def filterDf(df,lista):
        dfRet=df[[x for x in df.columns if any(map(x.__contains__,lista))]]
        return pd.DataFrame(dfRet)
    
    # headflows Rapel
    heads=['AN_03','CL_03']
    hfF=filterDf(hfF,heads)
    tl=TL(root)

    cols=[x for x in tl.columns if ('r_most' in x.lower()) & ('Withdrawal' in x)]
    riego=filterDf(tl,cols)

    # agua potable
    AP=tl[[x for x in tl.columns if 'AP_MOS01' in x]]

    GWin,GWout=GW(root)
    GWin=filterDf(GWin,['AC_MOS_01'])
    GWout=filterDf(GWout,['AC_MOS_01'])

    retornoRio=pd.read_csv(os.path.join(root,'ril.csv'),index_col=0)
    retornoRio.index=index()

    colsRIL=[x for x in retornoRio.columns if 'most' in str.lower(x)]
    retornoRio=retornoRio[colsRIL]

    qDesemb=pd.read_csv(os.path.join(root,'Mostazal.csv'),index_col=0)
    qDesemb.index=index()
    qDesemb=pd.DataFrame(qDesemb.iloc[:,-1])

    # caudal de entrada del Limari PROMMRA
    qAfluPROM=pd.read_csv(os.path.join(root,'mostazalDesembocadura.csv'),index_col=0)
    qAfluPROM.index=index()
    qAfluPROM=pd.DataFrame(qAfluPROM.iloc[:,-1])

    deficit=pd.read_csv(os.path.join(root,'unmetD.csv'),index_col=0)
    deficit.index=index()
    deficit=deficit[[x for x in deficit.columns if any(map(x.__contains__,
                                                    ['most']))]]
    
    #%%
    def fixMostazal(df):
        diff=df['qAfluPROM']-df['qDesemb']
        idx=diff[diff<0].index
        df.loc[idx,'GWin']=df.loc[idx,'GWin'].values-diff.loc[idx].values
        idx=diff[diff>0].index
        df.loc[idx,'GWout']=df.loc[idx,'GWout'].values-diff.loc[idx].values
        df['qDesemb']=df['qAfluPROM'].values
        del df['qAfluPROM']
        return df

# 1.06102358
    def plots(ineficienciaRiego=1.08):
        df=pd.DataFrame(index=index())
        df['headflows']=dfTocol(hfF)
        df['retornoRio']=dfTocol(retornoRio)
        df['qDesemb']=-dfTocol(qDesemb)
        df['qAfluPROM']=-dfTocol(qAfluPROM)
        df['GWin']=-dfTocol(GWin)
        df['GWout']=-dfTocol(GWout)
        df['riego']=-ineficienciaRiego*dfTocol(riego)
        df['AP']=-dfTocol(AP)
        df=fixMostazal(df)
        df['Balance']=df.sum(axis=1)
        df['Demanda insatisfecha']=dfTocol(deficit).values
        df=df.apply(lambda x: x*df.index.daysinmonth.values)
        df=df.multiply(86400/1e6)

        # df=df.loc[(df.index>='2015-04-01') & (df.index<='2016-03-01')]
        df=df.loc[(df.index>='2020-04-01') & (df.index<='2021-03-01')]
        
        #plot balance Alternativa X
        df.columns=['Agua superficial','Retornos de agua',
                    'Río Mostazal en Desembocadura',
                    'Entrada agua subterránea',
                    'Salida agua subterránea',
                    'Riego', 'Agua potable',
                    'Balance','Demanda insatisfecha']
        df.columns=[x+' (Hm3/mes)' for x in df.columns]
        res=abs(df.sum(axis=1)).sum()
        plt.close('all')
        fig, axes = plt.subplots(figsize = (17,11))
        sumas=pd.DataFrame(df.sum(axis=1))
        df.index=sumas.index
        df.plot(stacked=True, kind = 'bar', grid=True, ax = axes)

        sumas.plot(ax=axes, kind = 'line', grid=True, color = 'black',
                label = 'Total', marker = 'o', linewidth = 4, markersize = 10)
        pos = axes.get_position()
        # axes.set_position([pos.x0, pos.y0 * 4.5, pos.width * 1.0, pos.height * 0.5])
        # axes.legend(loc='center right', bbox_to_anchor=(1.0, -0.65), ncol=2)
        
        # axes.set_ylim([-8,8])
        axes.set_xlabel('Mes año hidrológico')
        axes.set_ylabel('Volumen ($Hm^3/mes$)')

        # get y-axis limits of the plot
        low, high = axes.get_ylim()
        # find the new limits
        bound = max(abs(low), abs(high))
        # set new limits
        axes.set_ylim(-bound, bound)
        # df.plot(stacked=True, kind = 'bar', grid=True, ax = axes[1], title = 'Desagregada')
        
        # df.plot(stacked=False, kind = 'line', grid=True, ax = axes[1], title = 'Desagregada', marker = 'o')
        # df.plot(stacked=False, kind = 'line', grid=True, ax = axes[1], title = 'Desagregada', marker = 'o',
        #         subplots=True, layout = (2,2))
        # nam = os.path.join(folder, cuenca.replace(' ','_') + 'v3.jpg')
        # axes[0].set_ylabel('Volumen ($Hm^3/mes$)')
        # axes[1].set_ylabel('Volumen ($Hm^3/mes$)')
        print(res)
        plt.suptitle('Balance Oferta-Demanda\n' + 'Choapa')
        return res

    import numpy as np
    import scipy.optimize as opt

    r = opt.root(plots, x0=1.1288985823336968, method='hybr')
    print(r)
    # array([1.97522498 3.47287981 5.1943792  2.10120135 4.09593969])

    print(r.x)
#%%
if __name__=='__main__':
    main()
