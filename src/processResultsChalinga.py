#%%

import pandas as pd
import os

def index():
    return pd.date_range('1989-04-01','2021-03-01',freq='MS')

def headflows(root):
    path=os.path.join(root,'hf.csv')
    df=pd.read_csv(path,index_col=0,encoding = "ISO-8859-1")

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

def rioChalinga(root):
    path=os.path.join(root,'chalinga.csv')
    df=pd.read_csv(path,index_col=0, encoding = "ISO-8859-1")

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
    entradas=entradas.applymap(lambda x: abs(x))
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
    df.columns=df.columns.str.replace('Río','Rio')
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
    df=pd.read_csv(path,index_col=0,encoding = "ISO-8859-1")
    df.index=index()
    df.columns=df.columns.str.replace('Río','RIO')

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
    
#%%
def main():

    #%%
    def plots(ineficienciaRiego=1.00092024):    
        import matplotlib.pyplot as plt
        root=r'G:\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Resultados\Choapa'
        root=r'G:\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Resultados\Illapel'
        root=r'G:\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Resultados\Chalinga'

        # entradas
        # q=rioChoapa(root)
        # q=rioIllapel(root)
        q=rioChalinga(root)
        runoffPp=pd.read_csv(r'G:\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Resultados\Chalinga\roCatch.csv',
                            index_col=0)
        runoffPp.index=index()
        # inEmbalse,outEmbalse=corrales(root)
        # inEmbalse=pd.DataFrame(inEmbalse[inEmbalse.columns[:-2]])
        # inEmbalse,outEmbalse=elbato(root)
        hf=headflows(root)
        # hfF=filterHFChoapa(hf)
        hfF=filterHFIllapel(hf)

        tl=TL(root)
        riego=riegoTL(tl)
        retornoRio=returnRiver(root)
        AP=tl[tl.columns[tl.columns.str.contains('to AP')]]*0
        MIN=tl[tl.columns[tl.columns.str.contains('to MIN')]]*0
        IND=tl[tl.columns[tl.columns.str.contains('to IND')]]*0
        qDesemb=pd.DataFrame(q.iloc[:,-1])

        GWin,GWout=GW(root)
        # GWin=pd.DataFrame(GWin['Overflow'])
        GWin=GWin[GWin.columns[GWin.columns.str.contains(' to ')]]
        # GWout=GWout[GWout.columns[GWout.columns.str.contains('Below')]]
        # GWin=GWin[GWin.columns[GWin.columns.str.contains('Outflow to REST_MPL')]]

        
        # GWin=GWin[[x for x in GWin.columns if 'Outflow' in x]]
        # GWout=GWout[[x for x in GWout.columns if 'Inflow' in x]]
        # GWout=GWout[[x for x in GWout.columns if 'Decrease' in x]]

        # dfGW=pd.read_csv(r'G:\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Resultados\Choapa\inout.csv',index_col=0) 
        # dfGW.index=index()
        # GWout=-dfGW[dfGW.columns[dfGW.columns.str.contains('Outflow to AC')]]
        # GWin=dfGW[dfGW.columns[dfGW.columns.str.contains('Surface Water Inflow')]].astype(float)

        rf=pd.read_csv(r'G:\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Resultados\Choapa\rf.csv',index_col=0)
        rf.index=index()
        rf=pd.DataFrame(rf[rf.columns[0]])
        GWout.index=index()
        entradas=suma([hfF,GWin,rf,retornoRio,riego])
        remanentesRiego=suma([GWout,suma([riego.multiply(-1)])])
        salidas=suma([AP,MIN,IND,qDesemb,GWout])
        balance=pd.DataFrame(entradas-salidas,index=hfF.index)
        plt.close('all')
        # fig,ax=plt.subplots(1)

        def dfTocol(df):
            return df.sum(axis=1)

        # pd.DataFrame(suma([hfF]).values,index=index(),columns=['hfF']).plot(ax=ax)
        # pd.DataFrame(suma([inEmbalse]).values,index=index(),columns=['inEmbalse']).plot(ax=ax)
        # pd.DataFrame(suma([retornoRio]).values,index=index(),columns=['retornoRio']).plot(ax=ax)
        # pd.DataFrame(suma([riego.multiply(-1)]).values,index=index(),columns=['riego']).plot(ax=ax)
        # pd.DataFrame(suma([GWout.multiply(-1)]).values,index=index(),columns=['GWout']).plot(ax=ax)
        # pd.DataFrame(suma([qDesemb.multiply(-1)]).values,index=index(),columns=['qDesemb']).plot(ax=ax)
        # pd.DataFrame(suma([outEmbalse.multiply(-1)]).values,index=index(),columns=['outEmbalse']).plot(ax=ax)
        # pd.DataFrame(suma([balance]).values,index=index(),columns=['balance']).plot(ax=ax)
        # # pd.DataFrame(suma([remanentesRiego.multiply(-1)]).values,index=index(),columns=['remanentesRiego']).plot(ax=ax)
        # # ax.set_xlim([pd.to_datetime('2015-01-01'),index()[-1]])
        # print(    balance.describe())

        df=pd.DataFrame(index=index())
        df['headflows']=dfTocol(hfF)
        df['GWin']=ineficienciaRiego*dfTocol(riego)
        df['qDesemb']=dfTocol(qDesemb.multiply(-1))

        df['GWout']=dfTocol(GWout.multiply(-1))+dfTocol(riego.multiply(1))
        df['riego']=dfTocol(riego.multiply(-1))
        df=df.apply(lambda x: x*df.index.daysinmonth.values)
        df=df.multiply(86400/1e6)
        df.columns=['Agua superficial','Entrada agua subterránea',
                    'Río Chalinga en Choapa',
                    'Salida agua subterránea',
                    'Riego']
        df['retornoRio']=dfTocol(retornoRio)
        df.columns=df.apply(lambda x: x.name + ' (Hm3/mes)')

        df=df.loc[(df.index>='2015-04-01') & (df.index<='2016-03-01')]
        df=df.loc[(df.index>='2020-04-01') & (df.index<='2021-03-01')]

        df.columns=['Agua superficial','Entrada agua subterránea','Río Chalinga en Choapa',
'Salidas agua subterránea','Riego','Retornos de agua']
        df.columns=df.apply(lambda x: x.name + ' (Hm3/mes)')
        df['Balance']=df.sum(axis=1)
        
        #plot balance Alternativa X
        df.index=['Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic','Ene',
                'Feb','Mar']
        fig, axes = plt.subplots(figsize = (17,11))
        sumas=pd.DataFrame(df.sum(axis=1))
        df.index=sumas.index
        df.plot(stacked=True, kind = 'bar', grid=True, ax = axes)

        sumas.plot(ax=axes, kind = 'line', grid=True, color = 'black',
                label = 'Total', marker = 'o', linewidth = 4, markersize = 10)
        df['Riego (Hm3/mes)'].multiply(1).plot(ax=axes, kind = 'line', grid=True, color = 'r',
                label = 'riego', marker = 'o', linewidth = 4, markersize = 10)
        pos = axes.get_position()
        # axes.set_position([pos.x0, pos.y0 * 4.5, pos.width * 1.0, pos.height * 0.5])
        # axes.legend(loc='center right', bbox_to_anchor=(1.0, -0.65), ncol=2)
        
        # axes.set_ylim([-3,3])
        axes.set_xlabel('Mes año hidrológico')
        axes.set_ylabel('Volumen ($Hm^3/mes$)')
        
        # df.plot(stacked=True, kind = 'bar', grid=True, ax = axes[1], title = 'Desagregada')
        
        # df.plot(stacked=False, kind = 'line', grid=True, ax = axes[1], title = 'Desagregada', marker = 'o')
        # df.plot(stacked=False, kind = 'line', grid=True, ax = axes[1], title = 'Desagregada', marker = 'o',
        #         subplots=True, layout = (2,2))
        # nam = os.path.join(folder, cuenca.replace(' ','_') + 'v3.jpg')
        # axes[0].set_ylabel('Volumen ($Hm^3/mes$)')
        # axes[1].set_ylabel('Volumen ($Hm^3/mes$)')
        res=abs(df.sum(axis=1)).sum()
        print(res)
        plt.suptitle('Balance Oferta-Demanda\n' + 'Choapa')

        return res


import numpy as np
import scipy.optimize as opt

r = opt.root(plots, x0=1.1288985823336968, method='hybr')
print(r.x)
# array([1.97522498 3.47287981 5.1943792  2.10120135 4.09593969])

plots(r.x)

#%%
if __name__=='__main__':
    main()
