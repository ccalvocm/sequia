#%%

import pandas as pd
import os


def index():
    return pd.date_range('1989-04-01','2021-03-01',freq='MS')

def headflows(root):
    path=os.path.join(root,'hf.csv')
    df=pd.read_csv(path,index_col=0)

    df.index=index()
    return df

def rioChoapa(root):
    path=os.path.join(root,'choapa.csv')
    df=pd.read_csv(path,index_col=0)

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

    entradas=df[['Decrease in Storage for Embalse El Bato']].applymap(lambda x: abs(x))
    salidas=df[['Increase in Storage for Embalse El Bato']].applymap(lambda x: abs(x))
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
    
#%%
def main():

    #%%
    def residuals(ineficienciariego=1):
        import matplotlib.pyplot as plt
        root=r'G:\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Resultados\Choapa'
        # root=r'G:\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Resultados\Illapel'
        # entradas
        q=rioChoapa(root)
        # q=rioIllapel(root)
        inEmbalse,outEmbalse=corrales(root)
        # inEmbalse=pd.DataFrame(inEmbalse[inEmbalse.columns[:-2]])
        # inEmbalse,outEmbalse=elbato(root)
        hf=headflows(root)
        hfF=filterHFChoapa(hf)
        canales=hf[[x for x in hf.columns if x not in hfF.columns]]
        # hfF=filterHFIllapel(hf)

        tl=TL(root)
        riego=riegoTL(tl)
        retornoRio=returnRiver(root)
        retornoAquifero=returnAq(root)
        AP=tl[tl.columns[tl.columns.str.contains('to AP')]]
        MIN=tl[tl.columns[tl.columns.str.contains('to MIN')]]
        IND=tl[tl.columns[tl.columns.str.contains('to IND')]]
        qDesemb=pd.DataFrame(q.iloc[:,-1])

        GWin,GWout=GW(root)
        overflow=pd.DataFrame(GWin['Overflow'])
        GWin=GWin[GWin.columns[GWin.columns.str.contains(' to_ ')]]
        GWout=GWout[GWout.columns[GWout.columns.str.contains('Below')]]
        GWin=GWin[GWin.columns[GWin.columns.str.contains('Outflow to REST_MPL')]]
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
        entradas=suma([hfF,inEmbalse,GWin,rf])
        remanentesRiego=suma([GWout,suma([riego.multiply(-1)])])
        salidas=suma([AP,MIN,IND,qDesemb,outEmbalse,GWout])
        balance=pd.DataFrame(entradas-salidas,index=hfF.index)
        plt.close('all')
        fig,ax=plt.subplots(1)

        def dfTocol(df):
            return df.sum(axis=1)

        pd.DataFrame(suma([hfF]).values,index=index(),columns=['hfF']).plot(ax=ax)
        pd.DataFrame(suma([inEmbalse]).values,index=index(),columns=['inEmbalse']).plot(ax=ax)
        pd.DataFrame(suma([retornoRio]).values,index=index(),columns=['retornoRio']).plot(ax=ax)
        pd.DataFrame(suma([riego.multiply(-1)]).values,index=index(),columns=['riego']).plot(ax=ax)
        pd.DataFrame(suma([GWout.multiply(-1)]).values,index=index(),columns=['GWout']).plot(ax=ax)
        pd.DataFrame(suma([qDesemb.multiply(-1)]).values,index=index(),columns=['qDesemb']).plot(ax=ax)
        pd.DataFrame(suma([outEmbalse.multiply(-1)]).values,index=index(),columns=['outEmbalse']).plot(ax=ax)
        pd.DataFrame(suma([balance]).values,index=index(),columns=['balance']).plot(ax=ax)
        pd.DataFrame(suma([remanentesRiego.multiply(-1)]).values,index=index(),columns=['remanentesRiego']).plot(ax=ax)
        # ax.set_xlim([pd.to_datetime('2015-01-01'),index()[-1]])
        balance.describe()

        df=pd.DataFrame(index=index())
        df['headflows']=dfTocol(hfF)
        df['inEmbalse']=dfTocol(inEmbalse)
        # df['retornoRio']=-dfTocol(retornoRio)
        df['GWin']=dfTocol(GWin)+dfTocol(AP.multiply(1))+dfTocol(MIN.multiply(1))+dfTocol(IND.multiply(1))
        df['rf']=dfTocol(rf)

        df['qDesemb']=dfTocol(qDesemb.multiply(-1))
        df['outEmbalse']=dfTocol(outEmbalse.multiply(-1))
        # df['balance']=dfTocol(balance)
        # df['GWout']=dfTocol(GWout.multiply(-1))

        df['GWout']=dfTocol(GWout.multiply(-1))+dfTocol(riego.multiply(1))
        df['riego']=ineficienciariego*dfTocol(riego.multiply(-1))
        df['IND']=dfTocol(IND.multiply(-1))
        df['MIN']=dfTocol(MIN.multiply(-1))
        df['AP']=dfTocol(AP.multiply(-1))

        df=df.apply(lambda x: x*df.index.daysinmonth.values)
        df=df.multiply(86400/1e6)
        df.columns=['Agua superficial','Entregas embalse','Entradas agua subterránea',
                    'Retornos de agua','Río Choapa en desembocadura',
                    'Retencion embalses','Salidas agua subteránea',
                    'Riego','Uso industrial','Uso minería','Agua potable']
        df['Balance']=df.sum(axis=1)
        df.columns=df.apply(lambda x: x.name+' (Hm3/mes)')        
        df=df.loc[(df.index>='2015-04-01') & (df.index<='2016-03-01')]
        # df=df.loc[(df.index>='2020-04-01') & (df.index<='2021-03-01')]
        
        #%%
        #plot balance Alternativa X
        df.index=['Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic','Ene',
                'Feb','Mar']
        fig, axes = plt.subplots(figsize = (17,11))
        sumas=pd.DataFrame(df.sum(axis=1))
        df.index=sumas.index
        df.plot(stacked=True, kind = 'bar', grid=True, ax = axes)

        sumas.plot(ax=axes, kind = 'line', grid=True, color = 'black',
                label = 'Total', marker = 'o', linewidth = 4, markersize = 10)
        df['riego'].plot(ax=axes, kind = 'line', grid=True, color = 'r',
                label = 'Total', marker = 'o', linewidth = 4, markersize = 10)
        pos = axes.get_position()
        # axes.set_position([pos.x0, pos.y0 * 4.5, pos.width * 1.0, pos.height * 0.5])
        # axes.legend(loc='center right', bbox_to_anchor=(1.0, -0.65), ncol=2)
        
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

    r = opt.root(residuals, x0=1.1288985823336968, method='hybr')
    print(r)
    # array([1.97522498 3.47287981 5.1943792  2.10120135 4.09593969])

    print(r.x)


#%%
if __name__=='__main__':
    main()
