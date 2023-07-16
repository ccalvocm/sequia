#%%

import pandas as pd
import os
import matplotlib.pyplot as plt

def rio(root):
    path=os.path.join(root,'limari.csv')
    df=pd.read_csv(path,index_col=0)

    df.index=index()
    return df

def GWLimari(root):
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

def index():
    return pd.date_range('2000-05-01','2022-04-01',freq='MS')

def headflows(root):
    path=os.path.join(root,'hf.csv')
    df=pd.read_csv(path,index_col=0,encoding = "ISO-8859-1")

    df.index=index()
    return df

def rioChoapa(root):
    path=os.path.join(root,'choapa.csv')
    df=pd.read_csv(path,index_col=0, encoding = "ISO-8859-1")

    df.index=index()
    return df

def rioIllapel(root):
    path=os.path.join(root,'illapel.csv')
    df=pd.read_csv(path,index_col=0, encoding = "ISO-8859-1")

    df.index=index()
    return df

def rioChalinga(root):
    path=os.path.join(root,'chalinga.csv')
    df=pd.read_csv(path,index_col=0, encoding = "ISO-8859-1")

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

def GWChoapa(root):
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

def GWChalinga(root):
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

def filterHFChoapa(df):
    wl=whitelistChoapa()
    cols=df.columns[df.columns.str.contains('|'.join(wl))]
    # cols=[x for x in cols if ('ILLAPEL' not in x) & ('CHALINGA' not in x)]

    return pd.DataFrame(df[cols])

def filterHFIllapel(df):
    wl=['Estero','Rio','CL_']
    cols=df.columns[df.columns.str.contains('|'.join(wl))]

    return pd.DataFrame(df[cols])

def whitelistChoapa():
    whitelist=['AN_','CL_','EST.','RIO']
    return whitelist


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

    def balanceHurtado(datei,datef):

        import matplotlib.pyplot as plt
        root=r'E:\CIREN\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Resultados\Hurtado'
        # entradas
        hfF=pd.DataFrame(headflows(root)).astype(float)

        hfF.index=index()

        cols=['AN_09','CL_20','CL_21','CL_22','CL_23','CL_24','CL_25','CL_26']
        hfF=hfF[hfF.columns[hfF.columns.str.contains('|'.join(cols))]]  

        tl=TL(root)

        cols=[x for x in tl.columns if '_hur' in x.lower()]
        tl=pd.DataFrame(tl[cols])

        colsRiego=[x for x in tl.columns if 'RIEG' in x or 'R_' in x or 'ET_HUR' in x]
        riego=tl

        colsAP=[x for x in tl.columns if 'AP_' in x]
        AP=tl[colsAP]

        GWin,GWout=GW(root)

        retornoRio=pd.read_csv(os.path.join(root,'ril.csv'),index_col=0)
        retornoRio.index=index()

        colsRIL=[x for x in retornoRio.columns if 'to Rio Hurtado' in x]
        retornoRio=retornoRio[colsRIL]

        qDesemb=pd.read_csv(os.path.join(root,'hurtado.csv'),index_col=0)
        qDesemb.index=index()

        GWout,GWin=GW(root)
        GWin=GWin[[x for x in GWin.columns if 'HUR' in x]]
        GWout=GWout[[x for x in GWout.columns if 'HUR' in x]]
        entradas=suma([GWin,hfF,retornoRio])
        remanentesRiego=suma([GWout,suma([riego.multiply(-1)])])
        salidas=suma([AP,qDesemb,GWout,riego])
        balance=pd.DataFrame(entradas-salidas,index=hfF.index)

        def plots(datei,datef,ineficienciaRiego=1.042e+00):
            df=pd.DataFrame(index=index())
            df['GWin']=dfTocol(GWin)
            df['headflows']=dfTocol(hfF)
            df['retornoRio']=dfTocol(retornoRio)
            df['AP']=dfTocol(AP.multiply(-1))
            df['qDesemb']=dfTocol(qDesemb.multiply(-1))
            df['GWout']=dfTocol(GWout.multiply(-1))
            df['riego']=ineficienciaRiego*dfTocol(riego.multiply(-1))
            df=df.loc[(df.index>=datei) & (df.index<=datef)]
            
            #plot balance Alternativa X
            df.index=['Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic','Ene',
                    'Feb','Mar']
            return df
    
        return plots(datei,datef)
        
    def balanceLimari(datei,datef):
        root=r'E:\CIREN\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Resultados\Limari'
        # entradas
        q=rio(root)
        hfF=pd.DataFrame(headflows(root)).astype(float)
        # hfF=hfF[[x for x in hfF.columns if 'Rio Hurtado  0 \ Headflow' not in x]]

        tl=TL(root)
        riego=tl
        retornoRio=pd.read_csv(os.path.join(root,'rf.csv'),index_col=0)
        retornoRio.index=index()
        sr=pd.read_csv(os.path.join(root,'sr.csv'),index_col=0,encoding='ISO-8859-1')
        sr.index=index()

        embalses=pd.read_csv(os.path.join(root,'EMBALSES.csv'),index_col=0)
        embalses.index=index()
        embalses=embalses.fillna(0)
        embalses=embalses[embalses.columns[embalses.columns.str.contains('Storage')]]

        AP=tl[tl.columns[tl.columns.str.contains('|'.join(['SSR','Sanitaria']))]]
        riego=riego[riego.columns[4:]]
        qDesemb=pd.DataFrame(q.iloc[:,-1])

        GWin,GWout=GWLimari(root)

        overflow=pd.DataFrame(GWin['Overflow'])*0
        GWin=GWin[GWin.columns[:-1]]

        GWin=GWin[[x for x in GWin.columns if 'Outflow' in x]]
        GWout=GWout[[x for x in GWout.columns if 'Inflow' in x]]

        GWout.index=index()

        entradas=suma([sr,embalses,GWin,hfF,retornoRio])
        remanentesRiego=suma([GWout,suma([riego.multiply(-1)])])
        salidas=suma([AP,qDesemb,GWout,riego,overflow])
        balance=pd.DataFrame(entradas-salidas,index=hfF.index)

        def plots(datei,datef,ineficienciaRiego=1.12889858):
            df=pd.DataFrame(index=index())
            df['sr']=dfTocol(sr)
            df['embalses']=dfTocol(embalses)
            df['GWin']=dfTocol(GWin)
            df['headflows']=dfTocol(hfF)
            df['retornoRio']=dfTocol(retornoRio)
            df['AP']=dfTocol(AP.multiply(-1))
            df['qDesemb']=dfTocol(qDesemb.multiply(-1))
            df['GWout']=dfTocol(GWout.multiply(-1))
            df['riego']=ineficienciaRiego*dfTocol(riego.multiply(-1))

            df=df.loc[(df.index>=datei) & (df.index<=datef)]
            
            #plot balance Alternativa X
            df.index=['Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic','Ene',
                    'Feb','Mar']
            return df
            
        return plots(datei,datef)
        
    #%%    
    datei='2020-04-01'
    datef='2021-03-01'
    datei='2015-04-01'
    datef='2016-03-01'
    dfHurtado=balanceHurtado(datei,datef).astype(float)
    dfLimari=balanceLimari(datei,datef)

    balance = pd.concat([dfHurtado, dfLimari],axis=1)

    balance=balance.groupby(lambda x:x, axis=1).sum()

    balance.columns=['Uso agua potable','Entrada agua subterránea',
                     'Salida agua subterránea','Entregas embalses',
                     'Agua superficial',
                     'Río Limarí en desembocadura',
                     'Retornos de agua','Riego','sr']
    
    balance['Agua superficial']=balance['Agua superficial']+balance['sr']
    del balance['sr']

    plt.close('all')
    fig, axes = plt.subplots(figsize = (17,11))
    sumas=pd.DataFrame(balance.sum(axis=1))
    balance.index=sumas.index
    balance.plot(stacked=True, kind = 'bar', grid=True, ax = axes,
                 legend=False,rot=0,colormap='Set3_r',
                 edgecolor='k', width=1, linestyle="--")

    sumas.plot(ax=axes, kind = 'line', grid=True, color = 'black',
            label = 'Total', linewidth = 3, markersize = 4,
            legend=False,marker='o',linestyle='-')
    fs=16
    axes.set_ylabel('Volumen (Hm3/mes)',fontsize=fs)
    axes.set_xlabel('Mes año hidrológico',fontsize=fs)
    axes.set_title('Balance hídrico Limarí',fontsize=fs)
    axes.legend(['Balance']+list(balance.columns),loc='best', ncol=2,
                fontsize=12)
    balance.sum(axis=1)

    if datei=='2020-04-01':
        axes.set_ylim([-30,30])
        plt.savefig('balance_Limari_Hurtado_2020.png',dpi=300,
                    bbox_inches='tight')
    else:
        plt.savefig('balance_Limari_Hurtado_2015.png',dpi=300,
                    bbox_inches='tight')
            

#%%
if __name__=='__main__':
    main()
