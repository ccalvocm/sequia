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

    def balanceChalinga(datei,datef):
        root=r'G:\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Resultados\Chalinga'

        q=rioChalinga(root)

        hf=headflows(root)
        hfF=pd.DataFrame(hf.sum(axis=1))
        tl=TL(root)
        riego=riegoTL(tl)
        # retornoRio=returnRiver(root)
        AP=tl[tl.columns[tl.columns.str.contains('to AP')]]*0
        MIN=tl[tl.columns[tl.columns.str.contains('to MIN')]]*0
        IND=tl[tl.columns[tl.columns.str.contains('to IND')]]*0
        qDesemb=pd.DataFrame(q.iloc[:,-1])

        GWin,GWout=GWChalinga(root)
        GWin=GWin[GWin.columns[GWin.columns.str.contains(' to ')]]*0
        GWout=GWout[GWout.columns[GWout.columns.str.contains('Below')]]

        GWout.index=index()
        entradas=suma([hfF,GWin,riego])
        remanentesRiego=suma([GWout,suma([riego.multiply(-1)])])
        salidas=suma([AP,MIN,IND,qDesemb,GWout])
        balance=pd.DataFrame(entradas-salidas,index=hfF.index)

        def dfTocol(df):
            return df.sum(axis=1)

        df=pd.DataFrame(index=index())
        df['headflows']=dfTocol(hfF)
        df['GWin']=dfTocol(riego)
        # df['qDesemb']=dfTocol(qDesemb.multiply(-1))

        df['GWout']=dfTocol(GWout.multiply(-1))+dfTocol(riego.multiply(1))
        df['riego']=dfTocol(riego.multiply(-1))
        df=df.apply(lambda x: x*df.index.daysinmonth.values)
        df=df.multiply(86400/1e6)

        df=df.loc[(df.index>=datei) & (df.index<=datef)]
        
        #plot balance Alternativa X
        df.index=['Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic','Ene',
                'Feb','Mar']
        return df
        
    def balanceChoapa(datei,datef):
        root=r'G:\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Resultados\Choapa'
        # entradas
        q=rioChoapa(root)
        inEmbalse,outEmbalse=corrales(root)
        hf=headflows(root)
        hfF=filterHFChoapa(hf)
        cols=[x for x in hfF.columns if ('ILLAPEL' not in x) & ('CHALINGA' not in x)]
        hfF=pd.DataFrame(hfF[cols])
        tl=TL(root)
        riego=riegoTL(tl)
        retornoRio=returnRiver(root)
        AP=tl[tl.columns[tl.columns.str.contains('to AP')]]
        MIN=tl[tl.columns[tl.columns.str.contains('to MIN')]]
        IND=tl[tl.columns[tl.columns.str.contains('to IND')]]
        qDesemb=pd.DataFrame(q.iloc[:,-1])

        GWin,GWout=GWChoapa(root)
        overflow=pd.DataFrame(GWin['Overflow'])
        GWin=GWin[GWin.columns[GWin.columns.str.contains(' to_ ')]]
        GWout=GWout[GWout.columns[GWout.columns.str.contains('Below')]]
        GWin=GWin[GWin.columns[GWin.columns.str.contains('Outflow to REST_MPL')]]

        rf=pd.read_csv(r'G:\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Resultados\Choapa\rf.csv',index_col=0)
        rf.index=index()
        rf=pd.DataFrame(rf[rf.columns[0]])
        GWout.index=index()
        entradas=suma([hfF,inEmbalse,GWin,rf])
        remanentesRiego=suma([GWout,suma([riego.multiply(-1)])])
        salidas=suma([AP,MIN,IND,qDesemb,outEmbalse,GWout])
        balance=pd.DataFrame(entradas-salidas,index=hfF.index)

        df=pd.DataFrame(index=index())
        df['headflows']=dfTocol(hfF)
        df['inEmbalse']=dfTocol(inEmbalse)
        df['GWin']=dfTocol(GWin)+dfTocol(AP.multiply(1))+dfTocol(MIN.multiply(1))+dfTocol(IND.multiply(1))
        df['rf']=dfTocol(rf)
        df['qDesemb']=dfTocol(qDesemb.multiply(-1))
        df['outEmbalse']=dfTocol(outEmbalse.multiply(-1))
        # df['retornoRio']=dfTocol(retornoRio)

        df['GWout']=dfTocol(GWout.multiply(-1))+dfTocol(riego.multiply(1))
        df['riego']=dfTocol(riego.multiply(-1))
        df['IND']=dfTocol(IND.multiply(-1))
        df['MIN']=dfTocol(MIN.multiply(-1))
        df['AP']=dfTocol(AP.multiply(-1))

        df=df.apply(lambda x: x*df.index.daysinmonth.values)
        df=df.multiply(86400/1e6)

        df=df.loc[(df.index>=datei) & (df.index<=datef)]
        # df=df.loc[(df.index>='2020-04-01') & (df.index<='2021-03-01')]
        
        #plot balance Alternativa X
        df.index=['Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic','Ene',
                'Feb','Mar']
        
        return df

    def balanceIllapel(datei,datef):
        root=r'G:\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Resultados\Illapel'
        # entradas
        q=rioIllapel(root)
        inEmbalse,outEmbalse=elbato(root)
        hf=headflows(root)
        hfF=filterHFIllapel(hf)

        tl=TL(root)
        riego=riegoTL(tl)
        AP=tl[tl.columns[tl.columns.str.contains('to AP')]]*0
        MIN=tl[tl.columns[tl.columns.str.contains('to MIN')]]*0
        IND=tl[tl.columns[tl.columns.str.contains('to IND')]]*0
        qDesemb=pd.DataFrame(q.iloc[:,-1])

        GWin,GWout=GWChoapa(root)
        GWin=GWin[GWin.columns[GWin.columns.str.contains(' to ')]]
        GWout=GWout[GWout.columns[GWout.columns.str.contains('Inflow from ')]]

        rf=pd.read_csv(r'G:\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Resultados\Choapa\rf.csv',index_col=0)
        rf.index=index()
        rf=pd.DataFrame(rf[rf.columns[0]])*0
        GWout.index=index()
        entradas=suma([hfF,inEmbalse,GWin,rf])
        remanentesRiego=suma([GWout,suma([riego.multiply(-1)])])
        salidas=suma([AP,MIN,IND,qDesemb,outEmbalse,GWout,riego])
        balance=pd.DataFrame(entradas-salidas,index=hfF.index)

        print(    balance.describe())

        df=pd.DataFrame(index=index())
        df['headflows']=dfTocol(hfF)
        df['inEmbalse']=dfTocol(inEmbalse)
        df['GWin']=dfTocol(GWin)
        df['rf']=dfTocol(rf)

        # df['qDesemb']=dfTocol(qDesemb.multiply(-1))
        df['outEmbalse']=dfTocol(outEmbalse.multiply(-1))

        df['GWout']=dfTocol(GWout.multiply(-1))
        df['riego']=dfTocol(riego.multiply(-1))

        df=df.apply(lambda x: x*df.index.daysinmonth.values)
        df=df.multiply(86400/1e6)

        df=df.loc[(df.index>=datei) & (df.index<=datef)]

        # df=df.loc[(df.index>='2020-04-01') & (df.index<='2021-03-01')]
        
        #plot balance Alternativa X
        df.index=['Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic','Ene',
                'Feb','Mar']
        

        return df
    
    datei='2020-04-01'
    datef='2021-03-01'
    datei='2015-04-01'
    datef='2016-03-01'
    dfChoapa=balanceChoapa(datei,datef).astype(float)
    dfIllapel=balanceIllapel(datei,datef)
    dfChalinga=balanceChalinga(datei,datef)
    dfChoapa.sum(axis=1)+dfIllapel.sum(axis=1)+dfChalinga.sum(axis=1)

    balance = pd.concat([dfChoapa, dfIllapel, dfChalinga],axis=1)

    balance=balance.groupby(lambda x:x, axis=1).sum()

    balance.columns=['Uso agua potable','Entrada agua subterránea',
                     'Salida agua subterránea','Uso Industrial',
                     'Uso Minería','Agua superficial','Entregas embalses',
                     'Retención embalses','Río Choapa en desembocadura',
                     'Retornos de agua','Riego']

    plt.close('all')

    fig, axes = plt.subplots(figsize = (17,11))
    sumas=pd.DataFrame(balance.sum(axis=1))
    balance.index=sumas.index
    balance.plot(stacked=True, kind = 'bar', grid=True, ax = axes,
                 legend=False,rot=0,colormap="viridis",
                 edgecolor='k', width=1, linestyle="--")

    sumas.plot(ax=axes, kind = 'line', grid=True, color = 'black',
            label = 'Total', linewidth = 3, markersize = 4,
            legend=False,marker='o',linestyle='-')
    fs=16
    axes.set_ylabel('Volumen (Hm3/mes)',fontsize=fs)
    axes.set_xlabel('Mes año hidrológico',fontsize=fs)
    axes.set_title('Balance hídrico Choapa-Illapel-Chalinga',fontsize=fs)
    axes.legend(['Balance']+list(balance.columns),loc='best', ncol=2,
                fontsize=12)
    balance.sum(axis=1)

    if datei=='2020-04-01':
        axes.set_ylim([-30,30])
        plt.savefig('balance_choapa_illapel_chalinga_2020.png',dpi=300,
                    bbox_inches='tight')
    else:
        plt.savefig('balance_choapa_illapel_chalinga_2015.png',dpi=300,
                    bbox_inches='tight')
            

#%%
if __name__=='__main__':
    main()
