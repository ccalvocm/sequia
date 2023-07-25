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
        root=r'G:\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Resultados\Hurtado'
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

        def dfRet(datei,datef,ineficienciaRiego):
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
            df=df.apply(lambda x: x*df.index.daysinmonth.values)
            df=df.multiply(86400/1e6)
            df.index=['Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic','Ene',
                    'Feb','Mar']
            return df
    
        def residual(ineficienciaRiego):
            df=dfRet(datei,datef,ineficienciaRiego)
            res=abs(df.sum(axis=1)).sum()
            print(res)
            return res

        import numpy as np
        import scipy.optimize as opt

        r = opt.root(residual, x0=1.1288985823336968, 
                     method='hybr')

        print(r)

        print(r.x)

        return dfRet(datei,datef,r.x)
        
    def balanceLimari(datei,datef):
        root=r'G:\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Resultados\Limari'
        # entradas
        q=rio(root)
        hfF=pd.DataFrame(headflows(root)).astype(float)

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

        # demanda otros usos
        IND=tl[tl.columns[tl.columns.str.contains('|'.join(['IND']))]]
        MIN=tl[tl.columns[tl.columns.str.contains('|'.join(['MIN']))]]
        PEC=tl[tl.columns[tl.columns.str.contains('|'.join(['PEC']))]]
        AP=tl[tl.columns[tl.columns.str.contains('|'.join(['SSR',
                                                           'Sanitaria']))]]
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

        def dfRet(datei,datef,ineficienciaRiego):
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
            df['IND']=dfTocol(IND.multiply(-1))
            df['MIN']=dfTocol(MIN.multiply(-1))
            df['PEC']=dfTocol(PEC.multiply(-1))
            df['GWout']=df['GWout']+df['GWin']
            del df['GWin']
            df=df.loc[(df.index>=datei) & (df.index<=datef)]
            
            #plot balance Alternativa X
            df=df.apply(lambda x: x*df.index.daysinmonth.values)
            df=df.multiply(86400/1e6)
            df.index=['Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic','Ene',
                    'Feb','Mar']
            return df

        def residualLimari(ineficienciaRiego):
            df=dfRet(datei,datef,ineficienciaRiego)
            res=abs(df.sum(axis=1)).sum()
            print(res)
            return res

        import numpy as np
        import scipy.optimize as opt

        r = opt.root(residualLimari, x0=1.1288985823336968, 
                     method='hybr')
        print(r)

        print(r.x)

        return dfRet(datei,datef,r.x)

    def balanceRapel(datei,datef):
        root=r'G:\OneDrive - ciren.cl\2022_ANID_sequia\Proyecto\3_Objetivo3\Resultados'
        root=os.path.join(root,'Rapel')
        # entradas
        hfF=pd.DataFrame(headflows(root)).astype(float)
        hfF.index=index()

        def filterDf(df,lista):
            dfRet=df[[x for x in df.columns if any(map(x.__contains__,lista))]]
            return pd.DataFrame(dfRet)
        
        # headflows Rapel
        heads=['AN_04','CL_05','CL_06','Rio Palomo','Rio Rapel','CL_07','CL_08',
            'Rio Los Molles']
        hfF=filterDf(hfF,heads)
        tl=TL(root)

        cols=[x for x in tl.columns if ('rap' in x.lower()) & ('Withdrawal' in x) | ('RIEG_14_19_RR' in x)]
        riego=filterDf(tl,cols)

        # agua potable
        AP=tl[[x for x in tl.columns if 'AP_AC_RAP_01' in x]]

        GWin,GWout=GW(root)
        GWin=filterDf(GWin,['AC_RAP_01'])
        GWout=filterDf(GWout,['AC_RAP_01'])

        retornoRio=pd.read_csv(os.path.join(root,'ril.csv'),index_col=0)
        retornoRio.index=index()

        colsRIL=[x for x in retornoRio.columns if 'rap' in str.lower(x)]
        retornoRio=retornoRio[colsRIL]

        qDesemb=pd.read_csv(os.path.join(root,'Rapel.csv'),index_col=0)
        qDesemb.index=index()
        qDesemb=pd.DataFrame(qDesemb.iloc[:,-1])

        # caudal de entrada del Limari PROMMRA
        qAfluPROM=pd.read_csv(os.path.join(root,'RapelEnJunta.csv'),index_col=0)
        qAfluPROM.index=index()
        qAfluPROM=pd.DataFrame(qAfluPROM.iloc[:,-1])

        deficit=pd.read_csv(os.path.join(root,'unmetD.csv'),index_col=0)
        deficit.index=index()
        deficit=deficit[[x for x in deficit.columns if any(map(x.__contains__,
                                                        ['rap']))]]
        def fixRapel(df):
            diff=df['qAfluPROM']-df['qDesemb']
            idx=diff[diff<0].index
            df.loc[idx,'GWin']=df.loc[idx,'GWin'].values-diff.loc[idx].values
            idx=diff[diff>0].index
            df.loc[idx,'GWout']=df.loc[idx,'GWout'].values-diff.loc[idx].values
            df['qDesemb']=df['qAfluPROM'].values
            del df['qAfluPROM']
            return df

        df=pd.DataFrame(index=index())
        df['headflows']=dfTocol(hfF)
        df['retornoRio']=dfTocol(retornoRio)
        df['qDesemb']=-dfTocol(qDesemb)
        df['qAfluPROM']=-dfTocol(qAfluPROM)
        df['GWin']=-dfTocol(GWin)
        df['GWout']=-dfTocol(GWout)
        df['riego']=-dfTocol(riego)
        df['AP']=-dfTocol(AP)
        df=fixRapel(df)
        df=df.apply(lambda x: x*df.index.daysinmonth.values)
        df=df.multiply(86400/1e6)
        df=df.loc[(df.index>=datei) & (df.index<=datef)]
        df.index=['Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic','Ene',
                'Feb','Mar']
        
        return df        
    
    def balanceMostazal(datei,datef):
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
        qAfluPROM=pd.read_csv(os.path.join(root,'mostazalDesembocadura.csv'),
                              index_col=0)
        qAfluPROM.index=index()
        qAfluPROM=pd.DataFrame(qAfluPROM.iloc[:,-1])

        def fixMostazal(df):
            diff=df['qAfluPROM']-df['qDesemb']
            idx=diff[diff<0].index
            df.loc[idx,'GWin']=df.loc[idx,'GWin'].values-diff.loc[idx].values
            idx=diff[diff>0].index
            df.loc[idx,'GWout']=df.loc[idx,'GWout'].values-diff.loc[idx].values
            df['qDesemb']=df['qAfluPROM'].values
            del df['qAfluPROM']
            return df

        df=pd.DataFrame(index=index())
        df['headflows']=dfTocol(hfF)
        df['retornoRio']=dfTocol(retornoRio)
        df['qDesemb']=-dfTocol(qDesemb)
        df['qAfluPROM']=-dfTocol(qAfluPROM)
        df['GWin']=-dfTocol(GWin)
        df['GWout']=-dfTocol(GWout)
        df['riego']=-dfTocol(riego)
        df['AP']=-dfTocol(AP)
        df=fixMostazal(df)
        df=df.apply(lambda x: x*df.index.daysinmonth.values)
        df=df.multiply(86400/1e6)
        df=df.loc[(df.index>=datei) & (df.index<=datef)]
        df.index=['Abr','May','Jun','Jul','Ago','Sep','Oct','Nov','Dic','Ene',
                'Feb','Mar']
        return df
    #%%    
    def balanceSHACLimari(datei,datef):
        # datei='2015-04-01'
        # datef='2016-03-01'

        # agrupa las demandas de los dos SHACs
        dfLimari=balanceLimari(datei,datef)
        dfRapel=balanceRapel(datei,datef)
        dfMostazal=balanceMostazal(datei,datef)

        dfLimari['headflows']=dfLimari['headflows'].values+dfRapel['qDesemb'].values+dfMostazal['qDesemb'].values
        del dfRapel['qDesemb']
        del dfMostazal['qDesemb']  
        balance = pd.concat([dfRapel,dfMostazal,dfLimari],axis=1)

        balance=balance.groupby(lambda x:x, axis=1).sum()

        balance.columns=['Uso agua potable','Entrada agua subterránea',
                        'Salida agua subterránea','Uso industrial',
                        'Uso minería','Uso pecuario','Entregas embalses',
                        'Agua superficial',
                        'Río Limarí en desembocadura',
                        'Retornos de agua','Riego','sr']
        
        balance['Agua superficial']=balance['Agua superficial']+balance['sr']
        del balance['sr']

        balance['Salida agua subterránea']=balance['Salida agua subterránea']+balance['Entrada agua subterránea']
        balance['Entrada agua subterránea']=0*balance['Entrada agua subterránea']

        plt.close('all')
        fig, axes = plt.subplots(figsize = (17,11))
        sumas=pd.DataFrame(balance.sum(axis=1))
        balance.index=sumas.index
        balance.plot(stacked=True, kind = 'bar', grid=True, ax = axes,
                    legend=False,rot=0,colormap='Set3',
                    edgecolor='k', width=1, linestyle="--")

        sumas.plot(ax=axes, kind = 'line', grid=True, color = 'black',
                label = 'Total', linewidth = 3, markersize = 4,
                legend=False,marker='o',linestyle='-')
        fs=16
        axes.set_ylabel('Volumen ($Hm^3$/mes)',fontsize=fs)
        axes.set_xlabel('Mes año hidrológico',fontsize=fs)
        axes.set_title('Balance hídrico Limarí',fontsize=fs)
        axes.legend(['Balance']+list(balance.columns),loc='best', ncol=2,
                    fontsize=12)
        unmetD2020=[0.0662459,0.0564198,0.0525379,0.1368770,0.1220770,0.2171360,
                    0.2261390,0.6010160,1.0263600,0.9745250,0.7091160,0.2906740]
        unmetD2015=[0.85410600,0.45570300,0.24526200,0.22282140,0.06601740,
0.08971860,0.00029400,0.00564240,0.11683680,0.64045800,0.69492000,0.34250340,]
        
        balance['Balance']=balance.sum(axis=1)
        if '2020' in datei:
            balance['Demanda insatisfecha']=unmetD2020
        else:
            balance['Demanda insatisfecha']=unmetD2015
        
        balance.columns=[x+' (Hm3/mes)' for x in balance.columns]
        return balance

    # balanceSHACLimari2015=balanceSHACLimari('2015-04-01','2016-03-01')
    # balanceSHACLimari2020=balanceSHACLimari('2020-04-01','2021-03-01')

    def cuencaLimari():
        datei='2020-04-01'
        datef='2021-03-01'
        # datei='2015-04-01'
        # datef='2016-03-01'

        # agrupa las demandas de los dos SHACs
        dfHurtado=balanceHurtado(datei,datef).astype(float)
        dfLimari=balanceLimari(datei,datef)
        dfRapel=balanceRapel(datei,datef)
        dfMostazal=balanceMostazal(datei,datef)
        
        dfLimari['headflows']=dfLimari['headflows'].values+dfRapel['qDesemb'].values+dfMostazal['qDesemb'].values+dfHurtado['qDesemb'].values
        del dfRapel['qDesemb']
        del dfMostazal['qDesemb'] 
        del dfHurtado['qDesemb']

        balance = pd.concat([dfHurtado, dfLimari],axis=1)

        balance=balance.groupby(lambda x:x, axis=1).sum()

        balance.columns=['Uso agua potable','Entrada agua subterránea',
                        'Salida agua subterránea','Uso industrial',
                        'Uso minería',
                        'Uso pecuario',
                        'Entregas embalses',
                        'Agua superficial',
                        'Río Limarí en desembocadura',
                        'Retornos de agua','Riego','sr']
        
        balance['Agua superficial']=balance['Agua superficial']+balance['sr']
        del balance['sr']

        balance['Salida agua subterránea']=balance['Salida agua subterránea']+balance['Entrada agua subterránea']
        balance['Entrada agua subterránea']=0*balance['Entrada agua subterránea']

        cols=['Uso agua potable', 'Entrada agua subterránea',
    'Salida agua subterránea','Entregas embalses',
    'Agua superficial','Río Limarí en desembocadura',
    'Retornos de agua', 'Riego','Uso industrial',
                     'Uso minería','Uso pecuario']
        balance=balance[cols]
        plt.close('all')
        from matplotlib.pyplot import cm
        import numpy as np
        color = [np.array([0.55294118, 0.82745098, 0.78039216, 1.        ]),
        np.array([1.        , 1.        , 0.70196078, 1.        ]),
        np.array([0.98431373, 0.50196078, 0.44705882, 1.        ]),
        np.array([0.99215686, 0.70588235, 0.38431373, 1.        ]),
        np.array([0.70196078, 0.87058824, 0.41176471, 1.        ]),
        np.array([0.85098039, 0.85098039, 0.85098039, 1.        ]),
        np.array([0.8       , 0.92156863, 0.77254902, 1.        ]),
        np.array([1.        , 0.92941176, 0.43529412, 1.        ])]
        color2 = list(cm.tab20(np.linspace(0, 1, len(balance.columns)+2)))
        colores=color+[color2[0]]+[color2[5]]+[color2[4]]
        test_keys = list(balance.columns)
        colors = {test_keys[i]: colores[i] for i in range(len(colores))}
        fig, axes = plt.subplots(figsize = (17,11))
        sumas=pd.DataFrame(balance.sum(axis=1))*0.5
        balance.index=sumas.index
        balance.plot(stacked=True, kind = 'bar', grid=True, ax = axes,
                 legend=False,rot=0,color=colors,
                 edgecolor='k', width=1, linestyle="--")

        sumas.plot(ax=axes, kind = 'line', grid=True, color = 'black',
                label = 'Total', linewidth = 3, markersize = 4,
                legend=False,marker='o',linestyle='-')
        fs=16
        axes.set_ylabel('Volumen ($Hm^3$/mes)',fontsize=fs)
        axes.set_xlabel('Mes año hidrológico',fontsize=fs)
        axes.set_title('Balance hídrico Limarí',fontsize=fs)
        axes.legend(['Balance']+list(balance.columns),loc='best', ncol=2,
                    fontsize=12)
        balance.sum(axis=1)

        if datei=='2020-04-01':
            axes.set_ylim([-90,90])
            plt.savefig('balance_Limari_Hurtado_2020.png',dpi=300,
                        bbox_inches='tight')
        else:
            plt.savefig('balance_Limari_Hurtado_2015.png',dpi=300,
                        bbox_inches='tight')
            

#%%
if __name__=='__main__':
    main()
