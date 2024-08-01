# -*- coding: utf-8 -*-
"""
Created on Thu Jan 27 08:17:53 2022

Analyse van aantal in- en uitstappers per halte.

Gegevens van format G01 voor Keolis

Voor Arriva een HB-log

XX = jaartotaal
YY = jaargemiddelde

@author: kuinr01
"""
import pandas as pd
import os
import calendar
import timeit
from folders import G01_folder, CHB_folder, HB_folder
#%% 
def O10_to_G01(df):    
    df_instappers = pd.DataFrame(df.groupby(['CONCESSIE','JAAR','MAAND','DAGTYPE','HALTECODE_HERKOMST'])['RITTEN'].sum())
    df_instappers.index = df_instappers.index.rename(['CONCESSIE','JAAR','MAAND','DAGTYPE','HALTECODE'])
    df_instappers = df_instappers.rename(columns = {'RITTEN':'INSTAP'})                         
    
    df_uitstappers = pd.DataFrame(df.groupby(['CONCESSIE','JAAR','MAAND','DAGTYPE','HALTECODE_BESTEMMING'])['RITTEN'].sum())
    df_uitstappers.index = df_uitstappers.index.rename(['CONCESSIE','JAAR','MAAND','DAGTYPE','HALTECODE'])
    df_uitstappers = df_uitstappers.rename(columns = {'RITTEN':'UITSTAP'})     
    
    df_jaar = pd.concat([df_instappers, df_uitstappers], axis=1)
    df_jaar = df_jaar.unstack('DAGTYPE')
    cols = ['_'.join(col) for col in df_jaar.columns]
    df_jaar.columns = cols
    return df_jaar

def tel_vakantie_en_niet_vakantie_op(df):              
    df['INSTAP_WERK'] = df[['INSTAP_WERK_NIETVAK','INSTAP_WERK_VAK']].sum(axis=1) 
    df['UITSTAP_WERK'] = df[['UITSTAP_WERK_NIETVAK','UITSTAP_WERK_VAK']].sum(axis=1)
   
    df['INSTAP_ZA'] = df[['INSTAP_ZA_NIETVAK','INSTAP_ZA_VAK']].sum(axis=1)
    df['UITSTAP_ZA'] = df[['UITSTAP_ZA_NIETVAK', 'UITSTAP_ZA_VAK']].sum(axis=1)
   
    df['INSTAP_ZO'] = df[['INSTAP_ZO_NIETVAK','INSTAP_ZO_VAK']].sum(axis=1)
    df['UITSTAP_ZO'] = df[['UITSTAP_ZO_NIETVAK', 'UITSTAP_ZO_VAK']].sum(axis=1)
    return df 

def per_dagtype(dagtype, jaar, maand, ritten):
    if dagtype == 'WERK':
        aantal = pd.bdate_range('{}-{}-01'.format(jaar,maand),
                                '{}-{}-{}'.format(jaar,maand,calendar.monthrange(int(jaar),int(maand))[1])).shape[0]
        ritten = ritten / aantal
        
    elif dagtype == 'ZA':
        aantal = pd.Series(pd.date_range('{}-{}-01'.format(jaar,maand),
             '{}-{}-{}'.format(jaar,maand,calendar.monthrange(int(jaar),int(maand))[1])).weekday).value_counts().loc[5]
        ritten = ritten / aantal

    elif dagtype == 'ZO':
        aantal = pd.Series(pd.date_range('{}-{}-01'.format(jaar,maand),
             '{}-{}-{}'.format(jaar,maand,calendar.monthrange(int(jaar),int(maand))[1])).weekday).value_counts().loc[6]
        ritten = ritten / aantal
        
    else:
        ritten = pd.NA
    return ritten

def leading_zero(a):
    if len(a)==1:
        return '0'+a
    else:
        return a
#%% Input
filter_jaar = '2023'
#%%

dtype_g01 = {'HNR':str, 'JAAR':str, 'MAAND':str, 'LN_ID_OV_MIJ':str, 'NR_CONS_GEB':str}

reizigerskolommen = ['INSTAP_WERK', 'UITSTAP_WERK', 'INSTAP_ZA', 'UITSTAP_ZA',
                               'INSTAP_ZO', 'UITSTAP_ZO']
g01_kolommen = ['DATAOWNERCODE','CONCESSIE','JAAR','MAAND','STOPPLACECODE','QUAYCODE'] + reizigerskolommen

dagen_per_type = {'2019':{'WERK':255,'ZA':52,'ZO':58},
                  '2020':{'WERK':255,'ZA':51,'ZO':60},
                  '2021':{'WERK':256,'ZA':52,'ZO':57},
                  '2022':{'WERK':255,'ZA':53,'ZO':57},
                  '2023':{'WERK': 254, 'ZA': 53,'ZO':58}}

#Feestdagen
#Nieuwjaarsdag, Tweede Paasdag,  Hemelvaartsdag, Tweede Pinksterdag, 1e en 2e kerstdag
feestzondagen = {'2019':['1-1-2019','22-4-2019','30-5-2019','10-6-2019','25-12-2019','26-12-2019'],
                  '2020':['1-1-2020','13-4-2020','21-5-2020','1-6-2020','25-12-2020','26-12-2020'],
                  '2021':['1-1-2021','5-4-2021','13-5-2021','24-5-2021','25-12-2021','26-12-2021'],
                  '2022': ['1-1-2022','18-4-2022','26-5-2022','5-6-2022','25-12-2022','26-12-2022']}
# Koningsdag, Bevrijdingsdag
feestzaterdagen = {'2019':['27-4-2019'], #bevrijdingsdag op zondag
                  '2020':['27-4-2020','5-5-2020'],
                  '2021':['27-4-2021','5-5-2021'],
                  '2022':['27-4-2022','5-5-2022'],
                  }



#laad CHB gegevens
if filter_jaar in ['2018','2019','2020','2021','2022']:
    df_chb = pd.read_csv(os.path.join(CHB_folder,f'CHB_quays_{int(filter_jaar ) + 1}-01-01.csv'), sep=';',
                     usecols=['stopplacecode','quaycode', 'name','town','rd-x', 'rd-y'])
elif filter_jaar in ['2023']:
    df_chb = pd.read_csv(os.path.join(CHB_folder,f'CHB_quays_{int(filter_jaar ) + 1}-01-01.csv'), sep=',',
                     usecols=['stopplacecode','quaycode', 'quayname','town','rd-x', 'rd-y'])
    df_chb = df_chb.rename(columns={'quayname':'name'})
else:
    raise 'Onbekend jaar'
df_chb.columns = [x.upper() for x in df_chb.columns]

PSA_tabel = pd.read_csv(os.path.join('C:\\','data','CHB','PSA_tabel.csv'), sep=',')
PSA_tabel.columns = [x.upper() for x in PSA_tabel.columns]



#%% KEOLIS / SYNTUS
busconcessies = ['17','18','19','21','300']

PSA_tabel_keolis = PSA_tabel.loc[PSA_tabel['DATAOWNERCODE'] == 'KEOLIS'].set_index('USERSTOPCODE')['QUAYCODE'].to_dict()
chb_keolis_dict = df_chb.loc[df_chb['QUAYCODE'].isin(PSA_tabel_keolis.values())].set_index('QUAYCODE')['STOPPLACECODE'].to_dict()
#maak een los dataframe om de gegevens van Keolis in te plaatsen
df_keolis = pd.DataFrame()

# de bestanden voor KEOLIS zitten in een map per maand
for maandfolder in os.listdir(os.path.join(G01_folder,'KEOLIS')):
    #als de maand in het onderzoeksjaar valt, neem het mee
    if maandfolder.split(' ')[2][0:2] not in filter_jaar[2:4]:
        continue
    print(maandfolder)
    #per concessies is er een document met de G01
    for concessiefile in os.listdir(os.path.join(G01_folder,'KEOLIS',maandfolder)):
        if concessiefile.split('_')[0] in busconcessies:
            print(concessiefile)
            #per modaliteit
            # in de tabel staat een rij met de totalen per uurblok per lijn voor een maand
            df_concessie = pd.read_csv(os.path.join(G01_folder, 'KEOLIS', maandfolder, concessiefile),
                                       encoding='latin-1',sep=";", dtype = dtype_g01)
            df_concessie['HNR'] = df_concessie['HNR'].str.strip()
            df_concessie['LN_ID_OV_MIJ'] = df_concessie['LN_ID_OV_MIJ'].str.strip()
            df_concessie = tel_vakantie_en_niet_vakantie_op(df_concessie)
            df_concessie.loc[df_concessie['HNR'].map(PSA_tabel_keolis).notna(),'QUAYCODE'] = df_concessie['HNR'].map(PSA_tabel_keolis)
            df_concessie['STOPPLACECODE'] = df_concessie['QUAYCODE'].map(chb_keolis_dict)
            
            df_keolis = pd.concat([df_keolis,df_concessie])

# om later ook bestanden op publiekslijn mogelijk te maken voen
# lijn = pd.concat([pd.read_csv("C:\data\kv1\{}\LINEXXXXXX.TMI".format(x), sep = '|',dtype={'[LinePlanningNumber]':str}) for x in ['KEOLIS','SYNTUS']]) 
# lijn = lijn.set_index('[LinePlanningNumber]')['[LinePublicNumber]'].to_dict()

# df_keolis.loc['LIJN'] = df_keolis['LN_ID_OV_MIJ'].replace(lijn)
  
df_keolis = df_keolis.rename(columns={'HNR':'HALTECODE','NR_CONS_GEB':'CONCESSIE'})
df_keolis.loc[df_keolis['CONCESSIE'].isin(['17','301','302','303']),'CONCESSIE'] =  '300'
df_keolis['DATAOWNERCODE'] = 'KEOLIS'
df_keolis['MAAND'] = df_keolis['MAAND'].astype(str).apply(leading_zero)

print("Klaar met Keolis")
#%% EBS
PSA_tabel_ebs = PSA_tabel.loc[PSA_tabel['DATAOWNERCODE'] == 'EBS'].set_index('USERSTOPCODE')['QUAYCODE'].to_dict()
chb_ebs_dict = df_chb.loc[df_chb['QUAYCODE'].isin(PSA_tabel_ebs.values())].set_index('QUAYCODE')['STOPPLACECODE'].to_dict()
#maak een los dataframe om de gegevens van EBS in te plaatsen
df_ebs = pd.DataFrame()

# de bestanden voor KEOLIS zitten in een map per maand
for concessiefile in os.listdir(os.path.join(G01_folder,'EBS')):
    if not concessiefile.split('_')[1] == filter_jaar:
        continue
    print(concessiefile)
    #per modaliteit
    # in de tabel staat een rij met de totalen per uurblok per lijn voor een maand
    df_concessie = pd.read_csv(os.path.join(G01_folder, 'EBS', concessiefile),
                               encoding='latin-1',sep=";", dtype = dtype_g01)
    df_concessie['HNR'] = df_concessie['HNR'].str.strip()
    df_concessie['LN_ID_OV_MIJ'] = df_concessie['LN_ID_OV_MIJ'].str.strip()
    df_concessie = tel_vakantie_en_niet_vakantie_op(df_concessie)
    df_concessie.loc[df_concessie['HNR'].map(PSA_tabel_ebs).notna(),'QUAYCODE'] = df_concessie['HNR'].map(PSA_tabel_ebs)
    df_concessie['STOPPLACECODE'] = df_concessie['QUAYCODE'].map(chb_ebs_dict)
    
    df_ebs = pd.concat([df_ebs,df_concessie])

# om later ook bestanden op publiekslijn mogelijk te maken voen
# lijn = pd.concat([pd.read_csv("C:\data\kv1\{}\LINEXXXXXX.TMI".format(x), sep = '|',dtype={'[LinePlanningNumber]':str}) for x in ['KEOLIS','SYNTUS']]) 
# lijn = lijn.set_index('[LinePlanningNumber]')['[LinePublicNumber]'].to_dict()

# df_keolis.loc['LIJN'] = df_keolis['LN_ID_OV_MIJ'].replace(lijn)
  
df_ebs = df_ebs.rename(columns={'HNR':'HALTECODE','NR_CONS_GEB':'CONCESSIE'})
df_ebs.loc[df_ebs['CONCESSIE'].isin(['17','301','302','303']),'CONCESSIE'] =  '300'
df_ebs['DATAOWNERCODE'] = 'EBS'
df_ebs['MAAND'] = df_ebs['MAAND'].astype(str).apply(leading_zero)

print("Klaar met EBS")
#%% ARRIVA LLS
df_arr_lls = pd.DataFrame()

if filter_jaar != '2022':
    PSA_tabel_arriva = PSA_tabel.loc[PSA_tabel['DATAOWNERCODE'] == 'ARR'].set_index('USERSTOPCODE')['QUAYCODE'].to_dict()
    PSA_tabel_arriva['62571270'] = 'NL:Q:62571270'
    chb_arriva_dict = df_chb.loc[df_chb['QUAYCODE'].isin(PSA_tabel_arriva.values())].set_index('QUAYCODE')['STOPPLACECODE'].to_dict()
    

    for g01_file in os.listdir(os.path.join(G01_folder,'Lelystad')):
        if g01_file.split('_')[1] == filter_jaar:
            print(g01_file)       
            df_arr_lls_file = pd.read_csv(os.path.join(G01_folder,'Lelystad',g01_file), sep=';', dtype = dtype_g01)
    
            df_arr_lls_file['QUAYCODE'] = df_arr_lls_file['HNR'].replace(PSA_tabel_arriva)
            df_arr_lls_file['STOPPLACECODE'] = df_arr_lls_file['QUAYCODE'].replace(chb_arriva_dict)
            
            df_arr_lls_file = tel_vakantie_en_niet_vakantie_op(df_arr_lls_file)
            
            #data aanvulling en correctie
            df_arr_lls_file['CONCESSIE'] = 'Lelystad'
            df_arr_lls_file['DATAOWNERCODE'] = 'ARR'  
            df_arr_lls_file['MAAND'] = df_arr_lls_file['MAAND'].astype(str).apply(leading_zero)
            df_arr_lls_file['STOPPLACECODE'] = df_arr_lls_file['STOPPLACECODE'].replace({'5309':'NL:S:49005400','5330':'NL:S:49001210',
                                '5362':'NL:S:49000570','17070':'NL:S:49000630','17069': 'NL:S:49000620'})
            df_arr_lls_file = df_arr_lls_file.loc[df_arr_lls_file['STOPPLACECODE']!='-1']
            df_arr_lls = pd.concat([df_arr_lls,df_arr_lls_file])
            
    if df_arr_lls.empty:
        df_arr_lls = pd.DataFrame(columns= g01_kolommen)
    else:
        koppeling_arriva = pd.read_excel(r"C:\data\O10\Arriva Koppeling.xlsx",
                    dtype= {'HNR':str}, usecols=['HNR', 'stopplacecode']).dropna().set_index('HNR')['stopplacecode'].to_dict()
    
        df_arr_lls['STOPPLACECODE'] = df_arr_lls['STOPPLACECODE'].astype(str).replace(koppeling_arriva)
    print("Klaar met Arriva Lelystad")
#%% ARRIVA AR 2019 heeft O10 formaat
# def read_arriva_o10(concessie):
#     dtype = {'UURBLOK':str,'LN_ID_OV_MIJ':str,'JAAR':str, 'MAAND':str, 'HALTECODE_HERKOMST':str, 'HALTECODE_BESTEMMING':str}
#     df = pd.read_excel(r"C:\data\O19\201911_O10_{}.xlsx".format(concessie), dtype=dtype)
#     df = df.rename(columns={'Uurblok':'UURBLOK','Ritten':'RITTEN','LN_ID_OV_MIJ':'LIJN'})        
#     df['CONCESSIE'] = concessie
    
#     uurblok_vertalen =    {'05:00-05:59':'05-06', '06:00-06:59':'06-07', '07:00-07:59':'07-08', '08:00-08:59':'08-09',
#         '09:00-09:59':'09-10', '10:00-10:59':'10-11', '11:00-11:59':'11-12', '12:00-12:59':'12-13', '13:00-13:59':'13-14',
#         '14:00-14:59':'14-15', '15:00-15:59':'15-16', '16:00-16:59':'16-17', '17:00-17:59':'17-18', '18:00-18:59':'18-19',
#         '19:00-19:59':'19-20', '20:00-20:59':'20-21', '21:00-21:59':'21-22', '22:00-22:59':'22-23', '23:00-23:59':'23-24'}    
#     df.loc[df['UURBLOK'].map(uurblok_vertalen).notna(), 'UURBLOK'] = df['UURBLOK'].map(uurblok_vertalen)
#     return df

# if '2019' in filter_jaar:
#     df = pd.concat([read_arriva_o10(concessie) for concessie in ['AR', '99']])
    
#     #arriva heeft eigen placecodes
#     koppeling_arriva = pd.read_excel(r"C:\data\O19\Arriva Koppeling.xlsx").rename(columns={'HALTE_HERKOMST':'PLACECODE'})
#     koppeling_arriva = koppeling_arriva.loc[koppeling_arriva['Stopplacecode'].notna()].set_index('PLACECODE')['Stopplacecode'].to_dict()#    
    
#     df['DAGTYPE'] = df['DAGTYPE'].map({'WEEKDAG':'WERK','ZATERDAG':'ZA','ZONDAG':'ZO'})
#     df['HALTECODE_HERKOMST'] = df['HALTE_HERKOMST'].replace(koppeling_arriva)   
#     df['HALTECODE_BESTEMMING'] = df['HALTE_BESTEMMING'].replace(koppeling_arriva)   
#     df_arriva = O10_to_G01(df.copy()).reset_index()
#     df_arriva['DATAOWNERCODE'] = 'ARR'

# print("Klaar met Arriva Achterhoek Rivierenland")
#%% Arriva versie 2

def arr_to_G01(df_arriva):    
    df_instappers = pd.DataFrame(df_arriva.groupby(['CONCESSIE','JAAR','MAAND','DAGTYPE','HALTE'])['CHECK INS'].sum())
    df_instappers.index = df_instappers.index.rename(['CONCESSIE','JAAR','MAAND','DAGTYPE','HALTE'])
    df_instappers = df_instappers.rename(columns = {'CHECK INS':'INSTAP'})                         
    
    df_uitstappers = pd.DataFrame(df_arriva.groupby(['CONCESSIE','JAAR','MAAND','DAGTYPE','HALTE'])['CHECK OUTS'].sum())
    df_uitstappers.index = df_uitstappers.index.rename(['CONCESSIE','JAAR','MAAND','DAGTYPE','HALTE'])
    df_uitstappers = df_uitstappers.rename(columns = {'CHECK OUTS':'UITSTAP'})     
    
    df_arriva_jaar = pd.concat([df_instappers, df_uitstappers], axis=1)
    df_arriva_jaar = df_arriva_jaar.unstack('DAGTYPE')
    cols = ['_'.join(col) for col in df_arriva_jaar.columns]
    df_arriva_jaar.columns = cols
    df_arriva_jaar = df_arriva_jaar.reset_index()
    return df_arriva_jaar     

dtype_arr = {'Jaar':str,'Maand':str, 'MAAND':str, 'JAAR':str, 'HNR':str}

df_arr_g01 = pd.DataFrame()

if filter_jaar in ['2019','2020','2021']:
    try:                  
        tic=timeit.default_timer()
    
        df_arr = pd.read_excel(os.path.join(G01_folder,'Arriva',r'in-_en_uitstappers_per_halte_{} (G01) bus.xlsx'.format(
                    filter_jaar)),dtype=dtype_arr, sheet_name='G01_ovc')
        toc=timeit.default_timer()
    
    
        print(f'Arriva gelezen in {toc - tic}')
        
        df_arr = df_arr.rename(columns={"Subconcessie":'CONCESSIE'})
        df_arr.columns = df_arr.columns.str.upper()
        if filter_jaar == '2022':
            df_arr_g01 = tel_vakantie_en_niet_vakantie_op(df_arr)
        else: 
            df_arr['DAGTYPE'] = df_arr['KALENDERDAG'].map({'Ma':'WERK','Di':'WERK', 'Wo':'WERK', 'Do':'WERK', 'Vr':'WERK', 'Za':'ZA', 'Zo':'ZO'})    
            df_arr.loc[df_arr['DATUM'].isin(feestzondagen[filter_jaar]),'DAGTYPE'] = 'ZO'
            df_arr.loc[df_arr['DATUM'].isin(feestzaterdagen[filter_jaar]),'DAGTYPE'] = 'ZA'
            df_arr_g01 = arr_to_G01(df_arr)
        
        if filter_jaar == '2022':
            df_arr_g01['QUAYCODE'] = df_arr_g01['HNR'].replace(PSA_tabel_arriva)
            df_arr_g01['STOPPLACECODE'] = df_arr_g01['QUAYCODE'].replace(chb_arriva_dict)
            koppeling_arriva = pd.read_excel(r"C:\data\O10\Arriva Koppeling.xlsx",
                    dtype= dtype_arr, usecols=['HNR', 'stopplacecode']).dropna().set_index('HNR')['stopplacecode'].to_dict()
            df_arr_g01['STOPPLACECODE'] = df_arr_g01['STOPPLACECODE'].astype(str).replace(koppeling_arriva)
        else: 
            df_arr_g01['STOPPLACECODE'] = df_arr_g01['HALTE'].replace(koppeling_arriva)
        df_arr_g01['DATAOWNERCODE'] = 'ARR'
        df_arr_g01['CONCESSIE'] = 'ACH-RIV'
        # in plaats van '1' '01' als maandnummer
        df_arr_g01['MAAND'] = df_arr_g01['MAAND'].apply(leading_zero)
        # een paar aparte haltes in de data die we niet meenemen.
        df_arr_g01 = df_arr_g01.loc[~df_arr_g01['HALTE'].isin(['Concessiegrens Twente - Achterhoek, Concessiegrens'
                        'Mp Leonard','Kdl Europarcs','Kdl de Zandmeren', 'Unknown'])]
        df_arr_g01 = df_arr_g01.loc[~df_arr_g01['HNR'].isin(['44310020'])]
        
    except FileNotFoundError:
        df_arr_g01 = pd.DataFrame(columns= g01_kolommen)
        print(f'Gegevens voor {filter_jaar} niet beschikbaar bij Arriva')



#%% Arriva per quaycode
koppeling_arriva = pd.read_excel(r"C:\data\O10\Arriva Koppeling.xlsx",
        dtype= dtype_arr, usecols=['code_lang', 'stopplacecode']).dropna().set_index('code_lang')['stopplacecode'].to_dict()

def verdeel_instappers_over_quays(df_stopplaces, df_chb):
    pass
df_arr_22 = pd.DataFrame()

if filter_jaar == '2022':

    for concessie in ['LLS', 'ACH', 'RIV']: #, 
        df_conc = pd.read_excel(os.path.join(G01_folder,'Arriva',f'G01_{concessie}_{filter_jaar}_quay.xlsx'), dtype=dtype_arr)
        df_conc = tel_vakantie_en_niet_vakantie_op(df_conc)
        df_conc['DATAOWNERCODE'] = 'ARR'
        df_conc['CONCESSIE'] = concessie
        df_conc['HNR'] = df_conc['HNR'].fillna('0')
        df_conc['MAAND'] = df_conc['MAAND'].apply(leading_zero)
        # if concessie == 'LLS':
        #     df_conc['HNR'] = df_conc['HNR'].apply(lambda x: f'NL:S:{x}')
        # else:
        #     pass
        df_conc_q = df_conc.loc[df_conc['HNR'].str.contains('NL:Q:')].rename(columns={'HNR':'QUAYCODE'})  
        
        df_conc_q['QUAYCODE'] = df_conc_q['QUAYCODE'].replace({'NL:Q:49005921':'NL:Q:49005920', 'NL:Q:49000820':'NL:Q:49005400', 'NL:Q:49110020':'NL:Q:49110010',
                                                               'NL:Q:49000730':'NL:Q:49000740', 'NL:Q:49110310':'NL:Q:49110300','NL:Q:49000420':'NL:Q:49001210',
                                                               'NL:Q:49005091':'NL:Q:49005020','NL:Q:49000580':'NL:Q:49000590'})     
        
        df_conc_q = df_conc_q.merge(df_chb[['STOPPLACECODE','QUAYCODE']],on='QUAYCODE', how='left')

        df_conc_s = df_conc.loc[df_conc['HNR'].str.contains('NL:S:')].rename(columns={'HNR':'STOPPLACECODE'})         
        
        #als haltenummer is korter dan len(NL:S:XXXXXXXX)
        df_conc_s_v = df_conc_s.loc[df_conc_s['STOPPLACECODE'].apply(lambda x: len(x)<13)].index
        
        print(df_conc_s.loc[df_conc_s_v].loc[~df_conc_s['HALTE'].isin(koppeling_arriva.keys()), 'HALTE'].unique())
        #vervang het door de koppeltabel
        df_conc_s.loc[df_conc_s_v,'STOPPLACECODE'] = df_conc_s.loc[df_conc_s_v,'HALTE'].replace(koppeling_arriva)
        
        #verdeel in+uitstappers stopplace over quays
        df_conc_s = df_conc_s.merge(df_chb[['STOPPLACECODE','QUAYCODE']],on='STOPPLACECODE', how='left')

        aantal_quays = df_conc_s.groupby('STOPPLACECODE')['QUAYCODE'].nunique()
        aantal_quays.name = 'AANTAL_QUAYS'
        df_conc_s = df_conc_s.merge(aantal_quays,left_on='STOPPLACECODE', right_index=True)
        for kolom in reizigerskolommen:
            df_conc_s[kolom] = df_conc_s[kolom]/ df_conc_s['AANTAL_QUAYS']
        
        df_arr_22 = pd.concat([df_arr_22, df_conc_s, df_conc_q])
    df_arriva = df_arr_22
    
#%%
if filter_jaar in ['2023']: 
    df_arriva = pd.read_csv(r"C:\data\G01\Arriva\20240328_G01_ARRIVA_AHRV_G01_2023.csv", dtype=dtype_g01, sep=';')
    
    #filter op treinconcessie
    df_arriva = df_arriva.loc[df_arriva['NR_CONS_GEB'].isin(['20','23'])]
    df_arriva = df_arriva.loc[df_arriva['MAAND']!='0']
    df_arriva['DATAOWNERCODE'] = 'ARR'
    df_arriva = df_arriva.rename(columns={'NR_CONS_GEB':'CONCESSIE'})
    
df_arriva = tel_vakantie_en_niet_vakantie_op(df_arriva)
df_arriva['QUAYCODE'] = df_arriva['HNR'].replace(PSA_tabel_arriva)
df_arriva['STOPPLACECODE'] = df_arriva['QUAYCODE'].replace(chb_arriva_dict)
      

# df_conc_s.loc[df_conc_s_v].loc[~df_conc_s['HALTE'].isin(koppeling_arriva.keys()), 'HALTE'].to_csv('haltes lls.csv')
#%% CXX

concessie_selectie = ['FL_IJM','OV_IJM'] #,'SAN','VZ'
dtype_cxx = {'Halte herkomst':str, 'Halte bestemming':str, 'Postcode herkomst':str,
       'Postcode bestemming':str, 'Haltecode herkomst':str, 'Haltecode bestemming':str,
       'Lijn':str, 'Uurblok':str, 'Ritten':float, 'Transactiewaarde (inc. btw)':str,
       'Kilometers':float}

connexxion =  pd.DataFrame()

PSA_tabel_CXX = PSA_tabel.loc[PSA_tabel['DATAOWNERCODE'] == 'CXX'].set_index('USERSTOPCODE')['QUAYCODE'].to_dict()
chb_CXX_dict = df_chb.loc[df_chb['QUAYCODE'].isin(PSA_tabel_CXX.values())].set_index('QUAYCODE')['STOPPLACECODE'].to_dict()

for concessiefile in os.listdir(os.path.join(HB_folder,'CXX')):
    
    concessie = concessiefile.split('.')[0].split(' ')[1]

    if not concessie in concessie_selectie:
        continue
    
    dagtype = concessiefile.split('.')[0].split(' ')[-1]
    jaar = concessiefile.split('.')[0].split(' ')[-2].split('-')[0]
    if not jaar == filter_jaar:
        continue
    print(concessiefile)
    maand = concessiefile.split('.')[0].split(' ')[-2].split('-')[1]
    df = pd.read_csv(os.path.join(HB_folder, 'CXX', concessiefile),
                     encoding='latin-1', sep=';', decimal=',') #dtype=dtype_cxx,
    df['Ritten'] = df['Ritten'].astype(float)
    
    #NAAR O10    
    df = df.rename(columns={'Haltecode herkomst':'HALTECODE_HERKOMST',
           'Haltecode bestemming':'HALTECODE_BESTEMMING','Uurblok':'UURBLOK','Ritten':'RITTEN','LN_ID_OV_MIJ':'LIJN'})        
    df['UURBLOK'] = df['UURBLOK'].replace(' ','')
    df['CONCESSIE'] = concessie
    df['DAGTYPE'] = dagtype
    df['JAAR'] = jaar
    df['MAAND'] = maand
    
    #omzetten naar G01
    df = O10_to_G01(df).reset_index()
    
    df['QUAYCODE'] = df['HALTECODE'].map(PSA_tabel_CXX)
    df['STOPPLACECODE'] = df['QUAYCODE'].map(chb_CXX_dict)
    connexxion = pd.concat([connexxion, df])

connexxion = connexxion.reset_index()
connexxion['DATAOWNERCODE'] = 'CXX'
if filter_jaar == '2022':
    connexxion['CONCESSIE'] = connexxion['CONCESSIE'].replace({'VZ':'304'})
print("Klaar met Connexxion")

#%% totaal tabel
df_lijst = [df_keolis, df_ebs, df_arriva, df_arr_g01, df_arr_22, df_arr_lls, connexxion]
dflijst_samenvoegen = [x[g01_kolommen] for x in df_lijst if not x.empty]
df_totaal = pd.concat(dflijst_samenvoegen)

print('Vervoerders samengevoegd')

#%% naar daggemiddelden
def jaar_totaal_naar_gemiddelde(df):
    df[['INSTAP_WERK', 'UITSTAP_WERK']] = df[['INSTAP_WERK', 'UITSTAP_WERK']]/dagen_per_type[filter_jaar]['WERK']
    df[['INSTAP_ZA', 'UITSTAP_ZA']] = df[['INSTAP_ZA', 'UITSTAP_ZA']]/dagen_per_type[filter_jaar]['ZA']
    df[['INSTAP_ZO', 'UITSTAP_ZO']] = df[['INSTAP_ZO', 'UITSTAP_ZO']]/dagen_per_type[filter_jaar]['ZO']
    return df

#maandfactoren, voor het berekenen van jaargemiddelden voor concessies waarbij niet alle maanden aanwezig zijn
factoren = {}
for concessie, df_group in df_totaal.groupby('CONCESSIE'):
    if {'01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'
     }.issubset(set(df_group['MAAND'].to_list())):
        df_group = df_group.groupby('MAAND').sum()
        factoren[concessie] = df_group[reizigerskolommen] / df_group[reizigerskolommen].sum()

maandfactoren = pd.concat(factoren.values()).groupby(level=0).mean()
def bepaling_per_halte(level):    
    df_per_halte_per_concessie = pd.DataFrame()
    
    # bepaal het jaargemiddelde per concessie voor een halte
    # variant bepalen per concessie. Als je dit per halte doet dan ga je te veel corrigeren als er eens een maandje niemand op een halte instapt.
    for concessie, df_tot_conc in df_totaal.groupby('CONCESSIE'):
        maandset = {'01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'}
        df_group_x = None
    
        # jaargemiddelde is nog niet aanwezig bij bushaltes
        if 'YY' in df_tot_conc['MAAND'].to_list():
            df_group_o = df_tot_conc.loc[df_group['MAAND']=='YY'].groupby([level])[reizigerskolommen].sum()
            return df_group_o
        
        # jaartotaal
        if 'XX' in df_tot_conc['MAAND'].to_list():
            print(f'Concessie {concessie} heeft variant jaartotaal')
            df_group_x = df_tot_conc.loc[df_tot_conc['MAAND']=='XX']
            df_group_x = df_group_x.groupby([level,'CONCESSIE'])[reizigerskolommen].sum()
            df_group_x = jaar_totaal_naar_gemiddelde(df_group_x)
            df_group_o = df_group_x
            
        #alle maanden zijn aanwezig    
        if maandset.issubset(set(df_tot_conc['MAAND'].to_list())):
            print(f'Concessie {concessie} heeft variant alle maanden')
            df_tot_conc = df_tot_conc.loc[df_tot_conc['MAAND'].isin(maandset)]
            df_group_m = df_tot_conc.groupby([level,'CONCESSIE'])[reizigerskolommen].sum()
            df_group_m = jaar_totaal_naar_gemiddelde(df_group_m)
            #voor SAN moet je de twee varianten optellen
            if df_group_x is not None:
                df_group_o =  df_group_m.add(df_group_x, fill_value=0)
            else:
                df_group_o = df_group_m 
        #ander is er een deel van de maanden
        elif ('YY' not in df_tot_conc['MAAND'].to_list()) & ('XX' not in df_tot_conc['MAAND'].to_list()):
            print(f'Concessie {concessie} heeft variant een of meer maanden')
            print(set(df_tot_conc['MAAND'].to_list()))
            df_group_o = df_tot_conc.set_index('MAAND')
            df_group_o[reizigerskolommen] = df_group_o[reizigerskolommen]/maandfactoren.loc[df_group_o.index]
            df_group_o = df_group_o.groupby([level,'CONCESSIE'])[reizigerskolommen].mean().fillna(0)
            # if len(df_group_o.shape)>1:
            #     df_group_o = df_group_o.iloc[0]
            df_group_o = jaar_totaal_naar_gemiddelde(df_group_o)
        df_group_o[reizigerskolommen] = df_group_o[reizigerskolommen].astype(float).round(1)
        df_per_halte_per_concessie = pd.concat([df_per_halte_per_concessie,df_group_o.reset_index()])
    
    #tel alle concessies bij elkaar op.
    df_per_halte = df_per_halte_per_concessie.groupby(level)[reizigerskolommen].sum()
    
    print('Reizigers per halte en per concessie bepaald')

    #VOEG NAAM EN PLAATS VAN STOPPLACE TOE op basis van chb
    df_per_halte_chb = df_per_halte.merge(df_chb[[level,'NAME','TOWN']].drop_duplicates(level), on=[level], how='left')
    
    df_per_halte_chb[[level,'NAME','TOWN']+reizigerskolommen].to_csv(f'../instappers per {level.lower()}/instappers per {level.lower()} {filter_jaar}.csv',sep=';', decimal=',', index=False)
    # voeg coördinaten toe (gemiddelde van quays per stopplace, afgerond op 1 decimaal (0.1m))
    # stopplace_coordinates = df_chb.groupby(level)[['RD-X','RD-Y']].mean().round(1).reset_index()
    # df_per_halte_coord = df_per_halte_chb.merge(stopplace_coordinates, on=level)
    # df_per_halte_coord.to_csv(f'../instappers per halte/instappers per halte {filter_jaar} met coordinaten.csv', sep=';', index=False)
    print(f'Reizigers per {level.lower()} en per concessie bepaald')
    return df_per_halte_chb
    

df_instappers = bepaling_per_halte('STOPPLACECODE') 
df_instappers = bepaling_per_halte('QUAYCODE') 
