# -*- coding: utf-8 -*-
"""
Created on Mon Mar  8 09:41:34 2021

Analyse van aantal in- en uitstappers voor treinstations.

Gegevens van format G01 voor Keolis

Voor Arriva een HB-log

Voor NS de instapperstabellen

XX = jaartotaal
YY = jaargemiddelde

@author: kuinr01
"""
import pandas as pd
import os
import calendar
import geopandas as gpd

from folders import G01_folder, PMR_folder, HB_folder
#%% 
def tel_vakantie_en_niet_vakantie_op(df):              
    df['INSTAP_WERK'] = df[['INSTAP_WERK_NIETVAK','INSTAP_WERK_VAK']].sum(axis=1) 
    df['UITSTAP_WERK'] = df[['UITSTAP_WERK_NIETVAK','UITSTAP_WERK_VAK']].sum(axis=1)
   
    df['INSTAP_ZA'] = df[['INSTAP_ZA_NIETVAK','INSTAP_ZA_VAK']].sum(axis=1)
    df['UITSTAP_ZA'] = df[['UITSTAP_ZA_NIETVAK', 'UITSTAP_ZA_VAK']].sum(axis=1)
   
    df['INSTAP_ZO'] = df[['INSTAP_ZO_NIETVAK','INSTAP_ZO_VAK']].sum(axis=1)
    df['UITSTAP_ZO'] = df[['UITSTAP_ZO_NIETVAK', 'UITSTAP_ZO_VAK']].sum(axis=1)
    return df 


def tel_vakantie_en_niet_vakantie_op_yy(df,dagen_per_subtype, filter_jaar):
    for plek in ['INSTAP', 'UITSTAP']:        
        for dagtype in ['WERK', 'ZA', 'ZO']:
            df[f'{plek}_{dagtype}'] = df[f'{plek}_{dagtype}_NIETVAK'] * dagen_per_subtype[filter_jaar]['NIETVAK'][dagtype] + df[f'{plek}_{dagtype}_VAK'] * dagen_per_subtype[filter_jaar]['VAK'][dagtype]
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



def corrigeer(a):
    if len(a)==1:
        return '0'+a
    else:
        return a
#%% variabelen
output_kolommen = ['NR_CONS_GEB', 'JAAR', 'MAAND', 'INSTAP_WERK', 'UITSTAP_WERK', 'INSTAP_ZA',
       'UITSTAP_ZA', 'INSTAP_ZO', 'UITSTAP_ZO']

#in deze sheet staan de haltenummers van de stations
stations = pd.read_excel('../Stationscodes.xlsx', dtype={'HNR':str})
stationslijst = pd.read_excel(os.path.join(PMR_folder,'stationslijst.xlsx'))
stationslijst = stationslijst.loc[stationslijst['Provincie'].isin(['Drenthe','Overijssel','Gelderland','Flevoland']) | stationslijst['Station'].isin(['Amersfoort Centraal'])]['Station']

concessie_dict = {103:'TZUHO', 106:'TZWKA', 111:'TZWEN',113:'TENGR'}

dagen_per_type = {'2015': {'WERK':258, 'ZA':52, 'ZO':58},
                  '2016': {'WERK':255, 'ZA':54, 'ZO':57},
                  '2017': {'WERK':254, 'ZA':53, 'ZO':58},
                  '2018': {'WERK':255, 'ZA':53, 'ZO':57},
                  '2019': {'WERK':255, 'ZA':52, 'ZO':58},
                  '2020': {'WERK':255, 'ZA':51, 'ZO':60},
                  '2021': {'WERK':256, 'ZA':52, 'ZO':57},
                  '2022': {'WERK':256, 'ZA':53, 'ZO':57}}

dagen_per_subtype = {'2015': {'NIETVAK': {'WERK': 130, 'ZA': 28,'ZO': 31},
                           'VAK': {'WERK':124, 'ZA': 25, 'ZO': 27}},
                    '2016': {'NIETVAK': {'WERK': 130, 'ZA': 28,'ZO': 31},
                           'VAK': {'WERK':124, 'ZA': 25, 'ZO': 27}},
                    '2017': {'NIETVAK': {'WERK': 130, 'ZA': 28,'ZO': 31},
                           'VAK': {'WERK':124, 'ZA': 25, 'ZO': 27}},       
                    '2018': {'NIETVAK': {'WERK': 130, 'ZA': 28,'ZO': 31},
                           'VAK': {'WERK':124, 'ZA': 25, 'ZO': 27}},
                    '2019': {'NIETVAK': {'WERK': 225, 'ZA': 46,'ZO': 52},
                           'VAK': {'WERK':30, 'ZA': 6, 'ZO': 6}
                           },
                    '2020':{'NIETVAK': {'WERK': 108, 'ZA': 21,'ZO': 30},
                            'VAK': {'WERK':105, 'ZA': 21, 'ZO': 21}
                            } ,
                    '2021':{'NIETVAK': {'WERK': 208, 'ZA': 44,'ZO': 46},
                            'VAK': {'WERK':48, 'ZA': 8, 'ZO': 11}},
                      '2022':{'NIETVAK': {'WERK': 88, 'ZA': 18,'ZO': 18},
                            'VAK': {'WERK':167, 'ZA': 35, 'ZO': 39}
                            }
                   }
#%% Input
filter_jaar = '2021'
#%% KEOLIS / SYNTUS

dtype_keolis = {'HNR':str,'JAAR':str, 'MAAND':str}
df_keolis = pd.DataFrame()

filter_jaar_keolis = filter_jaar[2:4]


for maandfolder in os.listdir(os.path.join(G01_folder,'KEOLIS')):
    # print(jaarfolder)
    if maandfolder.split(' ')[2][0:2] != filter_jaar_keolis:
        # print('Overgeslagen')
        continue

    print(maandfolder)
    for concessiefile in os.listdir(os.path.join(G01_folder,'KEOLIS', maandfolder)):
        if not concessiefile.split('_')[0] in str(concessie_dict.keys()):
            continue
        print(concessiefile)
        
        # in de tabel staat een rij met de totalen per uurblok per lijn voor een maand
        df_concessie = pd.read_csv(os.path.join(G01_folder,'KEOLIS', 
                    maandfolder,concessiefile), encoding='latin-1',sep=";", dtype = dtype_keolis)
        df_concessie['HNR'] = df_concessie['HNR'].str.strip()
        
        # # vanaf 2017 zijn er pas haltenamen toegevoegd. daarom wordt gefilterd op haltenummer
        # df_concessie  = df_concessie.loc[df_concessie['HNR'].isin(stations['HNR'])]
        #als er geen rijen door de selectie komen, sla de rest van de loop over
        # if len(df_concessie) == 0:
        #     continue

        #per modaliteit
        som = df_concessie.groupby(['NR_CONS_GEB','HNR'])[['INSTAP_WERK_NIETVAK', 'INSTAP_ZA_NIETVAK',
        'INSTAP_ZO_NIETVAK', 'INSTAP_WERK_VAK', 'INSTAP_ZA_VAK',
        'INSTAP_ZO_VAK', 'UITSTAP_WERK_NIETVAK', 'UITSTAP_ZA_NIETVAK',
        'UITSTAP_ZO_NIETVAK', 'UITSTAP_WERK_VAK', 'UITSTAP_ZA_VAK',
        'UITSTAP_ZO_VAK']].sum()
        
        # omdat je sommeert moet je nog wat terugzetten
        jaar = df_concessie.iloc[0]['JAAR']
        som['JAAR'] = jaar
        maand = df_concessie.iloc[0]['MAAND']
        som['MAAND'] = maand
        
        som = tel_vakantie_en_niet_vakantie_op(som)
 
        df_keolis = df_keolis.append(som.reset_index()[['NR_CONS_GEB','HNR','JAAR', 'MAAND','INSTAP_WERK', 'UITSTAP_WERK', 'INSTAP_ZA',
        'UITSTAP_ZA', 'INSTAP_ZO', 'UITSTAP_ZO']], ignore_index=True)

df_keolis['STATION'] = df_keolis['HNR'].map(stations.set_index('HNR')['STATION'].to_dict())
df_keolis['NR_CONS_GEB'] = df_keolis['NR_CONS_GEB'].map(concessie_dict)
df_keolis = df_keolis.drop(columns='HNR').rename(columns={'NR_CONS_GEB':'CONCESSIE'})
df_keolis['DATAOWNERCODE'] = 'KEOLIS'
df_keolis['MAAND'] = df_keolis['MAAND'].apply(corrigeer)

#%% NS
print('NS')
ns_instappers = pd.read_excel(os.path.join(PMR_folder,"Stationsinstappers\instappers per jaar.xlsx"))
ns_instappers = ns_instappers.loc[ns_instappers['Station'].isin(stationslijst)]
ns_instappers = ns_instappers.set_index('Station').stack() / 2
ns_instappers.name = 'INSTAP_WERK'
ns_instappers = pd.DataFrame(ns_instappers).reset_index().rename(columns={'level_1':'JAAR','Station':'STATION'})

ns_instappers['JAAR'] = ns_instappers['JAAR'].astype(str)
ns_instappers = ns_instappers.loc[ns_instappers['JAAR']==filter_jaar]

ns_instappers['UITSTAP_WERK'] = ns_instappers['INSTAP_WERK']
ns_instappers['CONCESSIE'] = 'NS'
ns_instappers['MAAND'] = 'YY'
ns_instappers['UURBLOK'] = 'XX'
ns_instappers['DATAOWNERCODE'] = 'NS'

#%% ARRIVA  
def O10_to_g1(arriva_hb):    
    df_instappers = pd.DataFrame(arriva_hb.groupby(['CONCESSIE','JAAR','MAAND','DAGTYPE','CI station'])['RITTEN'].sum())
    df_instappers.index = df_instappers.index.rename(['CONCESSIE','JAAR','MAAND','DAGTYPE','STATION'])
    df_instappers = df_instappers.rename(columns = {'RITTEN':'INSTAP'})                         
    
    df_uitstappers = pd.DataFrame(arriva_hb.groupby(['CONCESSIE','JAAR','MAAND','DAGTYPE','CO station'])['RITTEN'].sum())
    df_uitstappers.index = df_uitstappers.index.rename(['CONCESSIE','JAAR','MAAND','DAGTYPE','STATION'])
    df_uitstappers = df_uitstappers.rename(columns = {'RITTEN':'UITSTAP'})     
    
    df_arriva_jaar = pd.concat([df_instappers, df_uitstappers], axis=1)
    df_arriva_jaar = df_arriva_jaar.unstack('DAGTYPE')
    cols = ['_'.join(col) for col in df_arriva_jaar.columns]
    df_arriva_jaar.columns = cols
    return df_arriva_jaar

stationsdict_arr = {'Arnhem':'Arnhem Centraal',
                    'Ah Velperpoort':'Arnhem Velperpoort',
    'Apd De Maten':'Apeldoorn De Maten',
    'Dtc De Huet':'Doetinchem De Huet',
    'Hmn Dodewaard':'Hemmen-Dodewaard',
    'Ltv-Groenlo':'Lichtenvoorde-Groenlo'}

print('ARRIVA')
dtype_arr = {'jaarmaand':str}
df_arriva = pd.DataFrame()
for jaar in [filter_jaar]:
    arriva_hb = pd.DataFrame()
    for concessie in ['Vechtdal','GT']:
        try:                                            
            df_arr_concessie = pd.read_excel(r"C:\data\HB-log\{} - station HB-{}.xlsx".format(
            concessie, jaar), sheet_name='station (HB)', header = 3, dtype=dtype_arr)
            df_arr_concessie['CONCESSIE'] = concessie     
            arriva_hb = arriva_hb.append(df_arr_concessie)
        except FileNotFoundError:
            print(f'Data niet gevonden voor {concessie} {jaar}')
    if arriva_hb.empty:
        continue
    arriva_hb = arriva_hb.drop('Unnamed: 0', axis=1).rename(columns={"aantal CO's":'RITTEN',
                                                    "dagsoort":'DAGTYPE'})
    
    arriva_hb['JAAR'] = arriva_hb['jaarmaand'].apply(lambda x: x[0:4])
    arriva_hb['MAAND'] = arriva_hb['jaarmaand'].apply(lambda x: x[4:6])
    arriva_hb = arriva_hb.drop('jaarmaand', axis=1)
    arriva_hb['DAGTYPE'] = arriva_hb['DAGTYPE'].str.upper()
    
    #aantal per dagtype
    # arriva_hb['RITTEN'] = arriva_hb[['DAGTYPE','JAAR','MAAND','RITTEN']].apply(lambda x: per_dagtype(x[0], x[1], x[2], x[3]), axis=1)
    df_arriva_jaar = O10_to_g1(arriva_hb)
    
    df_arriva = df_arriva.append(df_arriva_jaar)
#aantal dagtypen

df_arriva = df_arriva.reset_index()
if not arriva_hb.empty:    
    df_arriva['STATION'] = df_arriva['STATION'].replace(stationsdict_arr)
    
    df_arriva['DATAOWNERCODE'] = 'ARR'
    
#%% ARRIVA GT 2019 heeft O10 formaat
# stationstabel = pd.read_excel("C:\data\O19\Stationstabel.xlsx")
# stationsdict = stationstabel.drop_duplicates(['[UserStopAreaCode]']).set_index('[UserStopAreaCode]')['[Name]'].to_dict()
# if '2019' == filter_jaar:
#     df = pd.read_excel(r"C:\data\O19\201911_O10_GT.xlsx", dtype=dtype_keolis)
#     df['CONCESSIE'] = 'GT'
#     df['CI station'] = df['HALTE_HERKOMST'].map(stationsdict)
#     df['CO station'] = df['HALTE_BESTEMMING'].map(stationsdict)
    
#     df['DAGTYPE'] = df['DAGTYPE'].map({'WEEKDAG':'WERK','ZATERDAG':'ZA','ZONDAG':'ZO'})
    
#     df2 = df.copy()
#     df3 = O10_to_g1(df2)
#     df3['DATAOWNERCODE'] = 'ARR'
#     df3 = df3.reset_index()

#%% CXX
dtype= {'Halte herkomst':str, 'Halte bestemming':str, 'Postcode herkomst':str,
       'Postcode bestemming':str, 'Haltecode herkomst':str, 'Haltecode bestemming':str,
       'Lijn':str, 'Uurblok':str, 'Ritten':float, 'Transactiewaarde (inc. btw)':str,
       'Kilometers':float}

cxx_userstops_stations = {'40615610':'Ede Centrum',	'40615630':'Ede-Wageningen',
	'40750650':'Lunteren', '40760610':'Barneveld Noord', '40760630':'Barneveld Centrum',
	'40760670':'Barneveld Zuid', '50429550':'Amersfoort Centraal', '50530600':'Hoevelaken'}

df_connexxion = pd.read_excel(os.path.join(G01_folder,f'CXX Valleilijn G01 {filter_jaar}-YY.xlsx'), header=2)
df_connexxion.iloc[:,range(2,len(df_connexxion.columns))] = df_connexxion.iloc[:,range(2,len(df_connexxion.columns))].replace('-',0)

kopregel = pd.read_excel(os.path.join(G01_folder,f'CXX Valleilijn G01 {filter_jaar}-YY.xlsx'), header=None, nrows=1).iloc[0].to_list()

kolommen = ['UURBLOK','STATION']
if 'Vakantie' in kopregel:    #vakantiedagen apart
    for vak in ['NIETVAK','VAK']:
        for dagtype in['WERK','ZA','ZO']:
           for gegeven in ['INSTAP','UITSTAP','BEZETTING']:
               kolommen.append(f'{gegeven}_{dagtype}_{vak}')
    df_connexxion.columns = kolommen
    df_connexxion = tel_vakantie_en_niet_vakantie_op_yy(df_connexxion,dagen_per_subtype, filter_jaar)
   
else:
    for dagtype in['WERK','ZA','ZO']: #zonder vakantiedagen gedefinieerd
           for gegeven in ['INSTAP','UITSTAP','BEZETTING']:
               kolommen.append(f'{gegeven}_{dagtype}')

    df_connexxion.columns = kolommen

df_connexxion = df_connexxion.loc[df_connexxion['UURBLOK']!='Totaal']
df_connexxion['JAAR'] = filter_jaar
df_connexxion['MAAND'] = 'YY'
df_connexxion['CONCESSIE'] = 'Valleilijn'
df_connexxion['DATAOWNERCODE'] = 'CXX'


df_connexxion['STATION'] = df_connexxion['STATION'].replace({'Amersfoort, Centraal':'Amersfoort Centraal',
            'Ede, Station Ede-Wageningen':'Ede-Wageningen', 'Hoevelaken, Station Hoevelaken':'Hoevelaken', 'Lunteren, Station Lunteren':'Lunteren'})
df_connexxion['STATION'] = df_connexxion['STATION'].apply(lambda x: x.replace(', Station',''))

df_connexxion = df_connexxion.loc[~df_connexxion['STATION'].isin(['Amersfoort, Aansluiting', 'Barneveld, Aansluiting'])]
print("Klaar met Connexxion")

#%% totaal tabel
reizigerskolommen = ['INSTAP_WERK', 'UITSTAP_WERK', 'INSTAP_ZA', 'UITSTAP_ZA',
                               'INSTAP_ZO', 'UITSTAP_ZO']
df_totaal = pd.DataFrame()
df_totaal = df_totaal.append(df_keolis)

# if '2019' in filter_jaar:
#     df_totaal = df_totaal.append(df3)
df_totaal = df_totaal.append(df_arriva)
df_totaal = df_totaal.append(df_connexxion)
df_totaal = df_totaal.sort_values(['STATION','JAAR','MAAND'])

df_totaal_incl_ns = df_totaal.copy().append(ns_instappers)
#%% naar daggemiddelden
def jaar_totaal_naar_gemiddelde(df):
    df[['INSTAP_WERK', 'UITSTAP_WERK']] = df[['INSTAP_WERK', 'UITSTAP_WERK']]/dagen_per_type[filter_jaar]['WERK']
    df[['INSTAP_ZA', 'UITSTAP_ZA']] = df[['INSTAP_ZA', 'UITSTAP_ZA']]/dagen_per_type[filter_jaar]['ZA']
    df[['INSTAP_ZO', 'UITSTAP_ZO']] = df[['INSTAP_ZO', 'UITSTAP_ZO']]/dagen_per_type[filter_jaar]['ZO']
    return df

#maandfactoren
try:
    factoren = {}
    for concessie, df_group in df_totaal.groupby(['CONCESSIE']):
        if {'01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'
         }.issubset(set(df_group['MAAND'].to_list())):
            df_group = df_group.groupby('MAAND').sum()
            factoren[concessie] = df_group[reizigerskolommen] / df_group[reizigerskolommen].sum()
    
    maandfactoren = pd.concat(factoren.values()).groupby(level=0).mean()
    maandfactoren.to_csv(f'maandfactoren {filter_jaar}.csv')
except:
    print('Geen maandfactoren te bepalen')
    pass

def jaargemiddeld_per_concessie(df_totaal):
    df_jaargemiddelde_per_station_concessie = pd.DataFrame()
    
    for concessie, df_group in df_totaal.groupby(['STATION','CONCESSIE']):  
        maandset = {'01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12'}
        df_group_x = None
    
        if 'YY' in df_group['MAAND'].to_list():
            df_group_o = df_group.groupby(['STATION','CONCESSIE'])[reizigerskolommen].sum()
            # return df_group_o
        
        if 'XX' in df_group['MAAND'].to_list():
            df_group_x = df_group.loc[df_group['MAAND']=='XX']
            df_group_x = df_group_x.groupby(['STATION','CONCESSIE'])[reizigerskolommen].sum()
            df_group_x = jaar_totaal_naar_gemiddelde(df_group_x)
            
            df_group_o = df_group_x
            
        if maandset.issubset(set(df_group['MAAND'].to_list())):
            df_group_m = df_group.loc[df_group['MAAND'].isin(maandset)]
            df_group_m = df_group_m.groupby(['STATION','CONCESSIE'])[['INSTAP_WERK', 'UITSTAP_WERK', 'INSTAP_ZA', 'UITSTAP_ZA',
                                    'INSTAP_ZO', 'UITSTAP_ZO']].sum()
            df_group_m = jaar_totaal_naar_gemiddelde(df_group_m)
            if df_group_x is not None:
                df_group_o =  df_group_m + df_group_x
            else:
                df_group_o = df_group_m 
        elif ('YY' not in df_group['MAAND'].to_list()) & ('XX' not in df_group['MAAND'].to_list()):
            df_group_o = df_group.set_index('MAAND')
            df_group_o[reizigerskolommen] = df_group_o[reizigerskolommen]/maandfactoren.loc[df_group_o.index]
            df_group_o = df_group_o.groupby(['STATION','CONCESSIE'])[reizigerskolommen].mean().fillna(0)
            # if len(df_group_o.shape)>1:
                
            #     df_group_o = df_group_o.iloc[0]
            df_group_o = jaar_totaal_naar_gemiddelde(df_group_o)
        df_group_o[reizigerskolommen] = df_group_o[reizigerskolommen].applymap(lambda x: round(x), na_action='ignore')
        
        df_jaargemiddelde_per_station_concessie = df_jaargemiddelde_per_station_concessie.append(df_group_o.reset_index())
    df_jaargemiddelde_per_station = df_jaargemiddelde_per_station_concessie.groupby(['STATION'])[reizigerskolommen].sum().reset_index()             
    return df_jaargemiddelde_per_station_concessie, df_jaargemiddelde_per_station
#%%
gem_per_station_concessie, gem_per_station = jaargemiddeld_per_concessie(df_totaal)

gem_per_station_concessie.to_csv(f'../Reizigers per station per concessie/Reizigers per station (cico) per concessie {filter_jaar}.csv', sep=',', decimal = '.', index=False)
# gem_per_station_concessie.to_excel(f'Reizigers per station (cico) per concessie {filter_jaar}.xlsx', index=False)

gem_per_station.to_csv(f'../Reizigers per station/Reizigers per station (cico) {filter_jaar}.csv', sep=',', index=False)
# gem_per_station.to_excel(f'Reizigers per station (cico) {filter_jaar}.xlsx', index=False)

df_per_station_concessie_ns, df_per_station_ns = jaargemiddeld_per_concessie(df_totaal_incl_ns)
# df_per_station_concessie_ns.to_csv(f'Reizigers per station (cico) per concessie incl. NS {filter_jaar}.csv', sep=',', decimal = '.', index=False)
df_per_station_ns.to_csv(f'../Reizigers per station inclusief NS/Reizigers per station inclusief NS (cico) {filter_jaar}.csv', sep=',', decimal = '.', index=False)
# df_per_station_ns.to_excel(f'Reizigers per station incl. NS (cico) incl. NS {filter_jaar}.xlsx', index=False)

#Kaart werkdagen
# df_per_station_concessie_ns['REIZIGERS'] = df_per_station_concessie_ns['INSTAP_WERK'] + df_per_station_concessie_ns['UITSTAP_WERK']

# df_reizigers = df_per_station_concessie_ns.groupby(['STATION','CONCESSIE'])['REIZIGERS'].mean().unstack('CONCESSIE')
# df_reizigers['TOTAAL'] = df_reizigers.sum(axis=1)
# df_reizigers.reset_index().to_csv(f'Werkdagreizigers per station (cico) incl. NS {filter_jaar}.csv', sep=';', index=False)