#%%
# Libraries
import pandas as pd
from pathlib import Path


#%% 
# Paths
maandtotalen = Path("J:/OBI/Obi_Data/mobiliteit/MIPOV/K07/EBS lijn-maandtotalen")
G01_folder = Path("J:/OBI/Obi_Data/mobiliteit/MIPOV/G01/EBS_2024")
G01_folder_opgehoogd = Path("J:/OBI/Obi_Data/mobiliteit/MIPOV/G01/EBS_opgehoogd")

#%%
# Constants
instap_kolommen = ['INSTAP_WERK_NIETVAK',
       'INSTAP_ZA_NIETVAK', 'INSTAP_ZO_NIETVAK', 'INSTAP_WERK_VAK',
       'INSTAP_ZA_VAK', 'INSTAP_ZO_VAK']
uitstap_kolommen = ['UITSTAP_WERK_NIETVAK',
       'UITSTAP_ZA_NIETVAK', 'UITSTAP_ZO_NIETVAK', 'UITSTAP_WERK_VAK',
       'UITSTAP_ZA_VAK', 'UITSTAP_ZO_VAK']
dtype_g01 = {
    'JAAR': 'int',
    'MAAND': 'str',
    'LN_ID_OV_MIJ': str,
    'NR_CONS_DEEL':str, 
    'HNR':str
}
#%%
def ophogen_data(df, instap_kolommen, uitstap_kolommen):
    """
    Function to load and preview  data.
    """

    return df  # The function is not implemented yet

def laad_instapperstotaal(jaar):
    """
    Function to load instapperstotalen data for a given year.
    """
    global instapperstotalen
    
    if jaar in instapperstotalen.keys():
            return instapperstotalen[jaar]
    else:
        print(f"Laad excel instapperstotalen voor jaar {jaar}")
        bestand = maandtotalen / f"YV Lijnmaandtotaal {jaar}.xlsx"
        df = pd.read_excel(bestand)
        df['LN_ID_OV_MIJ'] = df['Lijnnummer'].str.extract(r'\((\d+)\)', expand=False)
        df['JAAR'] = jaar
        df['MAAND'] = df['Exploitatiemaand'].str[-2:]
        instapperstotalen[jaar] = df
        return df
#%%
#NR_CONS_GEB;JAAR;MAAND;LN_ID_OV_MIJ;LYNCODE;RICHTING;VARIANT;HNR;HALTE;UURBLOK;INSTAP_WERK_NIETVAK;INSTAP_ZA_NIETVAK;INSTAP_ZO_NIETVAK;INSTAP_WERK_VAK;INSTAP_ZA_VAK;INSTAP_ZO_VAK;UITSTAP_WERK_NIETVAK;UITSTAP_ZA_NIETVAK;UITSTAP_ZO_NIETVAK;UITSTAP_WERK_VAK;UITSTAP_ZA_VAK;UITSTAP_ZO_VAK

# Global dictionary to store loaded instapperstotalen data
instapperstotalen = {}
#%%
if __name__ == "__main__":
    for file in G01_folder.glob("*.csv"):
        print(f"Processing file: {file.name}")
        df = pd.read_csv(file, sep=';', encoding='latin1', dtype= dtype_g01)
        df = df.rename(columns={'NR_CONS_DEEL': 'NR_CONS_GEB'})
        df['NR_CONS_GEB'] = df['NR_CONS_GEB'].fillna('300')
        start_kolommen = df.columns.tolist()
        df_som = df.groupby(['JAAR', 'MAAND', 'LN_ID_OV_MIJ'])[instap_kolommen].sum().sum(axis=1).reset_index()
        df_som = df_som.rename(columns={0: 'INSTAPPERS_G01'})

        jaar = df_som['JAAR'].iloc[0]
        maand = df_som['MAAND'].iloc[0]

        df_merged = pd.merge(df_som, laad_instapperstotaal(jaar),  on=['JAAR', 'MAAND', 'LN_ID_OV_MIJ'], how='left')
        df_merged['INSTAP_FACTOR'] = df_merged['Totaal'] / df_merged['INSTAPPERS_G01']
        gemiddelde_factor = df_merged['Totaal'].sum() / df_merged['INSTAPPERS_G01'].sum()
        print(f"Totaal misebs: {df_merged['Totaal'].sum()}")
        print(f"Totaal export: {df_merged['INSTAPPERS_G01'].sum()}")
        df_merged.loc[df_merged['INSTAP_FACTOR'].isna(), 'INSTAP_FACTOR'] = gemiddelde_factor
        df_merged.loc[df_merged['INSTAP_FACTOR']==float('inf'), 'INSTAP_FACTOR'] = gemiddelde_factor

        print(f"Gemiddelde instap factor voor {jaar}-{maand}: {gemiddelde_factor}")

        df = pd.merge(df, df_merged[['JAAR', 'MAAND', 'LN_ID_OV_MIJ', 'INSTAP_FACTOR']],
                       on=['JAAR', 'MAAND', 'LN_ID_OV_MIJ'], how='left')

        df.loc[df['INSTAP_FACTOR']>1, instap_kolommen + uitstap_kolommen] = df.loc[df['INSTAP_FACTOR']>1, instap_kolommen + uitstap_kolommen].multiply(
             df.loc[df['INSTAP_FACTOR']>1, 'INSTAP_FACTOR'], axis="index").round().astype(int)
        df= df[start_kolommen]

        output_file = G01_folder_opgehoogd / file.name
        df.to_csv(output_file, sep=';', index=False, encoding='latin1')
        # Remove this break to process all files
        #raise StopIteration("Processed one file for testing. Remove this line to process all files.")

    print("Processing completed.")
# %%
