#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import unicodedata
import os
import datetime
import re


# In[2]:


# Read Excel files
hl_df = pd.read_excel("/Users/mikemike/Downloads/EIFFAGE CODE/HL_MATERIAUX.xlsx")
siren_df = pd.read_excel("/Users/mikemike/Downloads/EIFFAGE CODE/SIREN_APE.xlsx")
naf_df = pd.read_excel("CF_WF_NAF_France_2024_Adjusted-code.xlsx")



# In[3]:


# 1. Define the name cleaning function
def clean_name(name):
    if isinstance(name, str):
        name = name.replace("(E)", "").strip().lower()  # Remove (E), trim spaces, convert to lowercase
        name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('utf-8')  # Remove accents
        name = " ".join(name.split())  # Replace multiple spaces with single space
        return name
    return name


# In[4]:


# 5. Apply name cleaning to both datasets
hl_df['Fournisseurs_Eiffage'] = hl_df['Fournisseur enfant panel'].apply(clean_name)
siren_df['Fournisseurs_Eiffage'] = siren_df['Fournisseur'].apply(clean_name)


# In[5]:


# 6. Merge both datasets using cleaned supplier names
merged_df = pd.merge(hl_df, siren_df[['Fournisseurs_Eiffage', 'Code SIREN', 'Code APE']],
                     on='Fournisseurs_Eiffage', how='left')


# In[6]:


# Cleaning function for SIREN and APE codes
def clean_code(code):
    if pd.isnull(code):
        return code
    return str(code).replace('\u00A0', '').replace('\u202F', '').replace(' ', '').strip()

# Apply cleaning on merged_df
merged_df['Code SIREN'] = merged_df['Code SIREN'].apply(clean_code)
merged_df['Code APE'] = merged_df['Code APE'].apply(clean_code)


# In[ ]:


# 7. For suppliers not found, ask operator to enter missing codes
for idx, row in merged_df.iterrows():
    if pd.isnull(row['Code SIREN']) or pd.isnull(row['Code APE']):
        supplier_name = row['Fournisseur enfant panel']
        print(f"\nSupplier not found: {supplier_name}")
        code_siren = input(f"Please enter Code SIREN for supplier '{supplier_name}': ")
        code_ape = input(f"Please enter Code APE for supplier '{supplier_name}': ")
        merged_df.loc[idx, 'Code SIREN'] = code_siren
        merged_df.loc[idx, 'Code APE'] = code_ape


# In[ ]:


# 8. Build the final output table (keep required columns)
final_columns = ['Panel parent', 'Panel enfant', 'Fournisseurs_Eiffage', 'Dépense N', 'Code SIREN', 'Code APE']
final_df = merged_df[final_columns]


# In[ ]:


# 9. Export the final enriched file with timestamp
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
output_file = f'HL_MATERIAUX_CODE_APE_SIREN_{timestamp}.xlsx'
final_df.to_excel(output_file, index=False)
print(f"\nOutput file generated: {output_file}")


# In[ ]:


def clean_entire_dataframe(df):
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].astype(str).str.replace('\u00A0', '', regex=True).str.strip()
    return df


# In[ ]:


# 8. Clean APE and NAF codes
merged_df['Code APE Clean'] = merged_df['Code APE'].str.replace('.', '', regex=False).str.strip()
naf_df['Code NAF Clean'] = naf_df['Code NAF'].str.replace('.', '', regex=False).str.strip()

# 9. Match Code APE ↔ Code NAF and get sector info
merged_sector = pd.merge(
    merged_df[['Panel parent', 'Panel enfant', 'Fournisseur enfant panel', 'Code SIREN', 'Code APE', 'Code APE Clean']],
    naf_df[['Code NAF Clean', 'new best match sector', 'kg CO2-eq per EUR2024', 'm3 water eq per EUR2024']],
    left_on='Code APE Clean',
    right_on='Code NAF Clean',
    how='left'
)


# In[ ]:


# 10. Prepare Output Table 1
table1 = merged_sector[[
    'Panel parent',
    'Panel enfant',
    'Fournisseur enfant panel',
    'Code APE Clean',
    'new best match sector',
    'kg CO2-eq per EUR2024',
    'm3 water eq per EUR2024'
]].rename(columns={
    'Code APE Clean': 'Code NAF',
    'Fournisseur enfant panel': 'Fournisseur Panel enfant'
})

# 11. Handle missing values
for col in table1.columns:
    table1[col] = table1[col].fillna("NOT FOUND")

# 12. OUTPUT TABLE 1
print("📄 Output Table 1:")
from IPython.display import display, HTML
pd.set_option('display.max_rows', None)
display(HTML(table1.to_html(index=False)))


# In[ ]:


print("Colonnes disponibles dans hl_df :")
for col in hl_df.columns:
    print(f"'{col}'")


# In[ ]:


# 13. La dépense est déjà présente dès le début dans 'Dépense N', on renomme pour la suite
# Normalise les colonnes juste après lecture du fichier
hl_df.columns = hl_df.columns.str.strip().str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')

merged_sector.rename(columns={'Dépense N': 'DEPENSES'}, inplace=True)


# In[ ]:


# 14.Clean NAF codes
final_table = merged_df[['Panel parent', 'Panel enfant', 'Fournisseur enfant panel', 'Code SIREN', 'Code APE', 'Dépense N']].copy()


# Clean both codes
naf_df['Code NAF Clean'] = naf_df['Code NAF'].str.replace('.', '', regex=False).str.strip()
final_table['Code APE Clean'] = final_table['Code APE'].str.replace('.', '', regex=False).str.strip()
# Very important: rename only ONCE here
final_table.rename(columns={'Dépense N': 'DEPENSES'}, inplace=True)

# Merge with the emission factors
merged_sector = pd.merge(
    final_table,
    naf_df[['Code NAF Clean', 'new best match sector', 'kg CO2-eq per EUR2024', 'm3 water eq per EUR2024']],
    left_on='Code APE Clean',
    right_on='Code NAF Clean',
    how='left'
)
# 15. Calculation of GHG emissions and water consumption
def safe_multiply(x, y):
    try:
        return float(x) * float(y)
    except:
        return None  # None pour garder propre

merged_sector['GHG Emissions (kg CO2)'] = merged_sector.apply(
    lambda row: safe_multiply(row['DEPENSES'], row['kg CO2-eq per EUR2024']), axis=1
)

merged_sector['Water Consumption (m³)'] = merged_sector.apply(
    lambda row: safe_multiply(row['DEPENSES'], row['m3 water eq per EUR2024']), axis=1
)


# In[ ]:


# 16. Structuring of the final table by panel
panel_parent_col = [col for col in merged_sector.columns if 'Panel parent' in col][0]
panel_enfant_col = [col for col in merged_sector.columns if 'Panel enfant' in col][0]

structured_rows = []
grouped = merged_sector.groupby([panel_parent_col, panel_enfant_col])

for (panel_parent, panel_enfant), group in grouped:
    total_depenses = group['DEPENSES'].sum(skipna=True)
    structured_rows.append({
        'Panel parent': panel_parent,
        'Panel enfant': panel_enfant,
        'Fournisseur': "",
        'DÉPENSES (€)': "Total : " + f"{total_depenses:,.0f}".replace(",", " ").replace(".", ",") if pd.notna(total_depenses) else "",
        'Code APE': "",
        'Code SIREN': "",
        'Émissions CO2 (kg)': "",
        'Consommation eau (m³)': ""
    })

    for _, row in group.iterrows():
        structured_rows.append({
            'Panel parent': panel_parent,
            'Panel enfant': panel_enfant,
            'Fournisseur': row['Fournisseur enfant panel'][:35],
            'DÉPENSES (€)': f"{row['DEPENSES']:,.0f}".replace(",", " ").replace(".", ",") if pd.notna(row['DEPENSES']) else "",
            'Code APE': row['Code APE'] if pd.notna(row['Code APE']) else "",
            'Code SIREN': row['Code SIREN'] if pd.notna(row['Code SIREN']) else "",
            'Émissions CO2 (kg)': f"{row['GHG Emissions (kg CO2)']:,.0f}".replace(",", " ").replace(".", ",") if pd.notna(row['GHG Emissions (kg CO2)']) else "",
            'Consommation eau (m³)': f"{row['Water Consumption (m³)']:,.0f}".replace(",", " ").replace(".", ",") if pd.notna(row['Water Consumption (m³)']) else ""
        })




# In[ ]:


# 17. Creation of the final table

final_structured_table = pd.DataFrame(structured_rows)


# In[ ]:


# 18. Excel export
final_structured_table.to_excel("final_table.xlsx", index=False)




# In[ ]:


# 19. display

from IPython.display import display, HTML
pd.set_option('display.max_rows', None)
display(HTML(final_structured_table.to_html(index=False)))

