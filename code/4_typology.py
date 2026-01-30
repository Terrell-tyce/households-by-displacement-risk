#!/usr/bin/env python
# coding: utf-8

# ==========================================================================
# Displacement Typology Classification
# Sacramento County, 2023 ACS Data
# ==========================================================================
# Adapted from: https://github.com/urban-displacement/displacement-typologies
# Updated for Sacramento 2023 ACS data and R lag variables
# ==========================================================================

import pandas as pd
from shapely import wkt
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import sys

# ==========================================================================
# Setup paths
# ==========================================================================

city_name = 'Sacramento'

home = str(Path.home())
home = home + '/Downloads/households-by-displacement-risk'
input_path = home + '/data/inputs/'
output_path = home + '/data/outputs/'

# ==========================================================================
# Read data
# ==========================================================================
lag = pd.read_csv(output_path + 'lags/lag_sacramento_2023.csv')

typology_input = pd.read_csv(
    output_path + '/databases/' + city_name.replace(" ", "") + '_database_2023.csv',
    index_col=0
)

typology_input['geometry'] = typology_input['geometry'].apply(wkt.loads)
geo_typology_input = gpd.GeoDataFrame(typology_input, geometry='geometry')
data = geo_typology_input.copy(deep=True)

print(f"Total tracts loaded: {len(data)}")

# ==========================================================================
# Income Categorization
# ==========================================================================

data['pop00flag'] = np.where((data['pop_00'] > 500), 1, 0)
print("Number of tracts with insufficient data: ")
print(data['pop_00'].size-data['pop00flag'].sum())
# ==========================================================================
# Define Vulnerability to Gentrification
# ==========================================================================
# Requires 3/4 criteria: low rent OR low home value + low income/high POC/high rent/low education

### 1990
data['vul_gent_90'] = np.where(
    ((data['aboverm_real_mrent_90'] == 0) | (data['aboverm_real_mhval_90'] == 0)) &
    ((data['aboverm_per_all_li_90'] +
      data['aboverm_per_nonwhite_90'] +
      data['aboverm_per_rent_90'] +
      (1 - data['aboverm_per_col_90'])) > 2),
    1, 0
)

### 2000
data['vul_gent_00'] = np.where(
    ((data['aboverm_real_mrent_00'] == 0) | (data['aboverm_real_mhval_00'] == 0)) &
    ((data['aboverm_per_all_li_00'] +
      data['aboverm_per_nonwhite_00'] +
      data['aboverm_per_rent_00'] +
      (1 - data['aboverm_per_col_00'])) > 2),
    1, 0
)

### 2023
data['vul_gent_23'] = np.where(
    ((data['aboverm_real_mrent_23'] == 0) | (data['aboverm_real_mhval_23'] == 0)) &
    ((data['aboverm_per_all_li_23'] +
      data['aboverm_per_nonwhite_23'] +
      data['aboverm_per_rent_23'] +
      (1 - data['aboverm_per_col_23'])) > 2),
    1, 0
)

# ==========================================================================
# Define Hot Market Variable
# ==========================================================================
# Hot market = above-regional-median price growth

data['hotmarket_90'] = np.where(
    (data['aboverm_pctch_real_mhval_90_00'] == 1) |
    (data['aboverm_pctch_real_mrent_90_00'] == 1),
    1, 0
)
data['hotmarket_90'] = np.where(
    (data['aboverm_pctch_real_mhval_90_00'].isna()) |
    (data['aboverm_pctch_real_mrent_90_00'].isna()),
    np.nan, data['hotmarket_90']
)

data['hotmarket_00'] = np.where(
    (data['aboverm_pctch_real_mhval_90_00'] == 1) |
    (data['aboverm_pctch_real_mrent_90_00'] == 1),
    1, 0
)
data['hotmarket_00'] = np.where(
    (data['aboverm_pctch_real_mhval_90_00'].isna()) |
    (data['aboverm_pctch_real_mrent_90_00'].isna()),
    np.nan, data['hotmarket_00']
)

data['hotmarket_23'] = np.where(
    (data['aboverm_pctch_real_mhval_00_23'] == 1) |
    (data['aboverm_pctch_real_mrent_12_23'] == 1),
    1, 0
)
data['hotmarket_23'] = np.where(
    (data['aboverm_pctch_real_mhval_00_23'].isna()) |
    (data['aboverm_pctch_real_mrent_12_23'].isna()),
    np.nan, data['hotmarket_23']
)

# ==========================================================================
# Define Experienced Gentrification
# ==========================================================================
# Gentrification = vulnerable + education increase + income increase + lost low-income + hot market

### 1990-2000
data['gent_90_00'] = np.where(
    (data['vul_gent_90'] == 1) &
    (data['aboverm_ch_per_col_90_00'] == 1) &
    (data['aboverm_pctch_real_hinc_90_00'] == 1) &
    (data['lostli_00'] == 1) &
    (data['hotmarket_00'] == 1),
    1, 0
)

data['gent_90_00_urban'] = np.where(
    (data['vul_gent_90'] == 1) &
    (data['aboverm_ch_per_col_90_00'] == 1) &
    (data['aboverm_pctch_real_hinc_90_00'] == 1) &
    (data['hotmarket_00'] == 1),
    1, 0
)

### 2000-2023
data['gent_00_23'] = np.where(
    (data['vul_gent_00'] == 1) &
    (data['aboverm_ch_per_col_00_23'] == 1) &
    (data['aboverm_pctch_real_hinc_00_23'] == 1) &
    (data['lostli_23'] == 1) &
    (data['hotmarket_23'] == 1),
    1, 0
)

data['gent_00_23_urban'] = np.where(
    (data['vul_gent_00'] == 1) &
    (data['aboverm_ch_per_col_00_23'] == 1) &
    (data['aboverm_pctch_real_hinc_00_23'] == 1) &
    (data['hotmarket_23'] == 1),
    1, 0
)

# ==========================================================================
# Merge lag variables from R script
# ==========================================================================

print("Merging lag variables...")
data = pd.merge(
    data,
    lag[['dp_PChRent', 'dp_RentGap', 'GEOID', 'tr_rent_gap', 'rm_rent_gap', 'dense']],
    on='GEOID'
)

# ==========================================================================
# ==========================================================================
# ==========================================================================
# CONSTRUCT TYPOLOGY
# ==========================================================================
# ==========================================================================
# ==========================================================================

print("\n" + "="*80)
print("CONSTRUCTING DISPLACEMENT TYPOLOGY")
print("="*80 + "\n")

df = data.copy()

# ==========================================================================
# Stable/Advanced Exclusive
# ==========================================================================

print("Classifying: Stable/Advanced Exclusive...")

df['SAE'] = 0
df['SAE'] = np.where(
    (df['pop00flag'] == 1) &
    (df['high_pdmt_medhhinc_00'] == 1) &
    (df['high_pdmt_medhhinc_23'] == 1) &
    ((df['lmh_flag_encoded'] == 3) | (df['lmh_flag_encoded'] == 6)) &
    ((df['change_flag_encoded'] == 1) | (df['change_flag_encoded'] == 2) |
     (df['change_flag_encoded'] == 3)),
    1, 0
)

df['SAE'] = np.where(
    (df['pop00flag'].isna()) |
    (df['high_pdmt_medhhinc_00'].isna()) |
    (df['high_pdmt_medhhinc_23'].isna()) |
    (df['lmh_flag_encoded'].isna()) |
    (df['change_flag_encoded'].isna()),
    np.nan, df['SAE']
)

print(f"  Stable/Advanced Exclusive: {(df['SAE'] == 1).sum()} tracts")

### Get exclusive tracts for proximity calculation
exclusive = df[df['SAE'] == 1].reset_index(drop=True)

### Find tracts adjacent to exclusive tracts
if len(exclusive) > 0:
    proximity = df[df.geometry.touches(exclusive.union_all())]
else:
    proximity = df[df['SAE'] == -999]  # Empty set

# ==========================================================================
# Advanced Gentrification
# ==========================================================================

print("Classifying: Advanced Gentrification...")

df['AdvG'] = 0
df['AdvG'] = np.where(
    (df['pop00flag'] == 1) &
    ((df['mod_pdmt_medhhinc_23'] == 1) | (df['mix_mod_medhhinc_23'] == 1) |
     (df['mix_high_medhhinc_23'] == 1) | (df['high_pdmt_medhhinc_23'] == 1)) &
    ((df['lmh_flag_encoded'] == 2) | (df['lmh_flag_encoded'] == 3) |
     (df['lmh_flag_encoded'] == 5) | (df['lmh_flag_encoded'] == 6)) &
    ((df['change_flag_encoded'] == 1) | (df['change_flag_encoded'] == 2) |
     (df['change_flag_encoded'] == 3)) &
    ((df['pctch_real_mhval_00_23'] > 0) | (df['pctch_real_mrent_12_23'] > 0)) &
    (
        ((df['dense'] == 0) & (df['gent_90_00'] == 1)) |
        ((df['dense'] == 0) & (df['gent_00_23'] == 1)) |
        ((df['dense'] == 1) & (df['gent_90_00_urban'] == 1)) |
        ((df['dense'] == 1) & (df['gent_00_23_urban'] == 1))
    ),
    1, 0
)

df['AdvG'] = np.where(
    (df['pop00flag'].isna()) |
    (df['mod_pdmt_medhhinc_23'].isna()) |
    (df['mix_mod_medhhinc_23'].isna()) |
    (df['mix_high_medhhinc_23'].isna()) |
    (df['high_pdmt_medhhinc_23'].isna()) |
    (df['lmh_flag_encoded'].isna()) |
    (df['change_flag_encoded'].isna()) |
    (df['gent_90_00'].isna()) |
    (df['gent_90_00_urban'].isna()) |
    (df['gent_00_23_urban'].isna()) |
    (df['pctch_real_mhval_00_23'].isna()) |
    (df['pctch_real_mrent_12_23'].isna()) |
    (df['gent_00_23'].isna()),
    np.nan, df['AdvG']
)

df['AdvG'] = np.where((df['AdvG'] == 1) & (df['SAE'] == 1), 0, df['AdvG'])

print(f"  Advanced Gentrification: {(df['AdvG'] == 1).sum()} tracts")

# ==========================================================================
# At Risk of Becoming Exclusive
# ==========================================================================

print("Classifying: At Risk of Becoming Exclusive...")

df['ARE'] = 0
df['ARE'] = np.where(
    (df['pop00flag'] == 1) &
    ((df['mod_pdmt_medhhinc_23'] == 1) | (df['mix_mod_medhhinc_23'] == 1) |
     (df['mix_high_medhhinc_23'] == 1) | (df['high_pdmt_medhhinc_23'] == 1)) &
    ((df['lmh_flag_encoded'] == 2) | (df['lmh_flag_encoded'] == 3) |
     (df['lmh_flag_encoded'] == 5) | (df['lmh_flag_encoded'] == 6)) &
    ((df['change_flag_encoded'] == 1) | (df['change_flag_encoded'] == 2)),
    1, 0
)

df['ARE'] = np.where(
    (df['pop00flag'].isna()) |
    (df['mod_pdmt_medhhinc_23'].isna()) |
    (df['mix_mod_medhhinc_23'].isna()) |
    (df['mix_high_medhhinc_23'].isna()) |
    (df['high_pdmt_medhhinc_23'].isna()) |
    (df['lmh_flag_encoded'].isna()) |
    (df['change_flag_encoded'].isna()),
    np.nan, df['ARE']
)

df['ARE'] = np.where((df['ARE'] == 1) & (df['AdvG'] == 1), 0, df['ARE'])
df['ARE'] = np.where((df['ARE'] == 1) & (df['SAE'] == 1), 0, df['ARE'])

print(f"  At Risk of Becoming Exclusive: {(df['ARE'] == 1).sum()} tracts")

# ==========================================================================
# Becoming Exclusive
# ==========================================================================

print("Classifying: Becoming Exclusive...")

df['BE'] = 0
df['BE'] = np.where(
    (df['pop00flag'] == 1) &
    ((df['mod_pdmt_medhhinc_23'] == 1) | (df['mix_mod_medhhinc_23'] == 1) |
     (df['mix_high_medhhinc_23'] == 1) | (df['high_pdmt_medhhinc_23'] == 1)) &
    ((df['lmh_flag_encoded'] == 2) | (df['lmh_flag_encoded'] == 3) |
     (df['lmh_flag_encoded'] == 5) | (df['lmh_flag_encoded'] == 6)) &
    (df['change_flag_encoded'] == 3) &
    (df['lostli_23'] == 1) &
    (df['per_limove_23'] < df['per_limove_12']) &
    (df['real_hinc_23'] > df['real_hinc_00']),
    1, 0
)

df['BE'] = np.where(
    (df['pop00flag'].isna()) |
    (df['mod_pdmt_medhhinc_23'].isna()) |
    (df['mix_mod_medhhinc_23'].isna()) |
    (df['mix_high_medhhinc_23'].isna()) |
    (df['high_pdmt_medhhinc_23'].isna()) |
    (df['lmh_flag_encoded'].isna()) |
    (df['change_flag_encoded'].isna()) |
    (df['lostli_23'].isna()) |
    (df['per_limove_23'].isna()) |
    (df['per_limove_12'].isna()) |
    (df['real_hinc_23'].isna()) |
    (df['real_hinc_00'].isna()),
    np.nan, df['BE']
)

df['BE'] = np.where((df['BE'] == 1) & (df['SAE'] == 1), 0, df['BE'])
df['BE'] = np.where((df['BE'] == 1) & (df['AdvG'] == 1), 0, df['BE'])

print(f"  Becoming Exclusive: {(df['BE'] == 1).sum()} tracts")

# ==========================================================================
# Stable Moderate/Mixed Income
# ==========================================================================

print("Classifying: Stable Moderate/Mixed Income...")

df['SMMI'] = 0
df['SMMI'] = np.where(
    (df['pop00flag'] == 1) &
    ((df['mod_pdmt_medhhinc_23'] == 1) | (df['mix_mod_medhhinc_23'] == 1) |
     (df['mix_high_medhhinc_23'] == 1) | (df['high_pdmt_medhhinc_23'] == 1)) &
    (df['ARE'] == 0) & (df['BE'] == 0) & (df['SAE'] == 0) & (df['AdvG'] == 0),
    1, 0
)

df['SMMI'] = np.where(
    (df['pop00flag'].isna()) |
    (df['mod_pdmt_medhhinc_23'].isna()) |
    (df['mix_mod_medhhinc_23'].isna()) |
    (df['mix_high_medhhinc_23'].isna()) |
    (df['high_pdmt_medhhinc_23'].isna()),
    np.nan, df['SMMI']
)

print(f"  Stable Moderate/Mixed Income: {(df['SMMI'] == 1).sum()} tracts")

# ==========================================================================
# At Risk of Gentrification
# ==========================================================================

print("Classifying: At Risk of Gentrification...")

df['ARG'] = 0
df['ARG'] = np.where(
    (df['pop00flag'] == 1) &
    ((df['low_pdmt_medhhinc_23'] == 1) | (df['mix_low_medhhinc_23'] == 1)) &
    ((df['lmh_flag_encoded'] == 1) | (df['lmh_flag_encoded'] == 4)) &
    ((df['change_flag_encoded'] == 1) | (df['ab_90percentile_ch'] == 1) |
     (df['rent_90percentile_ch'] == 1)) &
    (df['gent_90_00'] == 0) &
    ((df['dp_PChRent'] == 1) | (df['dp_RentGap'] == 1)) &
    (df['vul_gent_23'] == 1) &
    (df['gent_00_23'] == 0),
    1, 0
)

df['ARG'] = np.where(
    (df['pop00flag'].isna()) |
    (df['low_pdmt_medhhinc_23'].isna()) |
    (df['mix_low_medhhinc_23'].isna()) |
    (df['lmh_flag_encoded'].isna()) |
    (df['change_flag_encoded'].isna()) |
    (df['rent_90percentile_ch'].isna()) |
    (df['gent_90_00'].isna()) |
    (df['vul_gent_00'].isna()) |
    (df['dp_PChRent'].isna()) |
    (df['dp_RentGap'].isna()) |
    (df['gent_00_23'].isna()),
    np.nan, df['ARG']
)

print(f"  At Risk of Gentrification: {(df['ARG'] == 1).sum()} tracts")

# ==========================================================================
# Early/Ongoing Gentrification
# ==========================================================================

print("Classifying: Early/Ongoing Gentrification...")

df['EOG'] = 0
df['EOG'] = np.where(
    (df['pop00flag'] == 1) &
    ((df['low_pdmt_medhhinc_23'] == 1) | (df['mix_low_medhhinc_23'] == 1)) &
    ((df['lmh_flag_encoded'] == 2) | (df['lmh_flag_encoded'] == 5)) &
    ((df['change_flag_encoded'] == 2) | (df['change_flag_encoded'] == 3) |
     (df['hv_abrm_ch'] == 1) | (df['rent_abrm_ch'] == 1)) &
    (
        ((df['dense'] == 0) & (df['gent_90_00'] == 1)) |
        ((df['dense'] == 0) & (df['gent_00_23'] == 1)) |
        ((df['dense'] == 1) & (df['gent_90_00_urban'] == 1)) |
        ((df['dense'] == 1) & (df['gent_00_23_urban'] == 1))
    ),
    1, 0
)

df['EOG'] = np.where(
    (df['pop00flag'].isna()) |
    (df['low_pdmt_medhhinc_23'].isna()) |
    (df['mix_low_medhhinc_23'].isna()) |
    (df['lmh_flag_encoded'].isna()) |
    (df['change_flag_encoded'].isna()) |
    (df['gent_90_00'].isna()) |
    (df['gent_00_23'].isna()) |
    (df['gent_90_00_urban'].isna()) |
    (df['gent_00_23_urban'].isna()) |
    (df['ab_50pct_ch'].isna()) |
    (df['hv_abrm_ch'].isna()) |
    (df['rent_abrm_ch'].isna()) |
    (df['rent_50pct_ch'].isna()),
    np.nan, df['EOG']
)

print(f"  Early/Ongoing Gentrification: {(df['EOG'] == 1).sum()} tracts")

# ==========================================================================
# Ongoing Displacement
# ==========================================================================

print("Classifying: Ongoing Displacement...")

df['OD'] = 0
df['OD'] = np.where(
    (df['pop00flag'] == 1) &
    ((df['low_pdmt_medhhinc_23'] == 1) | (df['mix_low_medhhinc_23'] == 1)) &
    (df['lostli_23'] == 1),
    1, 0
)

df['OD_loss'] = np.where(
    (df['pop00flag'].isna()) |
    (df['low_pdmt_medhhinc_23'].isna()) |
    (df['mix_low_medhhinc_23'].isna()) |
    (df['lostli_23'].isna()),
    np.nan, df['OD']
)

df['OD'] = np.where((df['OD'] == 1) & (df['ARG'] == 1), 0, df['OD'])
df['OD'] = np.where((df['OD'] == 1) & (df['EOG'] == 1), 0, df['OD'])

print(f"  Ongoing Displacement: {(df['OD'] == 1).sum()} tracts")

# ==========================================================================
# Low-Income/Susceptible to Displacement
# ==========================================================================

print("Classifying: Low-Income/Susceptible to Displacement...")

df['LISD'] = 0
df['LISD'] = np.where(
    (df['pop00flag'] == 1) &
    ((df['low_pdmt_medhhinc_23'] == 1) | (df['mix_low_medhhinc_23'] == 1)) &
    (df['OD'] != 1) & (df['ARG'] != 1) & (df['EOG'] != 1),
    1, 0
)

print(f"  Low-Income/Susceptible to Displacement: {(df['LISD'] == 1).sum()} tracts")

# ==========================================================================
# Create Final Typology Variable
# ==========================================================================

print("\nAssigning typology classes...")

df['double_counted'] = (
    df['LISD'].fillna(0) + df['OD'].fillna(0) + df['ARG'].fillna(0) +
    df['EOG'].fillna(0) + df['AdvG'].fillna(0) + df['ARE'].fillna(0) +
    df['BE'].fillna(0) + df['SAE'] + df['SMMI']
)

df['typology'] = np.nan
df['typology'] = np.where(df['LISD'] == 1, 1, df['typology'])
df['typology'] = np.where(df['OD'] == 1, 2, df['typology'])
df['typology'] = np.where(df['ARG'] == 1, 3, df['typology'])
df['typology'] = np.where(df['EOG'] == 1, 4, df['typology'])
df['typology'] = np.where(df['AdvG'] == 1, 5, df['typology'])
df['typology'] = np.where(df['SMMI'] == 1, 6, df['typology'])
df['typology'] = np.where(df['ARE'] == 1, 7, df['typology'])
df['typology'] = np.where(df['BE'] == 1, 8, df['typology'])
df['typology'] = np.where(df['SAE'] == 1, 9, df['typology'])
df['typology'] = np.where(df['double_counted'] > 1, 99, df['typology'])

# Double Classification Check
cat_i = []
for i in range(len(df)):
    categories = []
    if df['LISD'].iloc[i] == 1:
        categories.append('LISD')
    if df['OD'].iloc[i] == 1:
        categories.append('OD')
    if df['ARG'].iloc[i] == 1:
        categories.append('ARG')
    if df['EOG'].iloc[i] == 1:
        categories.append('EOG')
    if df['AdvG'].iloc[i] == 1:
        categories.append('AdvG')
    if df['SMMI'].iloc[i] == 1:
        categories.append('SMMI')
    if df['ARE'].iloc[i] == 1:
        categories.append('ARE')
    if df['BE'].iloc[i] == 1:
        categories.append('BE')
    if df['SAE'].iloc[i] == 1:
        categories.append('SAE')
    cat_i.append(str(categories))

df['typ_cat'] = cat_i

# ==========================================================================
# Summary Statistics
# ==========================================================================

print("\n" + "="*80)
print("TYPOLOGY SUMMARY")
print("="*80)

typology_summary = df.groupby('typ_cat').size().sort_values(ascending=False)
print("\nTypology Distribution:")
print(typology_summary)

print(f"\nTotal tracts classified: {len(df[df['typology'].notna()])}")
print(f"Double-counted tracts (error): {(df['double_counted'] > 1).sum()}")
print(f"Missing typology: {df['typology'].isna().sum()}")

# ==========================================================================
# Export Results
# ==========================================================================

print("\nExporting typology results...")

# Remove geometry for CSV export
df_export = df.drop(columns=['geometry'])

df_export.to_csv(
    output_path + '/typologies/' + city_name.replace(" ", "") + '_typology_output.csv'
)

print(f"✓ Output saved to: {output_path}/typologies/{city_name.replace(' ', '')}_typology_output.csv")

# Find which tracts are missing and why
missing_tracts = df[df['typology'].isna()]

print("\n" + "="*80)
print("MISSING TYPOLOGY TRACTS - DIAGNOSTIC")
print("="*80)

for idx, tract in missing_tracts.iterrows():
    print(f"\nTract {tract['GEOID']}:")
    print(f"  pop_00: {tract['pop_00']}")
    print(f"  pop00flag: {tract['pop00flag']}")
    
    # Check which key variables are NaN
    key_vars = [
        'real_mhval_23', 'real_mrent_23', 'real_hinc_23',
        'pctch_real_mhval_00_23', 'pctch_real_mrent_12_23',
        'vul_gent_23', 'hotmarket_23', 'lostli_23'
    ]
    
    nan_vars = [var for var in key_vars if pd.isna(tract[var])]
    if nan_vars:
        print(f"  Missing variables: {nan_vars}")
    
    # Check which typology it ALMOST matched
    print(f"  AdvG: {tract['AdvG']}, ARE: {tract['ARE']}, BE: {tract['BE']}")
    print(f"  SMMI: {tract['SMMI']}, ARG: {tract['ARG']}, EOG: {tract['EOG']}")
    print(f"  OD: {tract['OD']}, LISD: {tract['LISD']}, SAE: {tract['SAE']}")