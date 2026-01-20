
# ==========================================================================
# ==========================================================================
# ==========================================================================
# Data Download
# Note: As of 2020, the Census API has been somewhat unreliable. We encourage
# everyone to save all their downloads so you don't run into delays while
# working on your project. Don't rely on the API to download everyday.
# ==========================================================================
# ==========================================================================
# ==========================================================================

#!/usr/bin/env python
# coding: utf-8

# ==========================================================================
# Import Libraries
# ==========================================================================

import census
import pandas as pd
import numpy as np
import sys
from pathlib import Path
import geopandas as gpd
from shapely.geometry import Point
from pyproj import Proj
import matplotlib.pyplot as plt
import os
import requests

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.options.display.float_format = '{:.2f}'.format # avoid scientific notation

home = str(Path.home())
input_path = home+'/Downloads/households-by-displacement-risk/data/inputs/'
output_path = home+'/Downloads/households-by-displacement-risk/data/outputs/'

# Get the directory where test.py is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FILE_API = os.path.join(BASE_DIR, 'api_key.txt')

with open(FILE_API, 'r') as file:
    key = file.read()
    
key = key.strip()
c = census.Census(key)

# ==========================================================================
# Choose Cities
# ==========================================================================

# Choose City and Census Tracts of Interest
# --------------------------------------------------------------------------
# To get city data, run the following code in the terminal
# `python data.py <city name>`
# Example: python data.py Atlanta

city_name = 'Sacramento'
state = '06'
FIPS = ['067']

sql_query='state:{} county:*'.format(state)


# Create Filter Function
# --------------------------------------------------------------------------

def filter_FIPS(df):
    df = df[df['county'].isin(FIPS)]
    return df


# ==========================================================================
# Download Raw Data
# ==========================================================================

# Download ACS 2023 5-Year Estimates
# --------------------------------------------------------------------------

df_vars_23=['B03002_001E',
            'B03002_003E',
            'B19001_001E',
            'B19013_001E',
            'B25077_001E',
            'B25077_001M',
            'B25064_001E',
            'B25064_001M',
            'B15003_001E',
            'B15003_022E',
            'B15003_023E',
            'B15003_024E',
            'B15003_025E',
            'B25034_001E',
            'B25034_010E',
            'B25034_011E',
            'B25003_002E',
            'B25003_003E',
            'B25105_001E',
            'B06011_001E']

# Income categories - see notes
var_str = 'B19001'
var_list = []
for i in range (1, 18):
    var_list.append(var_str+'_'+str(i).zfill(3)+'E')
df_vars_23 = df_vars_23 + var_list

# Migration - see notes
var_str = 'B07010'
var_list = []
for i in list(range(25,34))+list(range(36, 45))+list(range(47, 56))+list(range(58, 67)):
    var_list.append(var_str+'_'+str(i).zfill(3)+'E')
df_vars_23 = df_vars_23 + var_list


# Run API query
# --------------------------------------------------------------------------

var_dict_acs5 = c.acs5.get(df_vars_23, geo = {'for': 'tract:*',
                                 'in': sql_query}, year=2023)


# Convert and Rename Variables
# --------------------------------------------------------------------------

### Converts variables into dataframe and filters only FIPS of interest

df_vars_23 = pd.DataFrame.from_dict(var_dict_acs5)
df_vars_23['FIPS']=df_vars_23['state']+df_vars_23['county']+df_vars_23['tract']
df_vars_23 = filter_FIPS(df_vars_23)

### Renames variables

df_vars_23 = df_vars_23.rename(columns = {'B03002_001E':'pop_23',
                                          'B03002_003E':'white_23',
                                          'B19001_001E':'hh_23',
                                          'B19013_001E':'hinc_23',
                                          'B25077_001E':'mhval_23',
                                          'B25077_001M':'mhval_23_se',
                                          'B25064_001E':'mrent_23',
                                          'B25064_001M':'mrent_23_se',
                                          'B25003_002E':'ohu_23',
                                          'B25003_003E':'rhu_23',
                                          'B25105_001E':'mmhcosts_23',
                                          'B15003_001E':'total_25_23',
                                          'B15003_022E':'total_25_col_bd_23',
                                          'B15003_023E':'total_25_col_md_23',
                                          'B15003_024E':'total_25_col_pd_23',
                                          'B15003_025E':'total_25_col_phd_23',
                                          'B25034_001E':'tot_units_built_23',
                                          'B25034_010E':'units_40_49_built_23',
                                          'B25034_011E':'units_39_early_built_23',
                                          'B07010_025E':'mov_wc_w_income_23',
                                          'B07010_026E':'mov_wc_9000_23',
                                          'B07010_027E':'mov_wc_15000_23',
                                          'B07010_028E':'mov_wc_25000_23',
                                          'B07010_029E':'mov_wc_35000_23',
                                          'B07010_030E':'mov_wc_50000_23',
                                          'B07010_031E':'mov_wc_65000_23',
                                          'B07010_032E':'mov_wc_75000_23',
                                          'B07010_033E':'mov_wc_76000_more_23',
                                          'B07010_036E':'mov_oc_w_income_23',
                                          'B07010_037E':'mov_oc_9000_23',
                                          'B07010_038E':'mov_oc_15000_23',
                                          'B07010_039E':'mov_oc_25000_23',
                                          'B07010_040E':'mov_oc_35000_23',
                                          'B07010_041E':'mov_oc_50000_23',
                                          'B07010_042E':'mov_oc_65000_23',
                                          'B07010_043E':'mov_oc_75000_23',
                                          'B07010_044E':'mov_oc_76000_more_23',
                                          'B07010_047E':'mov_os_w_income_23',
                                          'B07010_048E':'mov_os_9000_23',
                                          'B07010_049E':'mov_os_15000_23',
                                          'B07010_050E':'mov_os_25000_23',
                                          'B07010_051E':'mov_os_35000_23',
                                          'B07010_052E':'mov_os_50000_23',
                                          'B07010_053E':'mov_os_65000_23',
                                          'B07010_054E':'mov_os_75000_23',
                                          'B07010_055E':'mov_os_76000_more_23',
                                          'B07010_058E':'mov_fa_w_income_23',
                                          'B07010_059E':'mov_fa_9000_23',
                                          'B07010_060E':'mov_fa_15000_23',
                                          'B07010_061E':'mov_fa_25000_23',
                                          'B07010_062E':'mov_fa_35000_23',
                                          'B07010_063E':'mov_fa_50000_23',
                                          'B07010_064E':'mov_fa_65000_23',
                                          'B07010_065E':'mov_fa_75000_23',
                                          'B07010_066E':'mov_fa_76000_more_23',
                                          'B06011_001E':'iinc_23',
                                          'B19001_002E':'I_10000_23',
                                          'B19001_003E':'I_15000_23',
                                          'B19001_004E':'I_20000_23',
                                          'B19001_005E':'I_25000_23',
                                          'B19001_006E':'I_30000_23',
                                          'B19001_007E':'I_35000_23',
                                          'B19001_008E':'I_40000_23',
                                          'B19001_009E':'I_45000_23',
                                          'B19001_010E':'I_50000_23',
                                          'B19001_011E':'I_60000_23',
                                          'B19001_012E':'I_75000_23',
                                          'B19001_013E':'I_100000_23',
                                          'B19001_014E':'I_125000_23',
                                          'B19001_015E':'I_150000_23',
                                          'B19001_016E':'I_200000_23',
                                          'B19001_017E':'I_201000_23'})

# Add 2010 Decennial Census download
var_dict_2010 = c.sf1.get(
    ('P005001', 'P005003', 'H004002', 'H004003'),  # Pop, white, owner, renter
    geo = {'for': 'tract:*', 'in': sql_query}, 
    year=2010)

df_vars_2010 = pd.DataFrame.from_dict(var_dict_2010)
df_vars_2010['FIPS'] = df_vars_2010['state'] + df_vars_2010['county'] + df_vars_2010['tract']
df_vars_2010 = filter_FIPS(df_vars_2010)
df_vars_2010 = df_vars_2010.rename(columns={
    'P005001': 'pop_10',
    'P005003': 'white_10',
    'H004002': 'ohu_10',
    'H004003': 'rhu_10'
})



# Download ACS 2012 5-Year Estimates
# --------------------------------------------------------------------------
# Note: If additional cities are added, make sure to change create_lag_vars.r
# accordingly.

### List variables of interest

df_vars_12=['B25077_001E',
            'B25077_001M',
            'B25064_001E',
            'B25064_001M',
            'B07010_025E',
            'B07010_026E',
            'B07010_027E',
            'B07010_028E',
            'B07010_029E',
            'B07010_030E',
            'B07010_031E',
            'B07010_032E',
            'B07010_033E',
            'B07010_036E',
            'B07010_037E',
            'B07010_038E',
            'B07010_039E',
            'B07010_040E',
            'B07010_041E',
            'B07010_042E',
            'B07010_043E',
            'B07010_044E',
            'B07010_047E',
            'B07010_048E',
            'B07010_049E',
            'B07010_050E',
            'B07010_051E',
            'B07010_052E',
            'B07010_053E',
            'B07010_054E',
            'B07010_055E',
            'B07010_058E',
            'B07010_059E',
            'B07010_060E',
            'B07010_061E',
            'B07010_062E',
            'B07010_063E',
            'B07010_064E',
            'B07010_065E',
            'B07010_066E',
            'B06011_001E']

# Run API query
# --------------------------------------------------------------------------
# NOTE: Memphis is located in two states so the query looks different


var_dict_acs5 = c.acs5.get(df_vars_12, geo = {'for': 'tract:*',
                                 'in': sql_query}, year=2012)


# Convert and Rename Variabls
# --------------------------------------------------------------------------

### Converts variables into dataframe and filters only FIPS of interest

df_vars_12 = pd.DataFrame.from_dict(var_dict_acs5)
df_vars_12['FIPS']=df_vars_12['state']+df_vars_12['county']+df_vars_12['tract']
df_vars_12 = filter_FIPS(df_vars_12)

### Renames variables

df_vars_12 = df_vars_12.rename(columns = {'B25077_001E':'mhval_12',
                                          'B25077_001M':'mhval_12_se',
                                          'B25064_001E':'mrent_12',
                                          'B25064_001M':'mrent_12_se',
                                          'B07010_025E':'mov_wc_w_income_12',
                                          'B07010_026E':'mov_wc_9000_12',
                                          'B07010_027E':'mov_wc_15000_12',
                                          'B07010_028E':'mov_wc_25000_12',
                                          'B07010_029E':'mov_wc_35000_12',
                                          'B07010_030E':'mov_wc_50000_12',
                                          'B07010_031E':'mov_wc_65000_12',
                                          'B07010_032E':'mov_wc_75000_12',
                                          'B07010_033E':'mov_wc_76000_more_12',
                                          'B07010_036E':'mov_oc_w_income_12',
                                          'B07010_037E':'mov_oc_9000_12',
                                          'B07010_038E':'mov_oc_15000_12',
                                          'B07010_039E':'mov_oc_25000_12',
                                          'B07010_040E':'mov_oc_35000_12',
                                          'B07010_041E':'mov_oc_50000_12',
                                          'B07010_042E':'mov_oc_65000_12',
                                          'B07010_043E':'mov_oc_75000_12',
                                          'B07010_044E':'mov_oc_76000_more_12',
                                          'B07010_047E':'mov_os_w_income_12',
                                          'B07010_048E':'mov_os_9000_12',
                                          'B07010_049E':'mov_os_15000_12',
                                          'B07010_050E':'mov_os_25000_12',
                                          'B07010_051E':'mov_os_35000_12',
                                          'B07010_052E':'mov_os_50000_12',
                                          'B07010_053E':'mov_os_65000_12',
                                          'B07010_054E':'mov_os_75000_12',
                                          'B07010_055E':'mov_os_76000_more_12',
                                          'B07010_058E':'mov_fa_w_income_12',
                                          'B07010_059E':'mov_fa_9000_12',
                                          'B07010_060E':'mov_fa_15000_12',
                                          'B07010_061E':'mov_fa_25000_12',
                                          'B07010_062E':'mov_fa_35000_12',
                                          'B07010_063E':'mov_fa_50000_12',
                                          'B07010_064E':'mov_fa_65000_12',
                                          'B07010_065E':'mov_fa_75000_12',
                                          'B07010_066E':'mov_fa_76000_more_12',
                                          'B06011_001E':'iinc_12'})

# ==========================================================================
# NHGIS — Census 2000 SF3 (CSV)
# ==========================================================================

nhgis_2000_file = input_path + 'nhgis_2000_ca.csv'
df_00 = pd.read_csv(nhgis_2000_file, dtype=str)
df_00 = df_00.drop(index=1)  # drop second row with metadata

df_00['FIPS'] = (
    df_00['STATEA'].str.zfill(2) +
    df_00['COUNTYA'].str.zfill(3) +
    df_00['TRACTA'].str.zfill(6)
)

df_00 = df_00[df_00['COUNTYA'].isin(FIPS)]

num_cols = df_00.columns.difference(['GISJOIN','YEAR','STATEA','COUNTYA','TRACTA','FIPS'])
df_00[num_cols] = df_00[num_cols].apply(pd.to_numeric, errors='coerce')

df_vars_00 = df_00.rename(columns={
    'GKT001':'total_25_00',
    'GKT013':'male_25_col_bd_00',
    'GKT014':'male_25_col_md_00',
    'GKT015':'male_25_col_psd_00',
    'GKT016':'male_25_col_phd_00',
    'GKT029':'female_25_col_bd_00',
    'GKT030':'female_25_col_md_00',
    'GKT031':'female_25_col_psd_00',
    'GKT032':'female_25_col_phd_00',
    'GMX001':'hh_00',
    'GNW001':'hinc_00',
    'F9C001':'ohu_00',
    'F9C002':'rhu_00',
    'E001001':'pop_00',
    'E001003':'white_00'
})


# ==========================================================================
# EXPORTS
# ==========================================================================
# Merge 2012 & 2018 files - same geometry

df_vars_00.to_csv(
    output_path + "downloads/" + city_name.replace(" ", "") + "census_00_2023.csv",
    index=False
)

df_vars_2010.to_csv(
    output_path+"downloads/"+city_name.replace(" ", "")+'census_10_2023.csv',
    index=False)

df_vars_12.to_csv(
    output_path + "downloads/" + city_name.replace(" ", "") + "census_12_2023.csv",
    index=False
)

df_vars_23.to_csv(
    output_path + "downloads/" + city_name.replace(" ", "") + "census_23_2023.csv",
    index=False
)
print("Data Download Complete")
