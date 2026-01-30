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
from pathlib import Path

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.options.display.float_format = '{:.2f}'.format # avoid scientific notation

home = str(Path.home())
DATA_Dir="I:\Projects\Josh\RHNA\Data\POPEMP_25\emp25_data"
input_path = DATA_Dir+'/inputs/'
output_path = DATA_Dir+'/outputs/'


# Get the directory where test.py is located
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
FILE_API = os.path.join(BASE_DIR, 'api_key.txt')

with open(FILE_API, 'r') as file:
    key = file.read()


key = key.strip()
c = census.Census(key)


city_name = 'Sacramento'
state = '06'
FIPS = ['067']


sql_query='state:{} county:*'.format(state)


# Create Filter Function
# --------------------------------------------------------------------------


def filter_FIPS(df):
    """
    Filters county to FIPS(currently Sacramento)
    """
    df = df[df['county'].isin(FIPS)]
    return df

    
def load_nhgis_csv(path):
    """Load NHGIS CSV and drop description row"""
    return pd.read_csv(path, skiprows=[1], dtype=str)


def nhgis_to_fips(df):
    """
    Convert NHGIS GISJOIN to standard 11-digit tract FIPS
    GISJOIN Example: G0600010400100
    Resulting FIPS: 06001400100
    """
    # 1. State FIPS (2 digits): Take indices 1 and 2
    # 2. County FIPS (3 digits): Skip index 3 (extra zero), take 4, 5, 6
    # 3. Tract FIPS (6 digits): Skip index 7 (extra zero), take everything after
    
    df['FIPS'] = (
        df['GISJOIN'].str.slice(1, 3)   # '06'
        + df['GISJOIN'].str.slice(4, 7) # '001'
        + df['GISJOIN'].str.slice(8)    # '400100'
    )
    
    return df

def audit_renames(rename_dict, meta_dict, title=None):
    """
    Takes the dictionary for each rename and prints out the column name as well as meta data
    as a visual sanity check
    """
    if title:
        print(f"\n{'='*80}\n{title}\n{'='*80}")

    for old, new in rename_dict.items():
        label = meta_dict.get(old, '⚠️ NO METADATA FOUND')

        print(f"NHGIS label : {label}")
        print(f"Column code : {old}")
        print(f"Renamed to  : {new}")
        print("-" * 80)


def nhgis_metadata(path):
    """
    Takes the second row of the nhgis csv with data descriptions and saves that as a dictionary with the column name as column code and 
    Returns dict: {column_code: description}
    """
    meta = pd.read_csv(path, nrows=2, header=None)
    return dict(zip(meta.iloc[0], meta.iloc[1]))

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

var_dict_acs5 = c.acs5.get(df_vars_23, geo = {'for': 'tract:*','in': sql_query}, year=2023)

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
            'B06011_001E',
            'B19013_001E']

# Run API query
# --------------------------------------------------------------------------


var_dict_acs5 = c.acs5.get(df_vars_12, geo = {'for': 'tract:*','in': sql_query}, year=2012)


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
                                          'B06011_001E':'iinc_12',
                                          'B19013_001E':'hinc_12'})

### Decennial Census 2000 Variables
# ======================================================================
# Decennial Census 2000 — NHGIS
# ======================================================================

path_sf1_00 = input_path + "nhgis_ca_sf1_2000_tract.csv"
path_sf3_00 = input_path + "nhgis_ca_sf3_2000_tract.csv"

df_sf1_00 = load_nhgis_csv(path_sf1_00)
df_sf3_00 = load_nhgis_csv(path_sf3_00)

df_sf1_00 = nhgis_to_fips(df_sf1_00)
df_sf3_00 = nhgis_to_fips(df_sf3_00)

# filter to Sacramento County
df_sf1_00 = df_sf1_00[df_sf1_00['COUNTYA'].isin(FIPS)]
df_sf3_00 = df_sf3_00[df_sf3_00['COUNTYA'].isin(FIPS)]
# -------------------------------
# Rename variables (MATCHES YOUR SCRIPT)
# -------------------------------
# Create a list of the columns to sum
pop_cols_00 = ['FMR001', 'FMR002', 'FMR003', 'FMR004', 'FMR005', 'FMR006', 'FMR007']

# Convert them to numeric first, then sum across the row (axis=1)
df_sf1_00['total_pop_00'] = df_sf1_00[pop_cols_00].apply(pd.to_numeric).fillna(0).sum(axis=1)							

df_sf1_00 = df_sf1_00.rename(columns={
    'total_pop_00': 'pop_00',      # Total population
    'FMR001': 'white_00',    # White alone
    'FKI001': 'hu_00',       # Housing units
    'FKM001': 'ohu_00',      # Owner occupied housing units
    'FKN002': 'rhu_00'       # Renter occupied housing units
})

sfl_dict_00 = {
    'total_pop_00': 'pop_00',      # Total population
    'FMR001': 'white_00',    # White alone
    'FKI001': 'hu_00',       # Housing units
    'FKM001': 'ohu_00',      # Owner occupied housing units
    'FKN002': 'rhu_00'       # Renter occupied housing units
}

# Create a list of columns for the count of households by language
hh_cols_00 = ['GI6001', 'GI6002', 'GI6003', 'GI6004', 'GI6005']
# Convert them to numeric first, then sum across the row (axis=1) to get total housholds
df_sf3_00['hh_00'] = df_sf3_00[hh_cols_00].apply(pd.to_numeric).fillna(0).sum(axis=1)

sf3_dict_00 = {
    #total population under 25
    'GKR001': 'total_25_00',
    # educational attainment under 25
    'GKT013': 'male_25_col_bd_00',
    'GKT014': 'male_25_col_md_00',
    'GKT015': 'male_25_col_psd_00',
    'GKT016': 'male_25_col_phd_00',

    'GKT029': 'female_25_col_bd_00',
    'GKT030': 'female_25_col_md_00',
    'GKT031': 'female_25_col_psd_00',
    'GKT031': 'female_25_col_phd_00',
    # house value
    'GB7001': 'mhval_00',
    # median rent
    'GBO001': 'mrent_00',
    # total houses
    'total households': 'hh_00',
    # median household  income
    'GMY001': 'hinc_00',

    # income bins
    'GMX001': 'I_10000_00',
    'GMX002': 'I_15000_00',
    'GMX003': 'I_20000_00',
    'GMX004': 'I_25000_00',
    'GMX005': 'I_30000_00',
    'GMX006': 'I_35000_00',
    'GMX007': 'I_40000_00',
    'GMX008': 'I_45000_00',
    'GMX009': 'I_50000_00',
    'GMX010': 'I_60000_00',
    'GMX011': 'I_75000_00',
    'GMX012': 'I_100000_00',
    'GMX013': 'I_125000_00',
    'GMX014': 'I_150000_00',
    'GMX015': 'I_200000_00',
    'GMX016': 'I_201000_00'
}
		

df_sf3_00 = df_sf3_00.rename(columns={
    #total population under 25
    'GKR001': 'total_25_00',
    # educational attainment under 25
    'GKT013': 'male_25_col_bd_00',
    'GKT014': 'male_25_col_md_00',
    'GKT015': 'male_25_col_psd_00',
    'GKT016': 'male_25_col_phd_00',

    'GKT029': 'female_25_col_bd_00',
    'GKT030': 'female_25_col_md_00',
    'GKT031': 'female_25_col_psd_00',
    'GKT031': 'female_25_col_phd_00',
    # house value
    'GB7001': 'mhval_00',
    # median rent
    'GBO001': 'mrent_00',
    # total houses
    'total households': 'hh_00',
    # median household  income
    'GMY001': 'hinc_00',

    # income bins
    'GMX001': 'I_10000_00',
    'GMX002': 'I_15000_00',
    'GMX003': 'I_20000_00',
    'GMX004': 'I_25000_00',
    'GMX005': 'I_30000_00',
    'GMX006': 'I_35000_00',
    'GMX007': 'I_40000_00',
    'GMX008': 'I_45000_00',
    'GMX009': 'I_50000_00',
    'GMX010': 'I_60000_00',
    'GMX011': 'I_75000_00',
    'GMX012': 'I_100000_00',
    'GMX013': 'I_125000_00',
    'GMX014': 'I_150000_00',
    'GMX015': 'I_200000_00',
    'GMX016': 'I_201000_00'
})

# combing sf3 and sf1 data for 2000 into one dataframe
df_vars_00 = df_sf1_00.merge(
    df_sf3_00.drop(columns=['COUNTYA']),
    on='FIPS',
    how='left'
)

# ======================================================================
# Decennial Census 1990 — NHGIS
# ======================================================================

path_sf3_90 = input_path + "nhgis_ca_sf3_1990_tract.csv"

df_vars_90 = load_nhgis_csv(path_sf3_90)
df_vars_90 = nhgis_to_fips(df_vars_90)
# Columns to sum
pop_cols_90 = ['E4S001', 'E4S002', 'E4S003', 'E4S004', 'E4S005']
# Convert to numeric then sum rows
df_vars_90['pop_90'] = df_vars_90[pop_cols_90].apply(pd.to_numeric).fillna(0).sum(axis=1)		

df_vars_90 = df_vars_90.rename(columns={
    # population & race
    'total_pop_90': 'pop_90',
    'E4S001': 'white_90',

    # housing totals
    'EXQ001': 'hu_90',        # total housing units
    'EZ2001': 'ohu_90',     # owner-occupied
    'EZ2002': 'rhu_90',    # renter-occupied

    # education (25+)
    'E33001': 'total_25_col_9th_90',
    'E33002': 'total_25_col_12th_90',
    'E33003': 'total_25_col_hs_90',
    'E33004': 'total_25_col_sc_90',
    'E33005': 'total_25_col_ad_90',
    'E33006': 'total_25_col_bd_90',
    'E33007': 'total_25_col_gd_90',

    # income
    'E4U001': 'hinc_90',       # median household income (1989)

    # households
    'E3V001': 'hh_90',        # total households
    # housing costs
    'EYU001': 'mrent_90',      # median gross rent
    'EZI001': 'mhval_90',      # median home value

    # household income bins
    'E4T001': 'I_5000_90',
    'E4T002': 'I_10000_90',
    'E4T003': 'I_12500_90',
    'E4T004': 'I_15000_90',
    'E4T005': 'I_17500_90',
    'E4T006': 'I_20000_90',
    'E4T007': 'I_22500_90',
    'E4T008': 'I_25000_90',
    'E4T009': 'I_27500_90',
    'E4T010': 'I_30000_90',
    'E4T011': 'I_32500_90',
    'E4T012': 'I_35000_90',
    'E4T013': 'I_37500_90',
    'E4T014': 'I_40000_90',
    'E4T015': 'I_42500_90',
    'E4T016': 'I_45000_90',
    'E4T017': 'I_47500_90',
    'E4T018': 'I_50000_90',
    'E4T019': 'I_55000_90',
    'E4T020': 'I_60000_90',
    'E4T021': 'I_75000_90',
    'E4T022': 'I_100000_90',
    'E4T023': 'I_125000_90',
    'E4T024': 'I_150000_90',
    'E4T025': 'I_150001_90',
})
sf3_dict_90 = {
    # population & race
    'total_pop_90': 'pop_90',
    'E4S001': 'white_90',

    # housing totals
    'EXQ001': 'hu_90',        # total housing units
    'EZ2001': 'ohu_90',     # owner-occupied
    'EZ2002': 'rhu_90',    # renter-occupied

    # education (25+)
    'E33001': 'total_25_col_9th_90',
    'E33002': 'total_25_col_12th_90',
    'E33003': 'total_25_col_hs_90',
    'E33004': 'total_25_col_sc_90',
    'E33005': 'total_25_col_ad_90',
    'E33006': 'total_25_col_bd_90',
    'E33007': 'total_25_col_gd_90',

    # income
    'E4U001': 'hinc_90',       # median household income (1989)

    # housing costs
    'EYU001': 'mrent_90',      # median gross rent
    'EZI001': 'mhval_90',      # median home value

    # household income bins
    'E4T001': 'I_5000_90',
    'E4T002': 'I_10000_90',
    'E4T003': 'I_12500_90',
    'E4T004': 'I_15000_90',
    'E4T005': 'I_17500_90',
    'E4T006': 'I_20000_90',
    'E4T007': 'I_22500_90',
    'E4T008': 'I_25000_90',
    'E4T009': 'I_27500_90',
    'E4T010': 'I_30000_90',
    'E4T011': 'I_32500_90',
    'E4T012': 'I_35000_90',
    'E4T013': 'I_37500_90',
    'E4T014': 'I_40000_90',
    'E4T015': 'I_42500_90',
    'E4T016': 'I_45000_90',
    'E4T017': 'I_47500_90',
    'E4T018': 'I_50000_90',
    'E4T019': 'I_55000_90',
    'E4T020': 'I_60000_90',
    'E4T021': 'I_75000_90',
    'E4T022': 'I_100000_90',
    'E4T023': 'I_125000_90',
    'E4T024': 'I_150000_90',
    'E4T025': 'I_150001_90',
}

meta_90     = nhgis_metadata(input_path + 'nhgis_ca_sf3_1990_tract.csv')
meta_00_sf1 = nhgis_metadata(input_path + 'nhgis_ca_sf1_2000_tract.csv')
meta_00_sf3 = nhgis_metadata(input_path + 'nhgis_ca_sf3_2000_tract.csv')

audit_renames(sf3_dict_90, meta_90, title="1990 SF3 Variable Renames")
audit_renames(sfl_dict_00, meta_00_sf1, title="2000 SF1 Variable Renames")
audit_renames(sf3_dict_00, meta_00_sf3, title="2000 SF3 Variable Renames")

# ==========================================================================
# Export Files
# ==========================================================================
# Note: ouput paths can be altered by changing the 'output path variable above'

# Merge 2012 & 2023 files they are both tablulated on 2010 census tract and will be until 2025 acs5 yr
df_vars_summ = df_vars_23.merge(df_vars_12, on ='FIPS')

#Export files to CSV
df_vars_summ.to_csv(output_path+"downloads/"+city_name.replace(" ", "")+'census_summ_2023.csv')
df_vars_90.to_csv(output_path+"downloads/"+city_name.replace(" ", "")+'census_90_2023.csv')
df_vars_00.to_csv(output_path+"downloads/"+city_name.replace(" ", "")+'census_00_2023.csv')

