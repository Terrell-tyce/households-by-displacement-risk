# ==========================================================================
# ==========================================================================
# ==========================================================================
# DATA CURATION
# ==========================================================================
# ==========================================================================
# ==========================================================================
# Note: (input needed) in title bars indicates that in order to bring in new city information
# users must input new code in the section

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

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.options.display.float_format = '{:.2f}'.format # avoid scientific notation

home = str(Path.home())
input_path = home+'/Downloads/households-by-displacement-risk/data/inputs/'
output_path = home+'/Downloads/households-by-displacement-risk/data/outputs/'

# Get the directory where data_curation.py is located and then find key
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
KEY_API = os.path.join(BASE_DIR, 'api_key.txt')

with open(KEY_API, 'r') as file:
    key = file.read()
    
key = key.strip()
c = census.Census(key)
city_name = 'Sacramento'

# ==========================================================================
# ==========================================================================
# ==========================================================================
# Crosswalk Files
# ==========================================================================
# ==========================================================================
# ==========================================================================

# ==========================================================================
# Read Files
# ==========================================================================
# Note: Most of the input files are located on google drive.
# UDP suggests downloading [Google's Drive File Stream](https://support.google.com/a/answer/7491144?utm_medium=et&utm_source=aboutdrive&utm_content=getstarted&utm_campaign=en_us)
# app, which doesn't download all Google Drive items to your computer
# but rather pulls them as necessary. This will save a lot of space but compromises speed.

# Data files
census_90 = pd.read_csv(output_path+'downloads/'+city_name.replace(" ", "")+'census_90_2023.csv', index_col = 0,dtype={'FIPS':str})
census_00 = pd.read_csv(output_path+'downloads/'+city_name.replace(" ", "")+'census_00_2023.csv', index_col = 0,dtype={'FIPS':str})

# Crosswalk files
xwalk_90_10 = pd.read_csv(input_path+'crosswalk_1990_2010.csv',dtype={'trtid90':str,
                                                                      'trtid10':str})
xwalk_00_10 = pd.read_csv(input_path+'crosswalk_2000_2010.csv',dtype={'trtid00':str,
                                                                      'trtid10':str})
# ==========================================================================
# Choose Census Tract (inputs needed)
# ==========================================================================
# Note: In order to add your city below, add a 'elif' statement similar
# to those already written


state = '06'
FIPS = ['067']

# ==========================================================================
# Create Crosswalk Functions / Files
# ==========================================================================

# Create a Filter Function
# --------------------------------------------------------------------------
# Note - Memphis and Boston are different bc they are located in 2 states

def filter_FIPS(df):
    df = df[df['county'].isin(FIPS)].reset_index(drop = True)
    return df


def crosswalk_files (df, xwalk, counts, medians, df_fips_base, xwalk_fips_base, xwalk_fips_horizon):
    # merge dataframe with xwalk file
    df_merge = df.merge(xwalk[['weight', xwalk_fips_base, xwalk_fips_horizon]], left_on = df_fips_base, right_on = xwalk_fips_base, how='left')
    df = df_merge
    # apply interpolation weight
    new_var_list = list(counts)+list(medians)
   
    for var in new_var_list:
        df[var] = pd.to_numeric(df[var]).fillna(0).astype(float)
        df[var] = df[var]*df['weight']
    # aggregate by horizon census tracts fips
    df = df.groupby(xwalk_fips_horizon).sum(numeric_only=True).reset_index()
    # rename trtid10 to FIPS & FIPS to trtid_base
    df = df.rename(columns = {'FIPS':'trtid_base',
                              xwalk_fips_horizon:'FIPS'})
    
    fips_str = df['FIPS'].astype(str).str.zfill(11)

    # fix state, county and fips code
    df ['state'] = fips_str.str[0:2]
    df ['county'] = fips_str.str[2:5]
    df ['tract'] = fips_str.str[5:]
    # drop weight column
    df = df.drop(columns = ['weight'])
    return df

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
# Crosswalking
# --------------------------------------------------------------------------

## 1990 Census Data
counts = census_90.columns.drop(['GISJOIN',	'YEAR',	'STUSAB',	'ANRCA',	'AIANHHA',	'RES_ONLYA'
,	'TRUSTA',	'AIANCC',	'RES_TRSTA',	'BLCK_GRPA',	'TRACTA',	'CD101A',	'C_CITYA',	
'CMSA',	'COUNTY',	'COUNTYA',	'CTY_SUBA',	'COUSUBCC',	'DIVISIONA',	'MSA_CMSAA',	'PLACEA',
'PLACECC',	'PLACEDC',	'PMSAA',	'REGIONA',	'STATE',	'STATEA',	'URBRURALA',	
'URB_AREAA',	'ZIPA',	'CD103A',	'AREALAND',	'AREAWAT',	'ANPSADPI',	'FUNCSTAT',	'INTPTLAT',	
'INTPTLNG',	'PSADC', 'mrent_90', 'mhval_90', 'hinc_90', 'FIPS'])

# Pad the 9-digit FIPS to 11 digits by adding '00' to the end
census_90['FIPS'] = census_90['FIPS'].apply(lambda x: x + '00' if len(str(x)) == 9 else x)

# Make sure census_90 is filtered to Sacramento before the crosswalk
census_90 = census_90[census_90['FIPS'].str.startswith('06067')].copy()


# Filter crosswalk to only include Sacramento 1990 tracts
xwalk_90_10 = xwalk_90_10[xwalk_90_10['trtid90'].str.startswith('06067')].copy()

# Use crosswalk to update tracts from 1990 to 2010
medians = ['mrent_90', 'mhval_90', 'hinc_90']
df_fips_base = 'FIPS'
xwalk_fips_base = 'trtid90'
xwalk_fips_horizon = 'trtid10'
census_90_xwalked = crosswalk_files (census_90, xwalk_90_10,  counts, medians, df_fips_base, xwalk_fips_base, xwalk_fips_horizon )



## 2000 Census Data
counts = census_00.columns.drop(['GISJOIN_x',	'YEAR_x',	'STUSAB_x',	'REGIONA_x',	'DIVISIONA_x',	'STATE_x',	'STATEA_x',	
                                 'COUNTY_x',	'COUNTYA',	'CTY_SUBA_x',	'COUSUBCC_x',	'PLACEA_x',	'PLACECC_x',	'PLACEDC_x',	
                                 'TRACTA_x',	'TRBL_CTA_x',	'BLCK_GRPA_x',	'TRBL_BGA_x',	'C_CITYA_x',	'AIANHHA_x',	
                                 'RES_ONLYA_x',	'TRUSTA_x',	'AIANHHCC_x',	'AITSCE_x',	'TRBL_SUBA_x',	'ANRCA_x',	'MSA_CMSAA_x',	
                                 'CMSA_x',	'MACCI_x',	'PMSAA_x',	'NECMAA_x',	'NECMACCI_x',	'URB_AREAA_x',	'UATYPE_x',	'CD106A_x',	
                                 'CD108A_x',	'CD109A_x',	'ZIP3A_x',	'ZCTAA_x',	'AREALAND_x',	'AREAWATR_x',	'NAME_x',	
                                 'FUNCSTAT_x',	'INTPTLAT_x',	'INTPLON_x',	'LSADC_x',	'MACC_x',	'UACP_x', 'GISJOIN_y',	'YEAR_y',
                                 'STUSAB_y',	'REGIONA_y',	'DIVISIONA_y',	'STATE_y',	'STATEA_y',	'COUNTY_y',	'CTY_SUBA_y',	
                                 'COUSUBCC_y',	'PLACEA_y'	,'PLACECC_y',	'PLACEDC_y'	,'TRACTA_y',	'TRBL_CTA_y',	'BLCK_GRPA_y',	
                                 'TRBL_BGA_y',	'C_CITYA_y',	'AIANHHA_y',	'RES_ONLYA_y',	'TRUSTA_y',	'AIANHHCC_y',	'AITSCE_y',	
                                 'TRBL_SUBA_y',	'ANRCA_y',	'MSA_CMSAA_y',	'CMSA_y',	'MACCI_y',	'PMSAA_y',	'NECMAA_y',	'NECMACCI_y',
                                 'URB_AREAA_y',	'UATYPE_y',	'URBRURALA',	'CD106A_y'	,'CD108A_y'	,'CD109A_y',	'ZIP3A_y',	'ZCTAA_y',
                                 'AREALAND_y',	'AREAWATR_y',	'NAME_y',	'FUNCSTAT_y',	'INTPTLAT_y', 'INTPLON_y',	'LSADC_y',	'MACC_y',	'UACP_y',
                                 'mrent_00', 'mhval_00', 'hinc_00', 'FIPS'])

# Pad the 9-digit FIPS to 11 digits by adding '00' to the end
census_00['FIPS'] = census_00['FIPS'].apply(lambda x: x + '00' if len(str(x)) == 9 else x)

# Make sure census_90 is filtered to Sacramento before the crosswalk
census_00 = census_00[census_00['FIPS'].str.startswith('06067')].copy()


# Filter crosswalk to only include Sacramento 1990 tracts
xwalk_00_10 = xwalk_00_10[xwalk_00_10['trtid00'].str.startswith('06067')].copy()

medians = ['mrent_00', 'mhval_00', 'hinc_00']
df_fips_base = 'FIPS'
xwalk_fips_base = 'trtid00'
xwalk_fips_horizon = 'trtid10'
census_00_xwalked = crosswalk_files (census_00, xwalk_00_10,  counts, medians, df_fips_base, xwalk_fips_base, xwalk_fips_horizon )

## Filters and exports data
census_90_filtered = filter_FIPS(census_90_xwalked)
census_00_filtered = filter_FIPS(census_00_xwalked)

# ==========================================================================
# ==========================================================================
# ==========================================================================
# Variable Creation
# ==========================================================================
# ==========================================================================
# ==========================================================================

# ==========================================================================
# Setup / Read Files (inputs needed)
# ==========================================================================

shp_folder = input_path+'shp/'+city_name.replace(" ", "")+'/'
data_1990 = census_90_filtered
data_2000 = census_00_filtered

acs_data = pd.read_csv(output_path+'downloads/'+city_name.replace(" ", "")+'census_summ_2023.csv', index_col = 0,dtype={'FIPS':str})
acs_data = acs_data.drop(columns = ['county_y', 'state_y', 'tract_y'])
acs_data = acs_data.rename(columns = {'county_x': 'county',
                                    'state_x': 'state',
                                    'tract_x': 'tract'})

# Clean sentinel/missing values from Census API
SENTINEL_VALUES = [-666666666, -222222222, -333333333, -999999999]

housing_cols = [col for col in acs_data.columns if 'mhval' in col or 'mrent' in col or 'mmhcosts' in col]
sentintel_tract_list=[]
for col in housing_cols:
    # Find which tracts have sentinel values BEFORE replacing
    sentinel_mask = acs_data[col].isin(SENTINEL_VALUES)
    tracts_with_sentinels = acs_data[sentinel_mask]['FIPS'].tolist()
    
    # Replace with NaN
    acs_data[col] = acs_data[col].replace(SENTINEL_VALUES, np.nan)
    
    # Report what was cleaned
    cleaned_count = (acs_data[col].isna()).sum()
    if cleaned_count > 0:
        print(f"⚠️  {col}: Cleaned {cleaned_count} sentinel values")
        if tracts_with_sentinels:
            print(f"   Tracts affected: {tracts_with_sentinels}")
            for tract in tracts_with_sentinels:
                sentintel_tract_list.append(tract)
            
sentintel_tract_list=set(sentintel_tract_list)
print(f"unique tracts with invalid zillow values{sentintel_tract_list}")
# Bring in PUMS data
# --------------------------------------------------------------------------

pums_r = pd.read_csv(input_path+'nhgis0002_ds267_20235_tract.csv', encoding = "ISO-8859-1",skiprows=[1],dtype={'STATEA':str,'COUNTYA':str}) #Gross Rent B25063
pums_o = pd.read_csv(input_path+'nhgis0002_ds268_20235_tract.csv', encoding = "ISO-8859-1",skiprows=[1],dtype={'STATEA':str,'COUNTYA':str}) #Owner Costs B25094

pums = pums_r.merge(pums_o, on = 'GISJOIN')

pums = pums.rename(columns = {'YEAR_x':'YEAR',
                               'STATE_x':'STATE',
                               'STATEA_x':'STATEA',
                               'COUNTY_x':'COUNTY',
                               'COUNTYA_x':'COUNTYA',
                               'TRACTA_x':'TRACTA',
                               'NAME_E_x':'NAME_E'})
pums = pums.dropna(axis = 1)

# Bring in Zillow, Rail, Hospital, Unversity, LIHTC, PH dat
# --------------------------------------------------------------------------
# Note: Make sure your city/county is included in these overlay files

## Zillow data
zillow = pd.read_csv(input_path+'Zip_Zhvi_AllHomes.csv', encoding = "ISO-8859-1")
zillow_xwalk = pd.read_csv(input_path+'TRACT_ZIP_032015.csv')

## Rail data
rail = pd.read_csv(input_path+'tod_database_download.csv')

## Hospitals
hospitals = pd.read_csv(input_path+'Hospitals.csv')

## Universities
university = pd.read_csv(input_path+'university_HD2023.csv')

## LIHTC
lihtc = pd.read_csv(input_path+'../overlays/LowIncome_Housing_Tax_Credit_Properties.csv',dtype={'PROJ_ZIP':str,'URB_OUT':str,'NECTA_NM':str})

## Public housing
pub_hous = pd.read_csv(input_path+'../overlays/Public_Housing_Buildings.csv',dtype={'URB_OUT':str})

# ==========================================================================
# Read Shapefile Data (inputs needed)
# ==========================================================================
# Note: Similar to above, add a 'elif' for you city here
# Pull cartographic boundary files from here:
# https://www.census.gov/geographies/mapping-files/time-series/geo/carto-boundary-file.2017.html


shp_name = 'cb_2024_06_tract_500k.shp'


city_shp = gpd.read_file(shp_folder+'cb_2024_06_tract_500k/'+shp_name)

# Define City Specific Variables
# --------------------------------------------------------------------------
# Note: Choose city and define city specific variables
# Add a new 'elif' for your city here


state = '06'
state_init = ['CA']
FIPS = ['067']
rail_agency = ['SacRT'] # 
zone = '10'


# ==========================================================================
# Income Interpolation
# ==========================================================================

# Merge census data in single file
# --------------------------------------------------------------------------
print("Preparing census data for income interpolation...")
# Save to the current working directory for inspection
census = acs_data.merge(data_2000, on = 'FIPS', how = 'outer').merge(data_1990, on = 'FIPS', how = 'outer')

## CPI indexing values
## This is based on the yearly CPI average
## Add in new CPI based on current year: https://www.bls.gov/data/inflation_calculator.htm
CPI_89_23 = 2.53
CPI_99_23 = 1.87
CPI_12_23 = 1.35

## This is used for the Zillow data, where january values are compared
CPI_0115_0124 = 1.32

# Income Interpolation
# --------------------------------------------------------------------------

census.loc[census['hinc_23'] < 0, 'hinc_23'] = np.nan
census.loc[census['hinc_00'] < 0, 'hinc_00'] = np.nan
census.loc[census['hinc_90'] < 0, 'hinc_90'] = np.nan


## Calculate regional medians (note that these are not indexed)
rm_hinc_23 = np.nanmedian(census['hinc_23'])
rm_hinc_00 = np.nanmedian(census['hinc_00'])
rm_hinc_90 = np.nanmedian(census['hinc_90'])
rm_iinc_23 = np.nanmedian(census['iinc_23'])
rm_iinc_12 = np.nanmedian(census['iinc_12'])

print(rm_hinc_23, rm_hinc_00,rm_hinc_90, rm_iinc_23, rm_iinc_12)

## Income Interpolation Function
## This function interpolates population counts using income buckets provided by the Census

def income_interpolation (census, year, cutoff, mhinc, tot_var, var_suffix, out):
    name = []
    for c in list(census.columns):
        if c.startswith(var_suffix) and c.endswith(f"_{year}"):
            name.append(c)

    name.append('FIPS')
    name.append(tot_var)
    income_cat = census[name].copy()
    income_group = income_cat.drop(columns = ['FIPS', tot_var]).columns
    income_group = income_group.str.split('_')
    number = []
    for i in range (0, len(income_group)):
        number.append(income_group[i][1])
    column = []
    for i in number:
        column.append('prop_'+str(i))
        income_cat['prop_'+str(i)] = income_cat[var_suffix+'_'+str(i)+'_'+year]/income_cat[tot_var]
    reg_median_cutoff = cutoff*mhinc
    cumulative = out+str(int(cutoff*100))+'_cumulative'
    income = out+str(int(cutoff*100))+'_'+year
    df = income_cat
    df[cumulative] = 0
    df[income] = 0
    for i in range(0,(len(number)-1)):
        a = (number[i])
        b = float(number[i+1])-0.01
        prop = str(number[i+1])
        df[cumulative] = df[cumulative]+df['prop_'+a]
        if (reg_median_cutoff>=int(a))&(reg_median_cutoff<b):
            df[income] = ((reg_median_cutoff - int(a))/(b-int(a)))*df['prop_'+prop] + df[cumulative]
    df = df.drop(columns = [cumulative])
    prop_col = df.columns[df.columns.str[0:4]=='prop']
    df = df.drop(columns = prop_col)
    census = census.merge (df[['FIPS', income]], on = 'FIPS')
    return census

census = income_interpolation (census, '23', 0.8, rm_hinc_23, 'hh_23', 'I', 'inc')
census = income_interpolation (census, '23', 1.2, rm_hinc_23, 'hh_23', 'I', 'inc')


census = income_interpolation (census, '00', 0.8, rm_hinc_00, 'hh_00', 'I', 'inc')
census = income_interpolation (census, '00', 1.2, rm_hinc_00, 'hh_00', 'I', 'inc')

census = income_interpolation (census, '90', 0.8, rm_hinc_90, 'hh_90', 'I', 'inc')
census = income_interpolation (census, '90', 1.2, rm_hinc_90, 'hh_90', 'I', 'inc')

income_col = census.columns[census.columns.str[0:2]=='I_']
census = census.drop(columns = income_col)

# ==========================================================================
# Generate Income Categories
# ==========================================================================

# Create Category Function + Run
# --------------------------------------------------------------------------

def income_categories (df, year, mhinc, hinc):
    df['hinc_'+year] = np.where(df['hinc_'+year]<0, 0, df['hinc_'+year])
    reg_med_inc80 = 0.8*mhinc
    reg_med_inc120 = 1.2*mhinc
    low = 'low_80120_'+year
    mod = 'mod_80120_'+year
    high = 'high_80120_'+year
    df[low] = df['inc80_'+year]
    df[mod] = df['inc120_'+year] - df['inc80_'+year]
    df[high] = 1 - df['inc120_'+year]
    ## Low income
    df['low_pdmt_medhhinc_'+year] = np.where((df['low_80120_'+year]>=0.55)&(df['mod_80120_'+year]<0.45)&(df['high_80120_'+year]<0.45),1,0)
    ## High income
    df['high_pdmt_medhhinc_'+year] = np.where((df['low_80120_'+year]<0.45)&(df['mod_80120_'+year]<0.45)&(df['high_80120_'+year]>=0.55),1,0)
    ## Moderate income
    df['mod_pdmt_medhhinc_'+year] = np.where((df['low_80120_'+year]<0.45)&(df['mod_80120_'+year]>=0.55)&(df['high_80120_'+year]<0.45),1,0)
    ## Mixed-Low income
    df['mix_low_medhhinc_'+year] = np.where((df['low_pdmt_medhhinc_'+year]==0)&
                                                  (df['mod_pdmt_medhhinc_'+year]==0)&
                                                  (df['high_pdmt_medhhinc_'+year]==0)&
                                                  (df[hinc]<reg_med_inc80),1,0)
    ## Mixed-Moderate income
    df['mix_mod_medhhinc_'+year] = np.where((df['low_pdmt_medhhinc_'+year]==0)&
                                                  (df['mod_pdmt_medhhinc_'+year]==0)&
                                                  (df['high_pdmt_medhhinc_'+year]==0)&
                                                  (df[hinc]>=reg_med_inc80)&
                                                  (df[hinc]<reg_med_inc120),1,0)
    ## Mixed-High income
    df['mix_high_medhhinc_'+year] = np.where((df['low_pdmt_medhhinc_'+year]==0)&
                                                  (df['mod_pdmt_medhhinc_'+year]==0)&
                                                  (df['high_pdmt_medhhinc_'+year]==0)&
                                                  (df[hinc]>=reg_med_inc120),1,0)
    df['inc_cat_medhhinc_'+year] = ""
    df.loc[df['low_pdmt_medhhinc_'+year]==1, 'inc_cat_medhhinc_'+year] = 1
    df.loc[df['mix_low_medhhinc_'+year]==1, 'inc_cat_medhhinc_'+year] = 2
    df.loc[df['mod_pdmt_medhhinc_'+year]==1, 'inc_cat_medhhinc_'+year] = 3
    df.loc[df['mix_mod_medhhinc_'+year]==1, 'inc_cat_medhhinc_'+year] = 4
    df.loc[df['mix_high_medhhinc_'+year]==1, 'inc_cat_medhhinc_'+year] = 5
    df.loc[df['high_pdmt_medhhinc_'+year]==1, 'inc_cat_medhhinc_'+year] = 6
    df['inc_cat_medhhinc_encoded'+year] = ""
    df.loc[df['low_pdmt_medhhinc_'+year]==1, 'inc_cat_medhhinc_encoded'+year] = 'low_pdmt'
    df.loc[df['mix_low_medhhinc_'+year]==1, 'inc_cat_medhhinc_encoded'+year] = 'mix_low'
    df.loc[df['mod_pdmt_medhhinc_'+year]==1, 'inc_cat_medhhinc_encoded'+year] = 'mod_pdmt'
    df.loc[df['mix_mod_medhhinc_'+year]==1, 'inc_cat_medhhinc_encoded'+year] = 'mix_mod'
    df.loc[df['mix_high_medhhinc_'+year]==1, 'inc_cat_medhhinc_encoded'+year] = 'mix_high'
    df.loc[df['high_pdmt_medhhinc_'+year]==1, 'inc_cat_medhhinc_encoded'+year] = 'high_pdmt'
    df.loc[df['hinc_'+year]==0, 'low_pdmt_medhhinc_'+year] = np.nan
    df.loc[df['hinc_'+year]==0, 'mix_low_medhhinc_'+year] = np.nan
    df.loc[df['hinc_'+year]==0, 'mod_pdmt_medhhinc_'+year] = np.nan
    df.loc[df['hinc_'+year]==0, 'mix_mod_medhhinc_'+year] = np.nan
    df.loc[df['hinc_'+year]==0, 'mix_high_medhhinc_'+year] = np.nan
    df.loc[df['hinc_'+year]==0, 'high_pdmt_medhhinc_'+year] = np.nan
    df.loc[df['hinc_'+year]==0, 'inc_cat_medhhinc_'+year] = np.nan
    return census

census = income_categories(census, '23', rm_hinc_23, 'hinc_23')
census = income_categories(census, '00', rm_hinc_00, 'hinc_00')

census.groupby('inc_cat_medhhinc_00').count()['FIPS']

census.groupby('inc_cat_medhhinc_23').count()['FIPS']

## Percentage & total low-income households - under 80% AMI
census ['per_all_li_90'] = census['inc80_90']
census ['per_all_li_00'] = census['inc80_00']
census ['per_all_li_23'] = census['inc80_23']

census['all_li_count_90'] = census['per_all_li_90']*census['hh_90']
census['all_li_count_00'] = census['per_all_li_00']*census['hh_00']
census['all_li_count_23'] = census['per_all_li_23']*census['hh_23']

len(census)

# ==========================================================================
# Rent, Median income, Home Value Data
# ==========================================================================

census['real_mhval_90'] = census['mhval_90']*CPI_89_23
census['real_mrent_90'] = census['mrent_90']*CPI_89_23
census['real_hinc_90'] = census['hinc_90']*CPI_89_23

census['real_mhval_00'] = census['mhval_00']*CPI_99_23
census['real_mrent_00'] = census['mrent_00']*CPI_99_23
census['real_hinc_00'] = census['hinc_00']*CPI_99_23

census['real_mhval_12'] = census['mhval_12']*CPI_12_23
census['real_mrent_12'] = census['mrent_12']*CPI_12_23
#census['real_hinc_12'] = census['hinc_12']*CPI_12_23 # this isn't calculated yet (2020.03.29)

census['real_mhval_23'] = census['mhval_23']
census['real_mrent_23'] = census['mrent_23']
census['real_hinc_23'] = census['hinc_23']

# ==========================================================================
# Demographic Data
# ==========================================================================

df = census

# In script 2, check income distribution:
print(df[['low_80120_23', 'mod_80120_23', 'high_80120_23']].describe())

# How many tracts meet the 55% threshold for moderate income?
print((df['mod_80120_23'] >= 0.55).sum())  # Should be > 0

# % of non-white
# --------------------------------------------------------------------------

## 1990
df['per_nonwhite_90'] = 1 - df['white_90']/df['pop_90']

## 2000
df['per_nonwhite_00'] = 1 - df['white_00']/df['pop_00']

## 2023
df['per_nonwhite_23'] = 1 - df['white_23']/df['pop_23']

# % of owner and renter-occupied housing units
# --------------------------------------------------------------------------

## 1990
df['per_rent_90'] = df['rhu_90']/df['hu_90']

## 2000
df['per_rent_00'] = df['rhu_00']/df['hu_00']

## 2023
df['hu_23'] = df['ohu_23']+df['rhu_23']
df['per_rent_23'] = df['rhu_23']/df['hu_23']

# % of college educated
# --------------------------------------------------------------------------

## 1990
var_list = ['total_25_col_9th_90',
            'total_25_col_12th_90',
            'total_25_col_hs_90',
            'total_25_col_sc_90',
            'total_25_col_ad_90',
            'total_25_col_bd_90',
            'total_25_col_gd_90']
df['total_25_90'] = df[var_list].sum(axis = 1)
df['per_col_90'] = (df['total_25_col_bd_90']+df['total_25_col_gd_90'])/(df['total_25_90'])

## 2000
df['male_25_col_00'] = (df['male_25_col_bd_00']+
                        df['male_25_col_md_00']+
                        df['male_25_col_phd_00'])
df['female_25_col_00'] = (df['female_25_col_bd_00']+
                          df['female_25_col_md_00']+
                          df['female_25_col_phd_00'])
df['total_25_col_00'] = df['male_25_col_00']+df['female_25_col_00']
df['per_col_00'] = df['total_25_col_00']/df['total_25_00']

## 2023
df['per_col_23'] = (df['total_25_col_bd_23']+
                    df['total_25_col_md_23']+
                    df['total_25_col_pd_23']+
                    df['total_25_col_phd_23'])/df['total_25_23']

# Housing units built
# --------------------------------------------------------------------------

df['per_units_pre50_23'] = (df['units_40_49_built_23']+df['units_39_early_built_23'])/df['tot_units_built_23']

## Percent of people who have moved who are low-income
# This function interpolates in mover population counts using income buckets provided by the Census

def income_interpolation_movein (census, year, cutoff, rm_iinc):
    # SUM EVERY CATEGORY BY INCOME
    ## Filter only move-in variables
    name = []
    for c in list(census.columns):
        if (c[0:3] == 'mov') & (c[-2:]==year):
            name.append(c)
    name.append('FIPS')
    income_cat = census[name].copy()
    ## Pull income categories
    income_group = income_cat.drop(columns = ['FIPS']).columns
    number = []
    for c in name[:9]:
        number.append(c.split('_')[2])
    ## Sum move-in in last 5 years by income category, including total w/ income
    column_name_totals = []
    for i in number:
        column_name = []
        for j in income_group:
            if j.split('_')[2] == i:
                column_name.append(j)
        if i == 'w':
            i = 'w_income'
        income_cat['mov_tot_'+i+'_'+year] = income_cat[column_name].sum(axis = 1)
        column_name_totals.append('mov_tot_'+i+'_'+year)
    # DO INCOME INTERPOLATION
    column = []
    number = [n for n in number if n != 'w'] ## drop total
    for i in number:
        column.append('prop_mov_'+i)
        income_cat['prop_mov_'+i] = income_cat['mov_tot_'+i+'_'+year]/income_cat['mov_tot_w_income_'+year]
    reg_median_cutoff = cutoff*rm_iinc
    cumulative = 'inc'+str(int(cutoff*100))+'_cumulative'
    per_limove = 'per_limove_'+year
    df = income_cat
    df[cumulative] = 0
    df[per_limove] = 0
    for i in range(0,(len(number)-1)):
        a = (number[i])
        b = float(number[i+1])-0.01
        prop = str(number[i+1])
        df[cumulative] = df[cumulative]+df['prop_mov_'+a]
        if (reg_median_cutoff>=int(a))&(reg_median_cutoff<b):
            df[per_limove] = ((reg_median_cutoff - int(a))/(b-int(a)))*df['prop_mov_'+prop] + df[cumulative]
    df = df.drop(columns = [cumulative])
    prop_col = df.columns[df.columns.str[0:4]=='prop']
    df = df.drop(columns = prop_col)
    col_list = [per_limove]+['mov_tot_w_income_'+year]
    census = census.merge (df[['FIPS'] + col_list], on = 'FIPS')
    return census

census = income_interpolation_movein (census, '23', 0.8, rm_iinc_23)
census = income_interpolation_movein (census, '12', 0.8, rm_iinc_12)

len(census)

# ==========================================================================
# Housing Affordability Variables
# ==========================================================================

def filter_PUMS(df, FIPS):
    FIPS = [int(x) for x in FIPS]
    df = df[(df['STATEA'] == int(state))&(df['COUNTYA'].isin(FIPS))].reset_index(drop = True)
    return df

def filter_FIPS_PUMS(df):
    df = df[df['COUNTYA'].isin(FIPS)].reset_index(drop = True)
    return df

pums = nhgis_to_fips(pums)
pums = pums[pums['FIPS'].str.startswith('06067')].copy()
pums = filter_FIPS_PUMS(pums)

pums = pums.rename(columns = {"ASVAE002":"rhu_23_wcash",
                                "ASVAE003":"R_100_23",
                                "ASVAE004":"R_150_23",
                                "ASVAE005":"R_200_23",
                                "ASVAE006":"R_250_23",
                                "ASVAE007":"R_300_23",
                                "ASVAE008":"R_350_23",
                                "ASVAE009":"R_400_23",
                                "ASVAE010":"R_450_23",
                                "ASVAE011":"R_500_23",
                                "ASVAE012":"R_550_23",
                                "ASVAE013":"R_600_23",
                                "ASVAE014":"R_650_23",
                                "ASVAE015":"R_700_23",
                                "ASVAE016":"R_750_23",
                                "ASVAE017":"R_800_23",
                                "ASVAE018":"R_900_23",
                                "ASVAE019":"R_1000_23",
                                "ASVAE020":"R_1250_23",
                                "ASVAE021":"R_1500_23",
                                "ASVAE022":"R_2000_23",
                                "ASVAE023":"R_2500_23",
                                "ASVAE024":"R_3000_23",
                                "ASVAE025":"R_3500_23",
                                "ASVAE026":"R_3600_23",
                                "ASVAE027":"rhu_23_wocash",
                                "ATD7E001":"ohu_tot_23",
                                "ATD7E002":"O_200_23",
                                "ATD7E003":"O_300_23",
                                "ATD7E004":"O_400_23",
                                "ATD7E005":"O_500_23",
                                "ATD7E006":"O_600_23",
                                "ATD7E007":"O_700_23",
                                "ATD7E008":"O_800_23",
                                "ATD7E009":"O_900_23",
                                "ATD7E010":"O_1000_23",
                                "ATD7E011":"O_1250_23",
                                "ATD7E012":"O_1500_23",
                                "ATD7E013":"O_2000_23",
                                "ATD7E014":"O_2500_23",
                                "ATD7E015":"O_3000_23",
                                "ATD7E016":"O_3500_23",
                                "ATD7E017":"O_4000_23",
                                "ATD7E018":"O_4100_23"})

aff_23 = rm_hinc_23*0.3/12
pums = income_interpolation (pums, '23', 0.6, aff_23, 'rhu_23_wcash', 'R', 'rent')
pums = income_interpolation (pums, '23', 1.2, aff_23, 'rhu_23_wcash', 'R', 'rent')

pums = income_interpolation (pums, '23', 0.6, aff_23, 'ohu_tot_23', 'O', 'own')
pums = income_interpolation (pums, '23', 1.2, aff_23, 'ohu_tot_23', 'O', 'own')

#pums['FIPS'] = pums['FIPS'].astype(str)
pums = pums.merge(census[['FIPS', 'mmhcosts_23']], on = 'FIPS')

pums['rlow_23'] = pums['rent60_23']*pums['rhu_23_wcash']+pums['rhu_23_wocash'] ## includes no cash rent
pums['rmod_23'] = pums['rent120_23']*pums['rhu_23_wcash']-pums['rent60_23']*pums['rhu_23_wcash']
pums['rhigh_23'] = pums['rhu_23_wcash']-pums['rent120_23']*pums['rhu_23_wcash']

pums['olow_23'] = pums['own60_23']*pums['ohu_tot_23']
pums['omod_23'] = pums['own120_23']*pums['ohu_tot_23'] - pums['own60_23']*pums['ohu_tot_23']
pums['ohigh_23'] = pums['ohu_tot_23'] - pums['own120_23']*pums['ohu_tot_23']

pums['hu_tot_23'] = pums['rhu_23_wcash']+pums['rhu_23_wocash']+pums['ohu_tot_23']

pums['low_tot_23'] = pums['rlow_23']+pums['olow_23']
pums['mod_tot_23'] = pums['rmod_23']+pums['omod_23']
pums['high_tot_23'] = pums['rhigh_23']+pums['ohigh_23']

pums['pct_low_23'] = pums['low_tot_23']/pums['hu_tot_23']
pums['pct_mod_23'] = pums['mod_tot_23']/pums['hu_tot_23']
pums['pct_high_23'] = pums['high_tot_23']/pums['hu_tot_23']

# Classifying tracts by housing afforablde by income
# --------------------------------------------------------------------------

## Low income
pums['predominantly_LI'] = np.where((pums['pct_low_23']>=0.55)&
                                       (pums['pct_mod_23']<0.45)&
                                       (pums['pct_high_23']<0.45),1,0)

## High income
pums['predominantly_HI'] = np.where((pums['pct_low_23']<0.45)&
                                       (pums['pct_mod_23']<0.45)&
                                       (pums['pct_high_23']>=0.55),1,0)

## Moderate income
pums['predominantly_MI'] = np.where((pums['pct_low_23']<0.45)&
                                       (pums['pct_mod_23']>=0.55)&
                                       (pums['pct_high_23']<0.45),1,0)

## Mixed-Low income
pums['mixed_low'] = np.where((pums['predominantly_LI']==0)&
                              (pums['predominantly_MI']==0)&
                              (pums['predominantly_HI']==0)&
                              (pums['mmhcosts_23']<aff_23*0.6),1,0)

## Mixed-Moderate income
pums['mixed_mod'] = np.where((pums['predominantly_LI']==0)&
                              (pums['predominantly_MI']==0)&
                              (pums['predominantly_HI']==0)&
                              (pums['mmhcosts_23']>=aff_23*0.6)&
                              (pums['mmhcosts_23']<aff_23*1.2),1,0)

## Mixed-High income
pums['mixed_high'] = np.where((pums['predominantly_LI']==0)&
                              (pums['predominantly_MI']==0)&
                              (pums['predominantly_HI']==0)&
                              (pums['mmhcosts_23']>=aff_23*1.2),1,0)

pums['lmh_flag_encoded'] = ""
pums.loc[pums['predominantly_LI']==1, 'lmh_flag_encoded'] = 1
pums.loc[pums['predominantly_MI']==1, 'lmh_flag_encoded'] = 2
pums.loc[pums['predominantly_HI']==1, 'lmh_flag_encoded'] = 3
pums.loc[pums['mixed_low']==1, 'lmh_flag_encoded'] = 4
pums.loc[pums['mixed_mod']==1, 'lmh_flag_encoded'] = 5
pums.loc[pums['mixed_high']==1, 'lmh_flag_encoded'] = 6

pums['lmh_flag_category'] = ""
pums.loc[pums['lmh_flag_encoded']==1, 'lmh_flag_category'] = 'aff_predominantly_LI'
pums.loc[pums['lmh_flag_encoded']==2, 'lmh_flag_category'] = 'aff_predominantly_MI'
pums.loc[pums['lmh_flag_encoded']==3, 'lmh_flag_category'] = 'aff_predominantly_HI'
pums.loc[pums['lmh_flag_encoded']==4, 'lmh_flag_category'] = 'aff_mix_low'
pums.loc[pums['lmh_flag_encoded']==5, 'lmh_flag_category'] = 'aff_mix_mod'
pums.loc[pums['lmh_flag_encoded']==6, 'lmh_flag_category'] = 'aff_mix_high'

pums.groupby('lmh_flag_category').count()['FIPS']

census = census.merge(pums[['FIPS', 'lmh_flag_encoded', 'lmh_flag_category']], on = 'FIPS')

len(census)

# ==========================================================================
# Setting 'Market Types'
# ==========================================================================

census['pctch_real_mhval_00_23'] = (census['real_mhval_23']-census['real_mhval_00'])/census['real_mhval_00']
census['pctch_real_mrent_12_23'] = (census['real_mrent_23']-census['real_mrent_12'])/census['real_mrent_12']
rm_pctch_real_mhval_00_23_increase=np.nanmedian(census['pctch_real_mhval_00_23'][census['pctch_real_mhval_00_23']>0.05])
rm_pctch_real_mrent_12_23_increase=np.nanmedian(census['pctch_real_mrent_12_23'][census['pctch_real_mrent_12_23']>0.05])
census['rent_decrease'] = np.where((census['pctch_real_mrent_12_23']<=-0.05), 1, 0)
census['rent_marginal'] = np.where((census['pctch_real_mrent_12_23']>-0.05)&
                                          (census['pctch_real_mrent_12_23']<0.05), 1, 0)
census['rent_increase'] = np.where((census['pctch_real_mrent_12_23']>=0.05)&
                                          (census['pctch_real_mrent_12_23']<rm_pctch_real_mrent_12_23_increase), 1, 0)
census['rent_rapid_increase'] = np.where((census['pctch_real_mrent_12_23']>=0.05)&
                                          (census['pctch_real_mrent_12_23']>=rm_pctch_real_mrent_12_23_increase), 1, 0)

census['house_decrease'] = np.where((census['pctch_real_mhval_00_23']<=-0.05), 1, 0)
census['house_marginal'] = np.where((census['pctch_real_mhval_00_23']>-0.05)&
                                          (census['pctch_real_mhval_00_23']<0.05), 1, 0)
census['house_increase'] = np.where((census['pctch_real_mhval_00_23']>=0.05)&
                                          (census['pctch_real_mhval_00_23']<rm_pctch_real_mhval_00_23_increase), 1, 0)
census['house_rapid_increase'] = np.where((census['pctch_real_mhval_00_23']>=0.05)&
                                          (census['pctch_real_mhval_00_23']>=rm_pctch_real_mhval_00_23_increase), 1, 0)

census['tot_decrease'] = np.where((census['rent_decrease']==1)|(census['house_decrease']==1), 1, 0)
census['tot_marginal'] = np.where((census['rent_marginal']==1)|(census['house_marginal']==1), 1, 0)
census['tot_increase'] = np.where((census['rent_increase']==1)|(census['house_increase']==1), 1, 0)
census['tot_rapid_increase'] = np.where((census['rent_rapid_increase']==1)|(census['house_rapid_increase']==1), 1, 0)

census['change_flag_encoded'] = ""
census.loc[(census['tot_decrease']==1)|(census['tot_marginal']==1), 'change_flag_encoded'] = 1
census.loc[census['tot_increase']==1, 'change_flag_encoded'] = 2
census.loc[census['tot_rapid_increase']==1, 'change_flag_encoded'] = 3

census['change_flag_category'] = ""
census.loc[census['change_flag_encoded']==1, 'change_flag_category'] = 'ch_decrease_marginal'
census.loc[census['change_flag_encoded']==2, 'change_flag_category'] = 'ch_increase'
census.loc[census['change_flag_encoded']==3, 'change_flag_category'] = 'ch_rapid_increase'

census.groupby('change_flag_category').count()['FIPS']

census.groupby(['change_flag_category', 'lmh_flag_category']).count()['FIPS']

len(census)

# ==========================================================================
# Zillow Data
# ==========================================================================

# Load Zillow Data
# --------------------------------------------------------------------------

def filter_ZILLOW(df, county):
    df['CountyName'] = df['CountyName'].astype(str).str.strip()
    df = df[df['CountyName'] == county].reset_index(drop = True)
    return df
def filter_ZILLOW_CROSSWALK(df):
    df.start

## Import Zillow data
zillow = pd.read_csv(input_path+'Zip_Zhvi_AllHomes.csv', encoding = "ISO-8859-1",dtype={'RegionName':str})
zillow_xwalk = pd.read_csv(input_path+'TRACT_ZIP_032015.csv',dtype={'ZIP':str,'TRACT':str})

# Calculate Zillow Measures
# --------------------------------------------------------------------------
# Strip spaces from RegionName column
zillow['RegionName'] = zillow['RegionName'].astype(str).str.strip()

# Strip spaces from ZIP column
zillow_xwalk['ZIP'] = zillow_xwalk['ZIP'].astype(str).str.strip()

zillow_xwalk= zillow_xwalk[zillow_xwalk['ZIP'].str.startswith(('95','94'))].copy()

#zillow=filter_ZILLOW(zillow,'Sacramento County')
## Compute change over time
zillow['ch_zillow_12_23'] = zillow['2023-01-31'] - zillow['2012-01-31']*CPI_12_23
zillow['per_ch_zillow_12_23'] = zillow['ch_zillow_12_23']/zillow['2012-01-31']
zillow = zillow[zillow['State'].isin(state_init)].reset_index(drop = True)

og_zillow=zillow.copy()
# Perform the merge
zillow = zillow_xwalk[['TRACT', 'ZIP', 'RES_RATIO']].merge(
    zillow[['RegionName','CountyName', 'ch_zillow_12_23', 'per_ch_zillow_12_23']], 
    left_on = 'ZIP', 
    right_on = 'RegionName', 
    how = "outer"
)

print(f"Merge Check: Found {zillow['RegionName'].notna().sum()} matches out of {len(zillow)} rows")

# Now find unmatched Zillow records
removed_rows = og_zillow[~og_zillow['RegionName'].isin(zillow['RegionName'].dropna())]

print(f"\nUnmatched Zillow regions ({len(removed_rows)} total):")

zillow = zillow.rename(columns = {'TRACT':'FIPS'})

# Filter only data of interest
zillow = filter_ZILLOW(zillow, 'Sacramento County')

## Keep only data for largest xwalk value, based on residential ratio
zillow = zillow.sort_values(by = ['FIPS', 'RES_RATIO'], ascending = False).groupby('FIPS').first().reset_index(drop = False)

## Compute 90th percentile change in region
percentile_90 = zillow['per_ch_zillow_12_23'].quantile(q = 0.9)
print("90th Percentile Change in Zillow Home Values (2012-2023):")
print(percentile_90)

# Create Flags
# --------------------------------------------------------------------------

## Change over 50% of change in region
zillow['ab_50pct_ch'] = np.where(zillow['per_ch_zillow_12_23']>0.5, 1, 0)

## Change over 90th percentile change
zillow['ab_90percentile_ch'] = np.where(zillow['per_ch_zillow_12_23']>percentile_90, 1, 0)

census['FIPS']=census['FIPS'].astype(str)
zillow['FIPS']=zillow['FIPS'].astype(str)

census_zillow = census.merge(zillow[['FIPS', 'per_ch_zillow_12_23', 'ab_50pct_ch', 'ab_90percentile_ch']], on = 'FIPS')

print(f"Merge Check: Found {census_zillow['per_ch_zillow_12_23'].notna().sum()} matches out of {len(census_zillow)} rows")

## Create 90th percentile for rent -
# census['rent_percentile_90'] = census['pctch_real_mrent_12_23'].quantile(q = 0.9)
census_zillow['rent_50pct_ch'] = np.where(census_zillow['pctch_real_mrent_12_23']>=0.5, 1, 0)
census_zillow['rent_90percentile_ch'] = np.where(census_zillow['pctch_real_mrent_12_23']>=0.9, 1, 0)

# ==========================================================================
# Calculate Regional Medians
# ==========================================================================

# Calculate medians necessary for typology designation
# --------------------------------------------------------------------------

rm_per_all_li_90 = np.nanmedian(census_zillow['per_all_li_90'])
rm_per_all_li_00 = np.nanmedian(census_zillow['per_all_li_00'])
rm_per_all_li_23 = np.nanmedian(census_zillow['per_all_li_23'])

rm_per_nonwhite_90 = np.nanmedian(census_zillow['per_nonwhite_90'])
rm_per_nonwhite_00 = np.nanmedian(census_zillow['per_nonwhite_00'])
rm_per_nonwhite_23 = np.nanmedian(census_zillow['per_nonwhite_23'])

rm_per_col_90 = np.nanmedian(census_zillow['per_col_90'])
rm_per_col_00 = np.nanmedian(census_zillow['per_col_00'])
rm_per_col_23 = np.nanmedian(census_zillow['per_col_23'])

rm_per_rent_90= np.nanmedian(census_zillow['per_rent_90'])
rm_per_rent_00= np.nanmedian(census_zillow['per_rent_00'])
rm_per_rent_23= np.nanmedian(census_zillow['per_rent_23'])

rm_real_mrent_90 = np.nanmedian(census_zillow['real_mrent_90'])
rm_real_mrent_00 = np.nanmedian(census_zillow['real_mrent_00'])
rm_real_mrent_12 = np.nanmedian(census_zillow['real_mrent_12'])
rm_real_mrent_23 = np.nanmedian(census_zillow['real_mrent_23'])

rm_real_mhval_90 = np.nanmedian(census_zillow['real_mhval_90'])
rm_real_mhval_00 = np.nanmedian(census_zillow['real_mhval_00'])
rm_real_mhval_23 = np.nanmedian(census_zillow['real_mhval_23'])

rm_real_hinc_90 = np.nanmedian(census_zillow['real_hinc_90'])
rm_real_hinc_00 = np.nanmedian(census_zillow['real_hinc_00'])
rm_real_hinc_23 = np.nanmedian(census_zillow['real_hinc_23'])
rm_per_units_pre50_23 = np.nanmedian(census_zillow['per_units_pre50_23'])
rm_per_ch_zillow_12_23 = np.nanmedian(census_zillow['per_ch_zillow_12_23'])
rm_pctch_real_mrent_12_23 = np.nanmedian(census_zillow['pctch_real_mrent_12_23'])

## Above regional median change home value and rent
census_zillow['hv_abrm_ch'] = np.where(census_zillow['per_ch_zillow_12_23'] > rm_per_ch_zillow_12_23, 1, 0)
census_zillow['rent_abrm_ch'] = np.where(census_zillow['pctch_real_mrent_12_23'] > rm_pctch_real_mrent_12_23, 1, 0)

## Percent changes
census_zillow['pctch_real_mhval_90_00'] = (census_zillow['real_mhval_00']-census_zillow['real_mhval_90'])/census_zillow['real_mhval_90']
census_zillow['pctch_real_mrent_90_00'] = (census_zillow['real_mrent_00']-census_zillow['real_mrent_90'])/census_zillow['real_mrent_90']
census_zillow['pctch_real_hinc_90_00'] = (census_zillow['real_hinc_00']-census_zillow['real_hinc_90'])/census_zillow['real_hinc_90']

census_zillow['pctch_real_mhval_00_23'] = (census_zillow['real_mhval_23']-census_zillow['real_mhval_00'])/census_zillow['real_mhval_00']
census_zillow['pctch_real_mrent_00_23'] = (census_zillow['real_mrent_23']-census_zillow['real_mrent_00'])/census_zillow['real_mrent_00']
census_zillow['pctch_real_mrent_12_23'] = (census_zillow['real_mrent_23']-census_zillow['real_mrent_12'])/census_zillow['real_mrent_12']
census_zillow['pctch_real_hinc_00_23'] = (census_zillow['real_hinc_23']-census_zillow['real_hinc_00'])/census_zillow['real_hinc_00']

## Regional Medians

pctch_rm_real_mhval_90_00 = (rm_real_mhval_00-rm_real_mhval_90)/rm_real_mhval_90
pctch_rm_real_mrent_90_00 = (rm_real_mrent_00-rm_real_mrent_90)/rm_real_mrent_90
pctch_rm_real_mhval_00_23 = (rm_real_mhval_23-rm_real_mhval_00)/rm_real_mhval_00
pctch_rm_real_mrent_00_23 = (rm_real_mrent_23-rm_real_mrent_00)/rm_real_mrent_00
pctch_rm_real_mrent_12_23 = (rm_real_mrent_23-rm_real_mrent_12)/rm_real_mrent_12
pctch_rm_real_hinc_90_00 = (rm_real_hinc_00-rm_real_hinc_90)/rm_real_hinc_90
pctch_rm_real_hinc_00_23 = (rm_real_hinc_23-rm_real_hinc_00)/rm_real_hinc_00


## Absolute changes

census_zillow['ch_all_li_count_90_00'] = census_zillow['all_li_count_00']-census_zillow['all_li_count_90']
census_zillow['ch_all_li_count_00_23'] = census_zillow['all_li_count_23']-census_zillow['all_li_count_00']
census_zillow['ch_per_col_90_00'] = census_zillow['per_col_00']-census_zillow['per_col_90']
census_zillow['ch_per_col_00_23'] = census_zillow['per_col_23']-census_zillow['per_col_00']
census_zillow['ch_per_limove_12_23'] = census_zillow['per_limove_23'] - census_zillow['per_limove_12']

## Regional Medians

ch_rm_per_col_90_00 = rm_per_col_00-rm_per_col_90
ch_rm_per_col_00_23 = rm_per_col_23-rm_per_col_00

# Calculate flags
# --------------------------------------------------------------------------

df = census_zillow
df['pop00flag'] = np.where(df['pop_00']>500, 1, 0)
df['aboverm_per_all_li_90'] = np.where(df['per_all_li_90']>=rm_per_all_li_90, 1, 0)
df['aboverm_per_all_li_00'] = np.where(df['per_all_li_00']>=rm_per_all_li_00, 1, 0)
df['aboverm_per_all_li_23'] = np.where(df['per_all_li_23']>=rm_per_all_li_23, 1, 0)
df['aboverm_per_nonwhite_23'] = np.where(df['per_nonwhite_23']>=rm_per_nonwhite_23, 1, 0)
df['aboverm_per_nonwhite_90'] = np.where(df['per_nonwhite_90']>=rm_per_nonwhite_90, 1, 0)
df['aboverm_per_nonwhite_00'] = np.where(df['per_nonwhite_00']>=rm_per_nonwhite_00, 1, 0)
df['aboverm_per_rent_90'] = np.where(df['per_rent_90']>=rm_per_rent_90, 1, 0)
df['aboverm_per_rent_00'] = np.where(df['per_rent_00']>=rm_per_rent_00, 1, 0)
df['aboverm_per_rent_23'] = np.where(df['per_rent_23']>=rm_per_rent_23, 1, 0)
df['aboverm_per_col_90'] = np.where(df['per_col_90']>=rm_per_col_90, 1, 0)
df['aboverm_per_col_00'] = np.where(df['per_col_00']>=rm_per_col_00, 1, 0)
df['aboverm_per_col_23'] = np.where(df['per_col_23']>=rm_per_col_23, 1, 0)
df['aboverm_real_mrent_90'] = np.where(df['real_mrent_90']>=rm_real_mrent_90, 1, 0)
df['aboverm_real_mrent_00'] = np.where(df['real_mrent_00']>=rm_real_mrent_00, 1, 0)
df['aboverm_real_mrent_12'] = np.where(df['real_mrent_12']>=rm_real_mrent_12, 1, 0)
df['aboverm_real_mrent_23'] = np.where(df['real_mrent_23']>=rm_real_mrent_23, 1, 0)
df['aboverm_real_mhval_90'] = np.where(df['real_mhval_90']>=rm_real_mhval_90, 1, 0)
df['aboverm_real_mhval_00'] = np.where(df['real_mhval_00']>=rm_real_mhval_00, 1, 0)
df['aboverm_real_mhval_23'] = np.where(df['real_mhval_23']>=rm_real_mhval_23, 1, 0)
df['aboverm_pctch_real_mhval_00_23'] = np.where(df['pctch_real_mhval_00_23']>=pctch_rm_real_mhval_00_23, 1, 0)
df['aboverm_pctch_real_mrent_00_23'] = np.where(df['pctch_real_mrent_00_23']>=pctch_rm_real_mrent_00_23, 1, 0)
df['aboverm_pctch_real_mrent_12_23'] = np.where(df['pctch_real_mrent_12_23']>=pctch_rm_real_mrent_12_23, 1, 0)
df['aboverm_pctch_real_mhval_90_00'] = np.where(df['pctch_real_mhval_90_00']>=pctch_rm_real_mhval_90_00, 1, 0)
df['aboverm_pctch_real_mrent_90_00'] = np.where(df['pctch_real_mrent_90_00']>=pctch_rm_real_mrent_90_00, 1, 0)
df['lostli_00'] = np.where(df['ch_all_li_count_90_00']<0, 1, 0)
df['lostli_23'] = np.where(df['ch_all_li_count_00_23']<0, 1, 0)
df['aboverm_pctch_real_hinc_90_00'] = np.where(df['pctch_real_hinc_90_00']>pctch_rm_real_hinc_90_00, 1, 0)
df['aboverm_pctch_real_hinc_00_23'] = np.where(df['pctch_real_hinc_00_23']>pctch_rm_real_hinc_00_23, 1, 0)
df['aboverm_ch_per_col_90_00'] = np.where(df['ch_per_col_90_00']>ch_rm_per_col_90_00, 1, 0)
df['aboverm_ch_per_col_00_23'] = np.where(df['ch_per_col_00_23']>ch_rm_per_col_00_23, 1, 0)
df['aboverm_per_units_pre50_23'] = np.where(df['per_units_pre50_23']>rm_per_units_pre50_23, 1, 0)
df.to_csv('flags.csv')
# Shapefiles
# --------------------------------------------------------------------------

## Filter only census_zillow tracts of interest from shp
census_zillow_tract_list = census_zillow['FIPS'].astype(str).str.zfill(11)
city_shp = city_shp[city_shp['GEOID'].isin(census_zillow_tract_list)].reset_index(drop = True)

## Create single region polygon
city_poly = city_shp.dissolve(by = 'STATEFP')
city_poly = city_poly.reset_index(drop = True)

census_zillow_tract_list.describe()

# ==========================================================================
# Overlay Variables (Rail + Housing)
# ==========================================================================

# Rail
# --------------------------------------------------------------------------

## Filter only existing rail
rail = rail[rail['Year Opened']=='Pre-2000'].reset_index(drop = True)

## Filter by city
rail = rail[rail['Agency'].isin(rail_agency)].reset_index(drop = True)
rail = gpd.GeoDataFrame(rail, geometry=[Point(xy) for xy in zip (rail['Longitude'], rail['Latitude'])])

## sets coordinate system to WGS84
rail.crs = 'EPSG:4269'

## project to UTM coordinate system
rail_proj = rail.to_crs(epsg=32610)

## create buffer around anchor institution in meters
rail_buffer = rail_proj.buffer(804.672)

## convert buffer back to WGS84
rail_buffer_wgs = rail_buffer.to_crs(epsg=4326)

## crate flag
city_shp['rail'] = np.where(city_shp.intersects(rail_buffer_wgs.union_all()) == True, 1, 0)

# Subsidized Housing
# --------------------------------------------------------------------------
## Convert to geodataframe
lihtc = gpd.GeoDataFrame(lihtc, geometry=[Point(xy) for xy in zip (lihtc['LON'], lihtc['LAT'])])
pub_hous = gpd.GeoDataFrame(pub_hous, geometry=[Point(xy) for xy in zip (pub_hous['X'], pub_hous['Y'])])

lihtc.crs = 'EPSG:4269'
pub_hous.crs ='EPSG:4269'



## Filter by state and county BEFORE spatial filtering
#lihtc = lihtc[lihtc['CURCNTY_NM'] == 'Sacramento'].reset_index(drop = True)

## LIHTC clean
lihtc = lihtc[lihtc['geometry'].within(city_poly.loc[0, 'geometry'])].reset_index(drop = True)
lihtc.to_csv("lihtc sample.csv")

## Public housing
pub_hous = pub_hous[pub_hous['geometry'].within(city_poly.loc[0, 'geometry'])].reset_index(drop = True)
pub_hous.to_csv('pub huose sample.csv')

## Check city_poly CRS
print(f"\ncity_poly CRS: {city_poly.crs}")
print(f"lihtc CRS: {lihtc.crs}")
print(f"city_poly bounds: {city_poly.loc[0, 'geometry'].bounds}")
print(f"lihtc bounds: {lihtc.total_bounds if len(lihtc) > 0 else 'N/A'}")
print(f"pubhous bounds: {pub_hous.total_bounds if len(lihtc) > 0 else 'N/A'}")

## Merge Datasets
presence_ph_LIHTC = pd.concat([lihtc[['geometry']], pub_hous[['geometry']]])

## check whether census_zillow tract contains public housing or LIHTC station
## and create public housing flag
city_shp['presence_ph_LIHTC'] = city_shp.intersects(presence_ph_LIHTC.union_all())

####
# Begin Map Plot
####
# ax = city_shp.plot(color = 'grey')
# city_shp.plot(ax = ax, column = 'presence_ph_LIHTC')
# presence_ph_LIHTC.plot(ax = ax)
# plt.show()
####
# End Map Plot
####

# ==========================================================================
# Merge Census and Zillow Data
# ==========================================================================

#city_shp['GEOID'] = city_shp['GEOID'].astype('int64')

census_zillow = census_zillow.merge(city_shp[['GEOID','geometry','rail',
	# 'anchor_institution',
	'presence_ph_LIHTC']], right_on = 'GEOID', left_on = 'FIPS')
census_zillow.query("FIPS == 13121011100")
# ==========================================================================
# Export Data
# ==========================================================================

census_zillow.to_csv(output_path+'databases/'+city_name.replace(" ", "")+'_database_2023.csv')
# pq.write_table(output_path+'downloads/'+city_name.replace(" ", "")+'_database.parquet')
