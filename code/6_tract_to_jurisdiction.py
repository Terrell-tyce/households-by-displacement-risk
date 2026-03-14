from pathlib import Path
import geopandas as gpd
import pandas as pd
import sys
import pandas as pd


# ==========================================================================
# Functions
# ==========================================================================

# ========================================================================
# Format the Summary data cleanly
# ========================================================================
def format_summary(df, group_col, o_col='ohu_24', r_col='rhu_24'):
    # Aggregate
    summary = df.groupby(group_col)[[o_col, r_col]].sum()
    
    # Add totals row
    totals = pd.DataFrame(summary.sum()).T
    totals.index = ['Totals']
    summary = pd.concat([summary, totals])
    
    # Reset index
    summary = summary.reset_index()
    
    # Rename columns
    summary = summary.rename(columns={
        group_col: 'Displacement Group',
        o_col: 'Owner Occupied',
        r_col: 'Renter Occupied'
    })
    
    # Add commas for readability
    summary['Owner Occupied'] = summary['Owner Occupied'].round()
    summary['Renter Occupied'] = summary['Renter Occupied'].round()
    
    return summary

# ========================================================================
# Region-level summary
# ========================================================================
def summarize_typology_region(df):
    return format_summary(df, 'typ_simplified_name')

# ========================================================================
# County-level summary
# ========================================================================
def summarize_typology_county(df):
    summaries = {}
    for county, group in df.groupby('county'):
        summaries[county] = format_summary(group, 'typ_simplified_name')
    return summaries
# ========================================================================
# Jurisdiction-level summary
# ========================================================================
def summarize_typology_jurisdiction(df):
    summaries = {}
    for juris, group in df.groupby('JURIS'):
        summaries[juris] = format_summary(group, 'typ_simplified_name')
    return summaries

# ==========================================================================
# Variables
# ==========================================================================
DATA_Dir="I:\Projects\Josh\RHNA\Data\POPEMP_25\emp25_data"
input_path = DATA_Dir+'/inputs/'
output_path = DATA_Dir+'/outputs/'

GDB = Path(r'I:\Projects\Josh\RHNA\ArcPro\RHNA.gdb')
FC_JURISDICTIONS = 'GISOWNER_City_County'
FC_TRACTS = 'GISOWNER_T2020_Census_Tracts_SACOG_Region'
FC_BLOCKS = 'GISOWNER_Census_Blocks_SACOG_Region' # Left join this guy onto the census tracts file, 
#only needing three fields from the Blocks layer [BLOCK ID, CENSUS TRACT GEOID, Population]

# Enter name of city
city_name = 'SACOG'
# ==========================================================================
#Reading in data
# ==========================================================================
print('Importing GIS layers:')
print('Jurisdictions...'); gdf_jurisdictions = gpd.read_file(GDB, layer=FC_JURISDICTIONS)
print('Census Tracts...'); gdf_tracts        = gpd.read_file(GDB, layer=FC_TRACTS      )
print('Blocks...'       ); gdf_blocks        = gpd.read_file(GDB, layer=FC_BLOCKS       ) # may take a few minutes to import

print("Importing typology data")
df_typol = pd.read_csv(output_path+'/typologies/'+city_name+'_typology_output.csv',dtype={'FIPS':str,'rhu_24':int,'ohu_24':int})
df_typol=df_typol.rename(columns={'FIPS':'GEOCODE'})

gdf_overlay = gpd.overlay(gdf_jurisdictions, gdf_tracts, how='intersection', keep_geom_type=False)
# Drop old merge suffix columns to prevent collision
df_typol = df_typol.loc[:, ~df_typol.columns.str.endswith(('_x', '_y'))]

typ_overlay_merge = pd.merge(df_typol,
                             gdf_overlay,
                             on='GEOCODE'
                             )

clean_typ_overlay = typ_overlay_merge[['GEOCODE','typ_cat','ohu_24','rhu_24','hh_24','JURIS','county','typ_simplified_name','typ_name']]

# =============================================================================
# STEP 1 — PREP BLOCK POPULATION
# =============================================================================
print("Preparing block population...")

gdf_blocks = gdf_blocks.rename(columns={
    'GEOID': 'BLOCK_GEOID',
    'POP100': 'POP',
    'HU100': 'HU'
})
print(gdf_blocks.columns)
gdf_blocks = gdf_blocks[['BLOCK_GEOID', 'POP', 'HU', 'geometry']].copy()
gdf_blocks['POP'] = gdf_blocks['POP'].fillna(0)
gdf_blocks['HU'] = gdf_blocks['HU'].fillna(0)

if gdf_blocks.crs.is_geographic:
    gdf_blocks = gdf_blocks.to_crs(gdf_tracts.crs)


# =============================================================================
# STEP 2 — BLOCK → TRACT JOIN
# =============================================================================
print("Assigning blocks to tracts...")

gdf_blocks_tract = gpd.sjoin(
    gdf_blocks,
    gdf_tracts[['GEOCODE','geometry']],
    how='left',
    predicate='within'
)

# =============================================================================
# STEP 3 — BLOCK → JURISDICTION OVERLAY
# =============================================================================
print("Overlaying blocks with jurisdictions...")

gdf_block_juris = gpd.overlay(
    gdf_blocks_tract,
    gdf_jurisdictions[['JURIS','geometry']],
    how='intersection'
)

gdf_block_juris['piece_area'] = gdf_block_juris.geometry.area

block_area = gdf_blocks[['BLOCK_GEOID','geometry']].copy()
block_area['block_area'] = block_area.geometry.area

gdf_block_juris = gdf_block_juris.merge(block_area[['BLOCK_GEOID','block_area']], on='BLOCK_GEOID')

gdf_block_juris['pop_piece'] = (
    gdf_block_juris['POP'] *
    (gdf_block_juris['piece_area'] / gdf_block_juris['block_area'])
)

# =============================================================================
# STEP 4 — TRACT × JURIS HU SCALAR
# =============================================================================
print("Computing tract-jurisdiction scalars...")

gdf_block_juris['hu_piece'] = (
    gdf_block_juris['HU'] *
    (gdf_block_juris['piece_area'] / gdf_block_juris['block_area'])
)

tract_juris_hu = (
    gdf_block_juris.groupby(['GEOCODE','JURIS'])['hu_piece']
    .sum().reset_index()
)
tract_tot_hu = (
    gdf_block_juris.groupby('GEOCODE')['hu_piece']
    .sum().reset_index()
    .rename(columns={'hu_piece':'tract_hu'})
)
tract_juris_hu = tract_juris_hu.merge(tract_tot_hu, on='GEOCODE')
tract_juris_hu['hu_scalar'] = tract_juris_hu['hu_piece'] / tract_juris_hu['tract_hu']

# =============================================================================
# STEP 5 — MERGE SCALAR INTO TYPOLOGY
# =============================================================================
print("Joining scalars to typology...")

clean_typ_overlay = clean_typ_overlay.merge(
    tract_juris_hu[['GEOCODE','JURIS','hu_scalar']],
    on=['GEOCODE','JURIS'],
    how='left'
)

clean_typ_overlay['hu_scalar'] = clean_typ_overlay['hu_scalar'].fillna(0)

# =============================================================================
# STEP 6 — SCALE HOUSING COUNTS
# =============================================================================
print("Scaling housing counts...")

for col in ['ohu_24','rhu_24','hh_24']:
    clean_typ_overlay[col] = clean_typ_overlay[col] * clean_typ_overlay['hu_scalar']

# =============================================================================
# FINAL SUMMARIES
# =============================================================================
print("Creating final weighted summaries...")

regional_summary = summarize_typology_region(clean_typ_overlay)

jur_summaries = summarize_typology_jurisdiction(clean_typ_overlay)
df_jur_summaries = pd.concat(jur_summaries.values(), keys=jur_summaries.keys())
df_jur_summaries = df_jur_summaries.reset_index()
df_jur_summaries = df_jur_summaries.rename(columns={
    'level_0': 'Jurisdiction',
    'index': 'Typology'
}).drop(columns=['level_1'])

county_summaries = summarize_typology_county(clean_typ_overlay)
df_county_summaries = pd.concat(county_summaries.values(), keys=county_summaries.keys())
df_county_summaries = df_county_summaries.reset_index()
df_county_summaries = df_county_summaries.rename(columns={
    'level_0': 'County',
    'index': 'Typology'
}).drop(columns=['level_1'])

regional_summary.to_csv(output_path+"/typologies/region_typology_summary.csv", index=False)
df_jur_summaries.to_csv(output_path+"/typologies/jurisdiction_typology_summary.csv", index=False)
df_county_summaries.to_csv(output_path+"/typologies/county_typology_summary.csv", index=False)

# Diagnostic
# elk = clean_typ_overlay[clean_typ_overlay['JURIS'] == 'Elk Grove']
# print(f"\nElk Grove tracts: {elk['GEOCODE'].nunique()}")
# print(f"Elk Grove ohu_24: {elk['ohu_24'].sum():,.0f}")
# print(f"Elk Grove rhu_24: {elk['rhu_24'].sum():,.0f}")

# missing = ['06067009333','06067009334','06067009335','06067009336',
#            '06067009642','06067009643','06067009644','06067009645',
#            '06067009646','06067009647','06067009648','06067009649',
#            '06067009650','06067009651','06067009652','06067009653']

# print("In gdf_overlay (spatial):    ", gdf_overlay['GEOCODE'].isin(missing).sum())
# print("In df_typol (typology CSV):  ", df_typol['GEOCODE'].isin(missing).sum())
# print("In typ_overlay_merge:        ", typ_overlay_merge['GEOCODE'].isin(missing).sum())
# print("In clean_typ_overlay:        ", clean_typ_overlay['GEOCODE'].isin(missing).sum())

# curation = pd.read_csv(output_path + '/downloads/SACOGcensus_summ_2024.csv', dtype={'FIPS': str})
# print("Missing tracts in curation output:", curation['FIPS'].isin(missing).sum())

# # Also check the database file that feeds into typology
# database = pd.read_csv(output_path + '/databases/SACOG_database_2024.csv', dtype={'FIPS': str})
# print("Missing tracts in database:       ", database['FIPS'].isin(missing).sum())