# ==========================================================================
# Map data for displacement and vulnerability measures
# Sacramento County 2023 - Adapted from original script
# Original Author: Tim Thomas - timthomas@berkeley.edu
# Adapted for: Sacramento County single-city analysis
# Note: The US Census API has been unreliable in some occasions. We therefore
#   suggest downloading every API run that you do when it is successful. 
#   The "Begin..." sections highlight these API downloads followed by a load
#   option. Uncomment and edit these API runs as needed and then comment them 
#   again when testing your maps. 
# ==========================================================================

# Clear the session
rm(list = ls())
options(scipen = 10) # avoid scientific notation

# ==========================================================================
# Load Libraries
# ==========================================================================

#
# Load packages and install them if they're not installed.
# --------------------------------------------------------------------------

# load packages
if (!require("pacman")) install.packages("pacman")
if (!require("tidyverse")) install.packages("tidyverse")
pacman::p_load(readxl, R.utils, bit64, rmapshaper, sf, geojsonsf, scales, data.table, tigris, tidycensus, leaflet, tidyverse)

update.packages(ask = FALSE)
# Cache downloaded tiger files
options(tigris_use_cache = TRUE)
census_api_key('4c26aa6ebbaef54a55d3903212eabbb506ade381') #enter your own key here

# ==========================================================================
# Data
# ==========================================================================

#
# Pull in Sacramento typology data
# --------------------------------------------------------------------------

data <- 
  read_csv('../data/outputs/typologies/Sacramento_typology_output.csv') %>% 
  mutate(city = 'Sacramento') %>%
  left_join(., 
            read_csv('../data/overlays/oppzones.csv',skip=4) %>% 
              select(
                State = 'Click arrow to filter state\n\nState',
                GEOID = 'Census Tract Number', 
                opp_zone = 'Tract Type'
              ) %>%
              mutate(GEOID = as.numeric(GEOID)),
            by = 'GEOID'
  )

#
# Create Neighborhood Racial Typologies for mapping
# --------------------------------------------------------------------------
# State fips code list: https://www.mcc.co.mercer.pa.us/dps/state_fips_code_listing.htm

states <- c('06')  # California only

###
# Begin Neighborhood Typology creation (OPTIONAL - can skip for Sacramento)
###
# For Sacramento, you can:
# 1. Skip this and just use displacement typology, OR
# 2. Create neighborhood racial typology if you have Census race data
# 
# If you want to add racial typology, uncomment and run:
# df_nt <- ntdf(state = states) %>% mutate(GEOID = as.numeric(GEOID))
# Then save: fwrite(df_nt, '~/git/displacement-typologies/data/outputs/downloads/df_nt_sacramento.csv.gz')
###
# End
###

# For now, skip neighborhood typology (Sacramento focus is displacement only)
# If you added it above, uncomment this:
# df_nt <- read_csv('~/git/displacement-typologies/data/outputs/downloads/df_nt_sacramento.csv.gz') %>%
#   mutate(nt_conc = factor(nt_conc, levels = c(
#     "Mostly Asian", "Mostly Black", "Mostly Latinx", "Mostly Other", "Mostly White",
#     "Asian-Black", "Asian-Latinx", "Asian-Other", "Asian-White",
#     "Black-Latinx", "Black-Other", "Black-White",
#     "Latinx-Other", "Latinx-White", "Other-White",
#     "3 Group Mixed", "4 Group Mixed", "Diverse", "Unpopulated Tract")))

#
# Demographics: Student population and vacancy (OPTIONAL)
# --------------------------------------------------------------------------

###
# Begin demographic download (OPTIONAL - can skip for Sacramento)
###
# dem_vars <- 
#   c('st_units' = 'B25001_001',
#     'st_vacant' = 'B25002_003', 
#     'st_ownocc' = 'B25003_002', 
#     'st_rentocc' = 'B25003_003',
#     'st_totenroll' = 'B14007_001',
#     'st_colenroll' = 'B14007_017',
#     'st_proenroll' = 'B14007_018')
# tr_dem_acs <- get_acs(
#     geography = "tract",
#     state = states,
#     output = 'wide',
#     variables = dem_vars,
#     cache_table = TRUE,
#     year = 2023
#   )
# fwrite(tr_dem_acs, '~/git/displacement-typologies/data/outputs/downloads/tr_dem_acs_sacramento.csv.gz')
### 
# End
###

# If you created demographic data, uncomment:
# tr_dem_acs <- read_csv('~/git/displacement-typologies/data/outputs/downloads/tr_dem_acs_sacramento.csv.gz')
# tr_dem <- 
#   tr_dem_acs %>% 
#   group_by(GEOID) %>% 
#   mutate(
#     tr_pstudents = sum(st_colenrollE, st_proenrollE, na.rm = TRUE)/st_totenrollE, 
#     tr_prenters = st_rentoccE/st_unitsE,
#     tr_pvacant = st_vacantE/st_unitsE,
#     GEOID = as.numeric(GEOID)
#   )

#
# Prep dataframe for mapping
# --------------------------------------------------------------------------

scale_this <- function(x){
  (x - mean(x, na.rm=TRUE)) / sd(x, na.rm=TRUE)
}

df <- 
  data %>% 
  # Uncomment if using neighborhood typology:
  # left_join(df_nt) %>% 
  # Uncomment if using demographic data:
  # left_join(tr_dem) %>% 
  group_by(city) %>% 
  mutate(
    # Create typology for maps (already in data from script 4)
    # If it's not there, create from typ_cat:
    Typology = 
      factor(
        case_when(
          typ_cat == "['AdvG', 'BE']" ~ 'Advanced Gentrification',
          typ_cat == "['LISD']" & gent_90_00 == 1 ~ 'Advanced Gentrification',
          typ_cat == "['LISD']" & gent_90_00_urban == 1 ~ 'Advanced Gentrification',
          typ_cat == "['OD']" & gent_90_00 == 1 ~ 'Advanced Gentrification',
          typ_cat == "['OD']" & gent_90_00_urban == 1 ~ 'Advanced Gentrification',
          typ_cat == "['LISD']" & gent_00_23 == 1 ~ 'Early/Ongoing Gentrification',
          typ_cat == "['LISD']" & gent_00_23_urban == 1 ~ 'Early/Ongoing Gentrification',
          typ_cat == "['OD']" & gent_00_23 == 1 ~ 'Early/Ongoing Gentrification',
          typ_cat == "['OD']" & gent_00_23_urban == 1 ~ 'Early/Ongoing Gentrification',
          typ_cat == "['AdvG']" ~ 'Advanced Gentrification',
          typ_cat == "['ARE']" ~ 'At Risk of Becoming Exclusive',
          typ_cat == "['ARG']" ~ 'At Risk of Gentrification',
          typ_cat == "['BE']" ~ 'Becoming Exclusive', 
          typ_cat == "['EOG']" ~ 'Early/Ongoing Gentrification',
          typ_cat == "['OD']" ~ 'Ongoing Displacement',
          typ_cat == "['SAE']" ~ 'Stable/Advanced Exclusive', 
          typ_cat == "['LISD']" ~ 'Low-Income/Susceptible to Displacement',
          typ_cat == "['SMMI']" ~ 'Stable Moderate/Mixed Income',
          TRUE ~ "Unavailable or Unreliable Data"
        ), 
        levels = 
          c(
            'Low-Income/Susceptible to Displacement',
            'Ongoing Displacement',
            'At Risk of Gentrification',
            'Early/Ongoing Gentrification',
            'Advanced Gentrification',
            'Stable Moderate/Mixed Income',
            'At Risk of Becoming Exclusive',
            'Becoming Exclusive',
            'Stable/Advanced Exclusive',
            'Unavailable or Unreliable Data'
          )
      ),
    real_mhval_23 = case_when(real_mhval_23 > 0 ~ real_mhval_23),
    real_mrent_23 = case_when(real_mrent_23 > 0 ~ real_mrent_23)
  ) %>% 
  group_by(city) %>% 
  mutate(
    rm_real_mhval_23 = median(real_mhval_23, na.rm = TRUE), 
    rm_real_mrent_23 = median(real_mrent_23, na.rm = TRUE), 
    rm_per_nonwhite_23 = median(per_nonwhite_23, na.rm = TRUE), 
    rm_per_col_23 = median(per_col_23, na.rm = TRUE)
  ) %>% 
  group_by(GEOID) %>% 
  mutate(
    per_ch_li = (all_li_count_23 - all_li_count_00) / all_li_count_00,
    ci = case_when(
      # Add community input if needed
      TRUE ~ NA_character_
    ), 
    popup = # What to include in the popup 
      str_c(
        '<b>Tract: ', GEOID, '<br>',  
        Typology, '</b>',
        # Community input layer
        case_when(!is.na(ci) ~ ci, TRUE ~ ''),
        # Market
        '<br><br>',
        '<b><i><u>Market Dynamics</u></i></b><br>',
        'Tract median home value: ', case_when(!is.na(real_mhval_23) ~ dollar(real_mhval_23), TRUE ~ 'No data'), '<br>',
        'Tract home value change from 2000 to 2023: ', case_when(is.na(real_mhval_23) ~ 'No data', TRUE ~ percent(pctch_real_mhval_00_23, accuracy = .1)),'<br>',
        'Regional median home value: ', dollar(rm_real_mhval_23), '<br>',
        '<br>',
        'Tract median rent: ', case_when(!is.na(real_mrent_23) ~ dollar(real_mrent_23), TRUE ~ 'No data'), '<br>', 
        'Regional median rent: ', case_when(is.na(real_mrent_23) ~ 'No data', TRUE ~ dollar(rm_real_mrent_23)), '<br>', 
        'Tract rent change from 2012 to 2023: ', percent(pctch_real_mrent_12_23, accuracy = .1), '<br>',
        '<br>',
        'Rent gap (nearby - local): ', dollar(tr_rent_gap), '<br>',
        'Regional median rent gap: ', dollar(rm_rent_gap), '<br>',
        '<br>',
        # demographics
        '<b><i><u>Demographics</u></i></b><br>', 
        'Tract population: ', comma(pop_23), '<br>', 
        'Tract household count: ', comma(hh_23), '<br>', 
        'Percent renter occupied: ', percent(per_rent_23, accuracy = .1), '<br>',
        'Tract median income: ', dollar(real_hinc_23), '<br>', 
        'Percent low income hh: ', percent(per_all_li_23, accuracy = .1), '<br>', 
        'Percent change in LI: ', percent(per_ch_li, accuracy = .1), '<br>',
        '<br>',
        'Percent POC: ', percent(per_nonwhite_23, accuracy = .1), '<br>',
        'Regional median POC: ', percent(rm_per_nonwhite_23, accuracy = .1), '<br>',
        'Percent college educated: ', percent(per_col_23, accuracy = .1), '<br>',
        'Regional median educated: ', percent(rm_per_col_23, accuracy = .1), '<br>',
        '<br>',
        # risk factors
        '<b><i><u>Risk Factors</u></i></b><br>', 
        'Mostly low income: ', case_when(low_pdmt_medhhinc_23 == 1 ~ 'Yes', TRUE ~ 'No'), '<br>',
        'Mix low income: ', case_when(mix_low_medhhinc_23 == 1 ~ 'Yes', TRUE ~ 'No'), '<br>',
        'Rent change: ', case_when(dp_PChRent == 1 ~ 'Yes', TRUE ~ 'No'), '<br>',
        'Rent gap: ', case_when(dp_RentGap == 1 ~ 'Yes', TRUE ~ 'No'), '<br>',
        'Hot Market: ', case_when(hotmarket_23 == 1 ~ 'Yes', TRUE ~ 'No'), '<br>',
        'Vulnerable to gentrification: ', case_when(vul_gent_23 == 1 ~ 'Yes', TRUE ~ 'No'), '<br>', 
        'Gentrified from 1990 to 2000: ', case_when(gent_90_00 == 1 | gent_90_00_urban == 1 ~ 'Yes', TRUE ~ 'No'), '<br>', 
        'Gentrified from 2000 to 2023: ', case_when(gent_00_23 == 1 | gent_00_23_urban == 1 ~ 'Yes', TRUE ~ 'No')
      )) %>% 
  ungroup() %>% 
  data.frame()

###
# Begin Download tracts in each of the shapes in sf (simple feature) class
###
 tracts <- 
     get_acs(
         geography = "tract", 
         variables = "B01003_001", 
         state = '06',
         county = '067',  # Sacramento County
         geometry = TRUE, 
         year = 2023
     ) %>% 
     select(GEOID) %>% 
     mutate(GEOID = as.numeric(GEOID)) %>% 
     st_transform(st_crs(4326)) 
     saveRDS(tracts, '../data/outputs/downloads/sacramento_tracts.RDS')
###
# End
###

tracts <- readRDS('../data/outputs/downloads/sacramento_tracts.RDS')

# Join the tracts to the dataframe
df_sf <- 
  right_join(tracts, df) 

# ==========================================================================
# Select tracts within urban areas (OPTIONAL for Sacramento)
# ==========================================================================

###
# Begin Download Urban Areas (OPTIONAL)
###
# urban_areas <- 
#   urban_areas() %>% 
#   st_transform(st_crs(df_sf))
# saveRDS(urban_areas, "~/git/displacement-typologies/data/outputs/downloads/urban_areas_sacramento.rds")
### 
# End Download
###

# For Sacramento, you can either:
# 1. Use all tracts (no urban area filter), OR
# 2. Filter to Sacramento metro area

# Option 1: All tracts
df_sf_urban <- df_sf

# Option 2: Filter to urban areas (uncomment if desired)
# urban_areas <- readRDS("~/git/displacement-typologies/data/outputs/downloads/urban_areas_sacramento.rds")
# df_sf_urban <- df_sf %>% st_crop(urban_areas)

# Simplify for faster rendering
df_sf_urban <- df_sf_urban %>% ms_simplify(keep = 0.5)

# ==========================================================================
# overlays
# ==========================================================================

### Redlining

red <- 
  rbind(
    geojson_sf('../data/overlays/CASacramento1937.json') %>% 
      mutate(city = 'Sacramento')
  ) %>% 
  mutate(
    Grade = 
      factor(
        case_when(
          grade == 'A' ~ 'A "Best"',
          grade == 'B' ~ 'B "Still Desirable"',
          grade == 'C' ~ 'C "Definitely Declining"',
          grade == 'D' ~ 'D "Hazardous"'
        ), 
        levels = c(
          'A "Best"',
          'B "Still Desirable"',
          'C "Definitely Declining"',
          'D "Hazardous"')
      ), 
    popup = str_c("1930's Redline Grade: ", Grade)
  ) 

### Industrial points (OPTIONAL - if you have data)

# industrial <- 
#     read_excel("~/git/displacement-typologies/data/overlays/industrial/industrial_NATIONAL.xlsx") %>% 
#     filter(Latitude != '') %>% 
#     st_as_sf(
#         coords = c('Longitude', 'Latitude'), 
#         crs = 4269) %>% 
#     st_transform(st_crs(df_sf_urban))

### HUD

hud <- 
  read_csv('../data/overlays/Public_Housing_Buildings.csv') %>% 
  filter(X != '') %>%
  st_as_sf(
    coords = c("X","Y"), 
    crs = 4269) %>% 
  st_transform(st_crs(df_sf_urban))

### Rail data
rail <- 
  st_join(
    fread('../data/inputs/tod_database_download.csv') %>% 
      st_as_sf(
        coords = c('Longitude', 'Latitude'), 
        crs = 4269
      ) %>% 
      st_transform(4326), 
    df_sf_urban %>% select(city), 
    join = st_intersects
  ) %>% 
  filter(!is.na(city))

### Hospitals
hospitals <- 
  st_join(
    fread('../data/inputs/Hospitals.csv') %>% 
      st_as_sf(
        coords = c('X', 'Y'), 
        crs = 4269
      ) %>% 
      st_transform(4326), 
    df_sf_urban %>% select(city), 
    join = st_intersects
  ) %>% 
  mutate(
    popup = str_c(NAME, "<br>", NAICS_DESC), 
    legend = "Hospitals"
  ) %>% 
  filter(!is.na(city), grepl("GENERAL", NAICS_DESC))

### Universities
university <- 
  st_join(
    fread('../data/inputs/university_HD2023.csv') %>% 
      st_as_sf(
        coords = c('LONGITUD', 'LATITUDE'), 
        crs = 4269
      ) %>% 
      st_transform(4326), 
    df_sf_urban %>% select(city), 
    join = st_intersects
  ) %>% 
  filter(ICLEVEL == 1, SECTOR < 3) %>%
  mutate(
    legend = case_when(
      SECTOR == 1 ~ 'Major University', 
      SECTOR == 2 ~ 'Medium University or College')
  ) %>% 
  filter(!is.na(city))

### Road map
###
# Begin download road maps (OPTIONAL)
###
 road_map <- 
     primary_secondary_roads('06', class = 'sf') %>% 
     filter(RTTYP %in% c('I','U')) %>% 
     ms_simplify(keep = 0.1) %>% 
     st_transform(st_crs(df_sf_urban)) %>%
     st_join(., df_sf_urban %>% select(city), join = st_intersects) %>% 
     mutate(rt = case_when(RTTYP == 'I' ~ 'Interstate', RTTYP == 'U' ~ 'US Highway')) %>% 
     filter(!is.na(city))
 saveRDS(road_map, '../data/outputs/downloads/roads_sacramento.rds')
###
# End
###

road_map <- readRDS('../data/outputs/downloads/roads_sacramento.rds')

### Opportunity Zones
# oppzones.csv is already joined to data at the beginning (lines 46-53)
# Since it's tract-level data (not polygon geometry), we skip the map overlay
# The opp_zone column is available in the data if needed in popups

# ==========================================================================
# Maps
# ==========================================================================

#
# Color palettes 
# --------------------------------------------------------------------------

redline_pal <- 
  colorFactor(
    c("#4ac938", "#2b83ba", "#ff8c1c", "#ff1c1c"), 
    domain = red$Grade, 
    na.color = "transparent"
  )

displacement_typologies_pal <- 
  colorFactor(
    c(
      '#87CEFA',
      '#6495ED',
      '#9e9ac8', 
      '#756bb1',
      '#54278f',
      '#FBEDE0',
      '#F4C08D',
      '#EE924F',
      '#C95123',
      "#C0C0C0"),
    domain = df$Typology, 
    na.color = '#C0C0C0'
  )

rail_pal <- 
  colorFactor(
    c(
      '#377eb8',
      '#4daf4a',
      '#984ea3'
    ), 
    domain = c("Proposed Transit", "Planned Transit", "Existing Transit"))

road_pal <- 
  colorFactor(
    c(
      '#333333',
      '#666666'
    ), 
    domain = c("Interstate", "US Highway"))

# ==========================================================================
# Mapping functions
# ==========================================================================

map_it <- function(city_name, st){
  leaflet(data = df_sf_urban %>% filter(city == city_name)) %>% 
    addMapPane(name = "polygons", zIndex = 410) %>% 
    addMapPane(name = "maplabels", zIndex = 420) %>%
    addProviderTiles("CartoDB.PositronNoLabels") %>%
    addProviderTiles("CartoDB.PositronOnlyLabels", 
                     options = leafletOptions(pane = "maplabels"),
                     group = "map labels") %>%
    addEasyButton(
      easyButton(
        icon="fa-crosshairs", 
        title="My Location",
        onClick=JS("function(btn, map){ map.locate({setView: true}); }"))) %>%
    # Displacement typology
    addPolygons(
      data = df_sf_urban %>% filter(city == city_name), 
      group = "Displacement Typology", 
      label = ~Typology,
      labelOptions = labelOptions(textsize = "12px"),
      fillOpacity = .5, 
      color = ~displacement_typologies_pal(Typology), 
      stroke = TRUE, 
      weight = .7, 
      opacity = .60, 
      highlightOptions = highlightOptions(
        color = "#ff4a4a",
        weight = 5,
        bringToFront = TRUE
      ),        
      popup = ~popup, 
      popupOptions = popupOptions(maxHeight = 215, closeOnClick = TRUE)
    ) %>%   
    addLegend(
      pal = displacement_typologies_pal, 
      values = ~Typology, 
      group = "Displacement Typology", 
      title = "Displacement Typology"
    ) %>% 
    # Redlined areas
    addPolygons(
      data = red %>% filter(city == city_name), 
      group = "Redlined Areas", 
      label = ~Grade,
      labelOptions = labelOptions(textsize = "12px"),
      fillOpacity = .3, 
      color = ~redline_pal(Grade), 
      stroke = TRUE, 
      weight = 1, 
      opacity = .8, 
      highlightOptions = highlightOptions(
        color = "#ff4a4a", 
        weight = 5,
        bringToFront = TRUE
      ), 
      popup = ~popup
    ) %>%   
    addLegend(
      data = red, 
      pal = redline_pal, 
      values = ~Grade, 
      group = "Redlined Areas",
      title = "Redline Zones"
    ) %>%  
    # Roads
    addPolylines(
      data = road_map %>% filter(city == city_name), 
      group = "Highways", 
      color = ~road_pal(rt), 
      stroke = TRUE, 
      weight = 1, 
      opacity = .1    
    ) %>%
    # Public Housing
    addCircleMarkers(
      data = hud[(df_sf_urban %>% filter(city == city_name)),],
      radius = 5, 
      label = ~FORMAL_PARTICIPANT_NAME,
      lng = ~LON, 
      lat = ~LAT, 
      color = "#ff7f00",
      group = 'Public Housing', 
      fillOpacity = .5, 
      stroke = FALSE
    ) %>%     
    # Rail
    addCircleMarkers(
      data = rail %>% filter(city == city_name), 
      label = ~Buffer, 
      radius = 5, 
      color = ~rail_pal(Buffer),
      group = 'Transit Stations', 
      popup = ~Buffer,
      fillOpacity = .8, 
      stroke = TRUE, 
      weight = .6
    ) %>%     
    addLegend(
      data = rail, 
      pal = rail_pal, 
      values = ~Buffer, 
      group = "Transit Stations", 
      title = "Transit Stations"
    ) %>%  
    # University
    addCircleMarkers(
      data = university %>% filter(city == city_name), 
      label = ~INSTNM, 
      radius = 5, 
      color = '#39992b',
      group = 'Universities & Colleges', 
      popup = ~INSTNM,
      fillOpacity = .8, 
      stroke = TRUE, 
      weight = .6
    ) %>%     
    # Hospitals
    addCircleMarkers(
      data = hospitals %>% filter(city == city_name), 
      label = ~NAME, 
      radius = 5, 
      color = '#e41a1c',
      group = 'Hospitals', 
      popup = ~popup,
      fillOpacity = .8, 
      stroke = TRUE, 
      weight = .6) 
}

# Opportunity Zones function removed - CSV only has tract IDs, not polygon geometry
# If you get polygon boundaries for opportunity zones, you can re-add this

# Options
options <- function(map = .){
  map %>% 
    addLayersControl(
      overlayGroups = 
        c("Displacement Typology", 
          "Redlined Areas", 
          "Hospitals", 
          "Universities & Colleges", 
          'Public Housing',
          "Transit Stations", 
          "Highways"),
      options = layersControlOptions(collapsed = FALSE, maxHeight = "auto")) %>% 
    hideGroup(
      c("Redlined Areas",
        "Hospitals", 
        "Universities & Colleges", 
        'Public Housing',
        "Transit Stations"))
}

#
# Sacramento map
# --------------------------------------------------------------------------

sacramento <- 
  map_it("Sacramento", 'CA') %>% 
  options() %>% 
  setView(lng = -121.4, lat = 38.6, zoom = 10)

# save map
htmlwidgets::saveWidget(sacramento, file="../maps/sacramento_udp.html")

print("✓ Sacramento map created successfully!")
print("Output: ~/git/displacement-typologies/maps/sacramento_udp.html")

#
# Create file exports (GPKG and CSV)
# --------------------------------------------------------------------------

sac_sf <- df_sf_urban %>% filter(city == "Sacramento") %>% select(GEOID, Typology)
st_write(sac_sf, "../data/downloads_for_public/sacramento.gpkg", append=FALSE)
write_csv(sac_sf %>% st_set_geometry(NULL), "../data/downloads_for_public/sacramento.csv")

print("✓ Sacramento data exports created:")
print("  - ../data/downloads_for_public/sacramento.gpkg")
print("  - ../data/downloads_for_public/sacramento.csv")