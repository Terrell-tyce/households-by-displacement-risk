# ==========================================================================
# ==========================================================================
# ==========================================================================
# DISPLACEMENT TYPOLOGY SET UP
# ==========================================================================
# ==========================================================================
# ==========================================================================

# ==========================================================================
# DISPLACEMENT TYPOLOGY - LAG VARIABLES
# Sacramento County, 2024 ACS Data
# ==========================================================================
# Adapted from: https://github.com/urban-displacement/displacement-typologies
# Updated for 2024 ACS, modern sf/sp libraries, and Sacramento only
# ==========================================================================

library(tidyverse)
library(tidycensus)
library(tigris)
library(sp)
library(spdep)
library(sf)
library(data.table)

#options(tigris_use_cache = TRUE)
#set wd to api key location


### Set API key
api_key <- trimws(readLines("api_key.txt"))
census_api_key(api_key)
DATA_DIR <- "I:/Projects/Josh/RHNA/Data/POPEMP_25/emp25_data"
input_path <- paste0(DATA_DIR,'/inputs/')
output_path <- paste0(DATA_DIR,'/outputs/')

# ==========================================================================
# Pull in data
# ==========================================================================
# ENTER CITY NAME HERE (options: Sacramento, San Francisco, San Jose, SACOG)
city_name <- 'SACOG'

if (city_name == 'Sacramento'){
    st <- c("CA")
    FIPS <- c('067') # Sacramento
} else if (city_name == 'SACOG'){
    st <- c("CA")
    FIPS <- c('067','017','113','115','101','061')
}


df <- 
  read_csv(paste0(output_path,"databases/",city_name,"_database_2024.csv")) %>% 
  dplyr::select(!...1) %>% 
  mutate(city = city_name)

# ==========================================================================
# Create rent gap and extra local change in rent
# ==========================================================================
# B25064_001 = Median Gross Rent (current year)
# This variable has been consistent since 2012, so no changes needed

tr_rent <- function(year, state){
  get_acs(
    geography = "tract",
    variables = c('medrent' = 'B25064_001'),
    state = state,
    county = NULL,
    geometry = FALSE,
    cache_table = TRUE,
    output = "tidy",
    year = year,
    keep_geo_vars = TRUE
  ) %>%
    dplyr::select(-moe) %>% 
    rename(medrent = estimate) %>% 
    mutate(
      county = str_sub(GEOID, 3, 5), 
      state = str_sub(GEOID, 1, 2),
      year = str_sub(year, 3, 4) 
    )
}

# Download rent data
tr_rents24 <- 
  tr_rent(year = 2024, state = st) %>% 
  mutate(COUNTY = substr(GEOID, 1, 5))

tr_rents12 <- 
  tr_rent(year = 2012, state = st) %>% 
  mutate(
    COUNTY = substr(GEOID, 1, 5),
    medrent = medrent * 1.35  # Inflation adjustment
  )

gc()

# Combine and reshape rent data
tr_rents <- 
  bind_rows(tr_rents24, tr_rents12) %>% 
  unite("variable", c(variable, year), sep = "") %>% 
  pivot_wider(names_from = variable, values_from = medrent) %>% 
  group_by(COUNTY) %>%
  mutate(
    tr_medrent24 = case_when(
      is.na(medrent24) ~ median(medrent24, na.rm = TRUE),
      TRUE ~ medrent24
    ),
    tr_medrent12 = case_when(
      is.na(medrent12) ~ median(medrent12, na.rm = TRUE),
      TRUE ~ medrent12
    ),
    tr_chrent = tr_medrent24 - tr_medrent12,
    tr_pchrent = (tr_medrent24 - tr_medrent12) / tr_medrent12,
    rm_medrent24 = median(tr_medrent24, na.rm = TRUE), 
    rm_medrent12 = median(tr_medrent12, na.rm = TRUE)
  ) %>% 
  dplyr::select(-medrent12, -medrent24) %>% 
  distinct() %>% 
  group_by(GEOID) %>% 
  filter(row_number() == 1) %>% 
  ungroup()

gc()

# ==========================================================================
# Get tract geometries (Sacramento County only)
# ==========================================================================

stsp <- tracts("CA", cb = TRUE, class = 'sf') %>% 
  filter(COUNTYFP %in% FIPS)

# Join rent data to tracts
stsp <- stsp %>%
  left_join(
    tr_rents %>% mutate(GEOID = as.character(GEOID)),
    by = "GEOID"
  )

# Check for tracts without rent data
if(any(is.na(stsp$tr_medrent24))) {
  print(paste("Warning: ", sum(is.na(stsp$tr_medrent24)), 
              "tracts missing rent data"))
}
# ==========================================================================
# Create neighbor matrix using spdep
# ==========================================================================

# Convert to sp temporarily for spdep operations
stsp_sp <- as_Spatial(stsp)

coords <- coordinates(stsp_sp)
IDs <- row.names(as(stsp_sp, "data.frame"))
stsp_nb <- poly2nb(stsp_sp)
lw_bin <- nb2listw(stsp_nb, style = "W", zero.policy = TRUE)

kern1 <- knn2nb(knearneigh(coords, k = 1), row.names = IDs)
dist <- unlist(nbdists(kern1, coords))
max_1nn <- max(dist)
dist_nb <- dnearneigh(coords, d1 = 0, d2 = 0.1 * max_1nn, row.names = IDs)

spdep::set.ZeroPolicyOption(TRUE)

dists <- nbdists(dist_nb, coordinates(stsp_sp))
idw <- lapply(dists, function(x) 1 / (x^2))
lw_dist_idwW <- nb2listw(dist_nb, glist = idw, style = "W")

# ==========================================================================
# Create lag variables
# ==========================================================================

stsp_sp$tr_pchrent.lag <- lag.listw(lw_dist_idwW, stsp_sp$tr_pchrent)
stsp_sp$tr_chrent.lag <- lag.listw(lw_dist_idwW, stsp_sp$tr_chrent)
stsp_sp$tr_medrent24.lag <- lag.listw(lw_dist_idwW, stsp_sp$tr_medrent24)

# ==========================================================================
# Join lag variables with original dataframe
# ==========================================================================

lag <-  
  left_join(
    df, 
    data.frame(stsp_sp@data) %>%
      mutate(GEOID = as.character(GEOID)) %>%
      dplyr::select(GEOID, tr_medrent24, tr_medrent12, tr_chrent, 
                    tr_pchrent, tr_pchrent.lag, tr_chrent.lag, 
                    tr_medrent24.lag),
    by = "GEOID"
  ) %>%
  mutate(
    tr_rent_gap = tr_medrent24.lag - tr_medrent24, 
    tr_rent_gapprop = tr_rent_gap / ((tr_medrent24 + tr_medrent24.lag) / 2),
    rm_rent_gap = median(tr_rent_gap, na.rm = TRUE), 
    rm_rent_gapprop = median(tr_rent_gapprop, na.rm = TRUE), 
    rm_pchrent = median(tr_pchrent, na.rm = TRUE),
    rm_pchrent.lag = median(tr_pchrent.lag, na.rm = TRUE),
    rm_chrent.lag = median(tr_chrent.lag, na.rm = TRUE),
    rm_medrent24.lag = median(tr_medrent24.lag, na.rm = TRUE), 
    dp_PChRent = case_when(
      tr_pchrent > 0 & tr_pchrent > rm_pchrent ~ 1,
      tr_pchrent.lag > rm_pchrent.lag ~ 1,
      TRUE ~ 0
    ),
    dp_RentGap = case_when(
      tr_rent_gapprop > 0 & tr_rent_gapprop > rm_rent_gapprop ~ 1,
      TRUE ~ 0
    )
  )

# ==========================================================================
# PUMA - Population Density Classification
# ==========================================================================
# B05006_001 = Total population (this variable is stable across years)

puma <-
  get_acs(
    geography = "public use microdata area", 
    variable = "B05006_001", 
    year = 2024,
    geometry = TRUE, 
    state = st
  ) %>% 
  mutate(
    area_sqmile = as.numeric(sf::st_area(geometry)) / 2589988,  # Convert to square miles
    puma_density = estimate / area_sqmile
  ) %>% 
  rename(PUMAID = GEOID) %>%
  dplyr::select(PUMAID, puma_density, geometry)

# Get density for Sacramento tracts
stsf <- stsp %>% 
  st_centroid() %>% 
  st_join(puma) %>% 
  st_drop_geometry() %>% 
  mutate(
    dense = case_when(puma_density >= 3000 ~ 1, TRUE ~ 0)
  ) %>%
  dplyr::select(GEOID, puma_density, dense) %>% 
  mutate(GEOID = as.character(GEOID))

lag <- lag %>% 
  left_join(stsf, by = "GEOID")

# ==========================================================================
# Export Data
# ==========================================================================
# Before fwrite:
print(paste("Rows in lag:", nrow(lag)))
print(paste("Missing tr_rent_gap:", sum(is.na(lag$tr_rent_gap))))
print(paste("Missing dense:", sum(is.na(lag$dense))))

fwrite(lag,paste0(output_path, "lags/lag_",city_name,"_2024.csv"))

print("✓ Lag variables created successfully!")
print(paste("Output: ../data/outputs/lags/lag_",city_name,"_2024.csv"))
print(paste("Tracts processed:", nrow(lag)))
head(lag %>% dplyr::select(GEOID, tr_rent_gap, tr_rent_gapprop, dense, dp_PChRent, dp_RentGap))