# Combined Spatial RDD Analysis Pipeline
# This script combines database creation and RDD analysis for 1860 wealth inequality

# Set the working directory to the script's location
setwd(tryCatch(getSrcDirectory(function(dummy) {dummy}), error = function(e) "."))

# Set a CRAN mirror
options(repos = c(CRAN = "https://cloud.r-project.org"))

# List of packages to install
packages <- c("sf", "dplyr", "purrr", "readr", "ipumsr", "stringr", 
              "rdrobust", "tidyr", "progress", "tools")

# Install packages not yet installed
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages], quiet = TRUE)
}

# Load the packages
invisible(lapply(packages, library, character.only = TRUE))
cat("Required packages loaded.\n")

# Create necessary directories
required_dirs <- c(
  "Data/IPUMS_Raw/1860",
  "Data/Shapefiles/1860",
  "Data/RDD_Database",
  "Results/RDD_wealth/CSV", 
  "Results/RDD_wealth/Geopackage",
  "Results/RDD_occupations/CSV", 
  "Results/RDD_occupations/Geopackage"
)
for (dir_path in required_dirs) {
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
    cat("Created directory:", dir_path, "\n")
  }
}

# --- PART 1: IPUMS Data Download for 1860 Population ---

get_api_key <- function() {
  api_key_env <- Sys.getenv("IPUMS_API_KEY")
  
  if (nzchar(api_key_env)) {
    cat("Using IPUMS API key found in environment variable IPUMS_API_KEY.\n")
    api_key <- api_key_env
  } else {
    ipums_url <- "https://account.ipums.org/api_keys"
    cat("This script requires an IPUMS API key, which can be obtained at", ipums_url, "\n")
    api_key <- readline(prompt = "Please enter your API key: ")
    
    if (nzchar(api_key)) {
      Sys.setenv(IPUMS_API_KEY = api_key)
      cat("IPUMS API key has been set for this session.\n")
    }
  }
  
  if (!nzchar(api_key)) {
    stop("API key is required to download NHGIS data. Exiting.")
  }
  return(api_key)
}

extract_all_zips_ipums <- function(directory) {
  zip_files <- list.files(directory, pattern = "\\.zip$", full.names = TRUE, recursive = TRUE)
  if (length(zip_files) == 0) return(invisible())
  
  cat(sprintf("Found %d zip files to extract in %s\n", length(zip_files), directory))
  for (zip_file in zip_files) {
    extract_dir <- gsub("\\.zip$", "", zip_file)
    if (!dir.exists(extract_dir)) {
      cat("Extracting:", basename(zip_file), "to", extract_dir, "\n")
      dir.create(extract_dir, recursive = TRUE)
      tryCatch({
        utils::unzip(zip_file, exdir = extract_dir)
      }, error = function(e) {
        warning(sprintf("Failed to extract %s: %s", basename(zip_file), e$message))
      })
    }
  }
  
  new_zip_files <- list.files(directory, pattern = "\\.zip$", full.names = TRUE, recursive = TRUE)
  new_zip_files <- new_zip_files[!new_zip_files %in% zip_files]
  if (length(new_zip_files) > 0) extract_all_zips_ipums(directory)
}

download_ipums_data_1860 <- function() {
  year <- "1860"
  year_dir <- file.path("Data/IPUMS_Raw", year)
  
  cat(sprintf("\n--- Downloading IPUMS data for year: %s ---\n", year))
  
  # Set IPUMS collection and get API key
  set_ipums_default_collection("nhgis")
  api_key <- get_api_key()
  
  # Define extract for 1860 population data
  pop_specs <- ds_spec(
    "1860_cPAX", 
    data_tables = c("NT1", "NT6"), # NT1: Total Pop; NT6: Characteristics (race, enslaved status)
    geog_levels = "county"
  )
  
  combined_extract <- define_extract_nhgis(
    description = paste("Population data for", year),
    datasets = list(pop_specs)
  )
  
  cat("Submitting IPUMS extract request...\n")
  submitted_extract <- submit_extract(combined_extract, api_key = api_key)
  cat("Waiting for IPUMS extract to complete...\n")
  ready_extract <- wait_for_extract(submitted_extract, api_key = api_key)
  
  cat("Downloading IPUMS extract files...\n")
  downloaded_files <- download_extract(
    ready_extract, 
    download_dir = year_dir, 
    api_key = api_key, 
    overwrite = FALSE
  )
  
  # Extract all zip files
  extract_all_zips_ipums(year_dir)
  
  # Process CSV files
  csv_files <- list.files(year_dir, pattern = "\\.csv$", full.names = TRUE, recursive = TRUE)
  csv_files <- csv_files[!grepl("codebook", csv_files, ignore.case = TRUE)]
  
  if (length(csv_files) == 0) {
    stop("No IPUMS CSV data files found for 1860")
  }
  
  # Read and combine data tables
  data_tables_list <- list()
  for (csv_path in csv_files) {
    cat(sprintf("Reading IPUMS dataset from %s...\n", basename(csv_path)))
    data_tables_list[[basename(csv_path)]] <- tryCatch({
      readr::read_csv(csv_path, show_col_types = FALSE, guess_max = 5000) %>% 
        mutate(GISJOIN = as.character(GISJOIN))
    }, error = function(e) {
      warning(sprintf("Error reading IPUMS CSV %s: %s", basename(csv_path), e$message))
      NULL
    })
  }
  
  data_tables_list <- data_tables_list[!sapply(data_tables_list, is.null)]
  
  # Combine tables if multiple
  if (length(data_tables_list) == 1) {
    combined_data <- data_tables_list[[1]]
  } else {
    combined_data <- data_tables_list[[1]]
    if (length(data_tables_list) > 1) {
      for (i in 2:length(data_tables_list)) {
        table_to_join <- data_tables_list[[i]]
        cols_to_join <- setdiff(names(table_to_join), names(combined_data))
        table_to_join_selected <- table_to_join %>% select(GISJOIN, all_of(cols_to_join))
        combined_data <- full_join(combined_data, table_to_join_selected, by = "GISJOIN")
      }
    }
  }
  
  combined_data$year <- as.integer(year)
  
  # Save processed data
  output_file <- "Data/census.csv"
  write_csv(combined_data, output_file)
  cat("IPUMS census data for 1860 has been saved to", output_file, "\n")
  
  return(output_file)
}

# --- PART 2: Helper Functions ---

fix_geometries <- function(sf_object) {
  sf_object %>%
    st_make_valid() %>%
    st_buffer(0)
}

add_slavery_legality <- function(df) {
  slave_states <- c(
    "Alabama", "Arkansas", "Delaware", "District of Columbia", "Florida", "Georgia",
    "Kentucky", "Louisiana", "Maryland", "Mississippi", "Missouri",
    "North Carolina", "South Carolina", "Tennessee", "Texas", "Virginia", "West Virginia"
  )
  
  df %>%
    mutate(slavery_legal = as.integer(state %in% slave_states))
}

read_geology_data <- function(year_val, type) {
  file_path <- paste0("Data/Geology/USGS_", type, "/", type, "_", year_val, ".csv")
  if (!file.exists(file_path)) {
    warning("Geology file not found: ", file_path, ". Returning empty data frame.")
    return(data.frame(GISJOIN = character(0)) %>% 
             mutate(!!paste0(type, "_mean") := numeric(0)))
  }
  
  tryCatch({
    read_csv(file_path, show_col_types = FALSE,
             col_types = cols(GISJOIN = col_character(), .default = col_double())) %>%
      select(GISJOIN, contains(paste0(type, "_mean"))) %>%
      mutate(GISJOIN = as.character(GISJOIN)) %>%
      distinct(GISJOIN, .keep_all = TRUE)
  }, error = function(e) {
    warning(paste("Error reading geology file:", file_path, "\nError:", e$message))
    data.frame(GISJOIN = character(0)) %>% 
      mutate(!!paste0(type, "_mean") := numeric(0))
  })
}

read_ph_data <- function(year_val) {
  ph_files_info <- list(
    list(name = "ph_0_5", path = paste0("Data/Geology/Soilgrids/ph_0_5_", year_val, ".csv")),
    list(name = "ph_5_15", path = paste0("Data/Geology/Soilgrids/ph_5_15_", year_val, ".csv")),
    list(name = "ph_15_30", path = paste0("Data/Geology/Soilgrids/ph_15_30_", year_val, ".csv"))
  )
  
  ph_data_list <- list()
  ph_col_names <- c("ph_0-5_mean", "ph_5-15_mean", "ph_15-30_mean")
  
  for (i in 1:length(ph_files_info)) {
    file_info <- ph_files_info[[i]]
    expected_col <- ph_col_names[i]
    
    if (!file.exists(file_info$path)) {
      warning("pH file not found: ", file_info$path, ". Skipping this pH layer.")
      df <- data.frame(GISJOIN = character(0))
      df[[expected_col]] <- numeric(0)
      ph_data_list[[file_info$name]] <- df
    } else {
      ph_data_list[[file_info$name]] <- read_csv(
        file_info$path, 
        col_types = cols(GISJOIN = col_character(), .default = col_double()),
        show_col_types = FALSE
      ) %>%
        select(GISJOIN, all_of(expected_col)) %>%
        mutate(GISJOIN = as.character(GISJOIN)) %>%
        distinct(GISJOIN, .keep_all = TRUE)
    }
  }
  
  # Join all pH data
  ph_data_combined <- ph_data_list[[1]]
  if (length(ph_data_list) > 1) {
    for (j in 2:length(ph_data_list)) {
      ph_data_combined <- full_join(ph_data_combined, ph_data_list[[j]], by = "GISJOIN")
    }
  }
  
  # Calculate mean pH if all columns are present
  if (all(ph_col_names %in% names(ph_data_combined))) {
    ph_data_combined <- ph_data_combined %>%
      rowwise() %>%
      mutate(ph_mean = mean(c(`ph_0-5_mean`, `ph_5-15_mean`, `ph_15-30_mean`), na.rm = TRUE)) %>%
      ungroup() %>%
      select(GISJOIN, ph_mean)
  } else {
    warning("Not all pH data columns present for mean calculation.")
    ph_data_combined$ph_mean <- NA_real_
    ph_data_combined <- ph_data_combined %>% select(GISJOIN, ph_mean)
  }
  
  return(ph_data_combined)
}

# --- PART 3: Create RDD Database ---

create_rdd_database_1860 <- function() {
  cat("\n=== Creating RDD Database for 1860 ===\n")
  year_val <- "1860"
  
  # 1. Load 1860 county shapefile
  shp_dir <- "Data/US_county_1860/"
  shp_files <- list.files(shp_dir, pattern = "\\.shp$", full.names = TRUE)
  if (length(shp_files) == 0) {
    stop(sprintf("No shapefile found for 1860 in %s.", shp_dir))
  }
  
  shapefile_path <- shp_files[1]
  cat("Using county shapefile:", shapefile_path, "\n")
  
  counties_sf <- st_read(shapefile_path, quiet = TRUE) %>%
    fix_geometries() %>%
    mutate(GISJOIN = as.character(GISJOIN))
  
  # Standardize state name column
  if ("STATENAM" %in% names(counties_sf)) {
    counties_sf <- counties_sf %>% rename(state = STATENAM)
  } else if (!"state" %in% names(counties_sf)) {
    warning("Column 'state' or 'STATENAM' not found in shapefile.")
    counties_sf$state <- NA_character_
  }
  
  # 2. Add border county data
  border_file_path <- file.path("Data", "Border", paste0("border_", year_val, ".csv"))
  if (file.exists(border_file_path)) {
    border_data <- read_csv(border_file_path, show_col_types = FALSE,
                            col_types = cols(GISJOIN = col_character(), .default = col_double())) %>%
      mutate(GISJOIN = as.character(GISJOIN)) %>%
      distinct(GISJOIN, .keep_all = TRUE)
    
    # Standardize border column name
    border_col_candidates <- names(border_data)[tolower(names(border_data)) == "border"]
    if (length(border_col_candidates) > 0) {
      border_data <- border_data %>% 
        select(GISJOIN, border_val = !!sym(border_col_candidates[1]))
      
      counties_sf <- counties_sf %>%
        left_join(border_data, by = "GISJOIN") %>%
        mutate(border_county = ifelse(!is.na(border_val) & border_val == 1, 1, 0)) %>%
        select(-border_val)
      
      cat("Border data loaded from:", border_file_path, "\n")
    } else {
      cat("Border file found but 'border' column missing. Setting border_county to 0.\n")
      counties_sf$border_county <- 0
    }
  } else {
    cat("Border file not found. Setting border_county to 0 for all counties.\n")
    counties_sf$border_county <- 0
  }
  
  # 3. Load wealth and occupation data from Results/Maps.gpkg
  maps_gpkg_path <- "Results/Maps.gpkg"
  if (!file.exists(maps_gpkg_path)) {
    stop("Results/Maps.gpkg not found! This file should contain wealth and occupation statistics.")
  }
  
  cat("Loading wealth and occupation data from:", maps_gpkg_path, "\n")
  wealth_occupation_data <- st_read(maps_gpkg_path, quiet = TRUE) %>%
    st_drop_geometry() %>%
    mutate(GISJOIN = as.character(GISJOIN)) %>%
    select(
      GISJOIN,
      # Only variables used in RDD analysis
      Mean_household, Median_household, Gini_household, observations_hh,
      Pct_heads_farmers_planters, Pct_heads_unskilled_laborers
    ) %>%
    distinct(GISJOIN, .keep_all = TRUE)
  
  # 4. Load IPUMS population data and calculate enslaved_per_white
  census_csv_path <- "Data/census.csv"
  if (!file.exists(census_csv_path)) {
    stop("Data/census.csv not found! Run IPUMS download first.")
  }
  
  cat("Loading IPUMS population data from:", census_csv_path, "\n")
  ipums_pop_data <- read_csv(census_csv_path, show_col_types = FALSE) %>%
    filter(year == as.integer(year_val)) %>%
    mutate(GISJOIN = as.character(GISJOIN))
  
  # Calculate enslaved_per_white using 1860 NHGIS variable codes
  population_data <- ipums_pop_data %>%
    transmute(
      GISJOIN,
      white_pop = AH3001,
      enslaved_pop = AH3003,
      enslaved_per_white = ifelse(white_pop > 0 & !is.na(enslaved_pop) & !is.na(white_pop), 
                                  (enslaved_pop / white_pop) * 100, NA_real_)
    ) %>%
    select(GISJOIN, white_pop, enslaved_per_white) %>%
    distinct(GISJOIN, .keep_all = TRUE)
  
  # 5. Load geology data (only variables used as controls in RDD)
  cat("Loading geology data for 1860...\n")
  slope_data <- read_geology_data(year_val, "slope")
  elevation_data <- read_geology_data(year_val, "elevation")
  ph_data <- read_ph_data(year_val)
  
  # 6. Merge all data
  cat("Merging all datasets...\n")
  rdd_database_sf <- counties_sf %>%
    left_join(wealth_occupation_data, by = "GISJOIN") %>%
    left_join(population_data, by = "GISJOIN") %>%
    left_join(slope_data, by = "GISJOIN") %>%
    left_join(elevation_data, by = "GISJOIN") %>%
    left_join(ph_data, by = "GISJOIN") %>%
    rename(
      slope = slope_mean,
      elevation = elevation_mean,
      ph = ph_mean
    )
  
  # Add slavery legality and year
  rdd_database_sf <- add_slavery_legality(rdd_database_sf)
  rdd_database_sf$year <- as.integer(year_val)
  
  # 7. Select only variables needed for RDD analysis
  rdd_variables <- c(
    "year", "state", "slavery_legal", "GISJOIN", "border_county", "white_pop",
    # Wealth outcome variables
    "Mean_household", "Median_household", "Gini_household", "observations_hh",
    # Occupation outcome variables  
    "Pct_heads_farmers_planters", "Pct_heads_unskilled_laborers",
    # Control variables
    "slope", "elevation", "ph", "enslaved_per_white"
  )
  
  # Add missing columns as NA
  for (col_name in rdd_variables) {
    if (!col_name %in% names(rdd_database_sf)) {
      rdd_database_sf[[col_name]] <- NA
      cat("Added missing column '", col_name, "' as NA.\n")
    }
  }
  
  # Final selection
  rdd_database_sf <- rdd_database_sf %>%
    select(all_of(rdd_variables), geometry)
  
  # 8. Save database
  csv_output_path <- "Data/RDD_Database/rdd_database_1860.csv"
  gpkg_output_path <- "Data/RDD_Database/rdd_database_1860.gpkg"
  
  # Save CSV (without geometry)
  rdd_database_df <- st_drop_geometry(rdd_database_sf)
  write_csv(rdd_database_df, csv_output_path)
  cat("RDD database (tabular) saved to:", csv_output_path, "\n")
  
  # Save GeoPackage (with geometry)
  st_write(rdd_database_sf, gpkg_output_path, delete_layer = TRUE, quiet = TRUE)
  cat("RDD database (spatial) saved to:", gpkg_output_path, "\n")
  
  # Print summary
  cat("\n=== RDD Database Summary ===\n")
  cat("Total counties:", nrow(rdd_database_df), "\n")
  cat("Variables:", paste(names(rdd_database_df), collapse = ", "), "\n")
  
  if ("slavery_legal" %in% names(rdd_database_df)) {
    cat("\nTreatment status (slavery_legal):\n")
    print(table(rdd_database_df$slavery_legal, useNA = "always"))
  }
  
  if ("border_county" %in% names(rdd_database_df)) {
    cat("\nBorder counties:\n")
    print(table(rdd_database_df$border_county, useNA = "always"))
  }
  
  # Check data completeness for key variables
  cat("\nData completeness for RDD variables:\n")
  key_vars <- c("Mean_household", "Median_household", "Gini_household", 
                "Pct_heads_farmers_planters", "Pct_heads_unskilled_laborers",
                "slope", "elevation", "ph", "enslaved_per_white")
  
  for (var in key_vars) {
    if (var %in% names(rdd_database_df)) {
      n_missing <- sum(is.na(rdd_database_df[[var]]))
      pct_complete <- round((1 - n_missing/nrow(rdd_database_df)) * 100, 1)
      cat(sprintf("  %s: %.1f%% complete (%d missing)\n", var, pct_complete, n_missing))
    }
  }
  
  return(csv_output_path)
}

# --- PART 4: RDD Analysis Functions ---

load_and_fix_shapefile_rdd <- function(path, name) {
  tryCatch({
    sf_obj <- st_read(path, quiet = TRUE)
    sf_obj <- st_make_valid(sf_obj)
    if (st_geometry_type(sf_obj, by_geometry = FALSE) %in% c("POLYGON", "MULTIPOLYGON")) {
      sf_obj <- st_cast(sf_obj, "MULTILINESTRING")
    } else {
      sf_obj <- st_cast(sf_obj, "LINESTRING")
    }
    return(sf_obj)
  }, error = function(e) {
    stop("Error loading or fixing ", name, " shapefile: ", conditionMessage(e))
  })
}

generate_border_points_rdd <- function(border_sf, n_points = 50) {
  if (!inherits(border_sf, "sf")) border_sf <- st_as_sf(border_sf)
  
  geom_type <- st_geometry_type(border_sf, by_geometry = FALSE)[1]
  
  border_line_features <- NULL
  if (geom_type == "MULTILINESTRING") {
    border_line_features <- st_cast(border_sf, "LINESTRING")
  } else if (geom_type == "LINESTRING") {
    border_line_features <- border_sf
  } else if (geom_type %in% c("POLYGON", "MULTIPOLYGON")) {
    cat("Warning: Border geometry is Polygon, attempting to convert to LineString.\n")
    border_line_features <- st_cast(st_cast(border_sf, "MULTILINESTRING"), "LINESTRING")
  } else {
    stop(paste("Border geometry is", geom_type, "- cannot process. Needs LINESTRING or MULTILINESTRING."))
  }
  
  border_line_features <- border_line_features[!st_is_empty(border_line_features), ]
  if (nrow(border_line_features) == 0) {
    stop("Border has no valid/non-empty LINESTRING features after filtering.")
  }
  
  border_line_features$length <- st_length(border_line_features)
  total_length <- sum(border_line_features$length)
  
  all_sampled_points_list <- list()
  
  for (i in 1:nrow(border_line_features)) {
    line_feature <- border_line_features[i, ]
    num_points_for_segment <- max(1, round(n_points * (line_feature$length / total_length)))
    
    if (units(line_feature$length)$numerator == "m" && line_feature$length > units::set_units(0.1, "m")) {
      sampled_on_line <- st_line_sample(line_feature, n = num_points_for_segment, type = "regular")
      
      if (!st_is_empty(sampled_on_line)) {
        points_from_segment <- st_cast(sampled_on_line, "POINT")
        if (length(points_from_segment) > 0) {
          all_sampled_points_list[[length(all_sampled_points_list) + 1]] <- points_from_segment
        }
      }
    }
  }
  
  if (length(all_sampled_points_list) == 0) {
    stop("No points could be sampled from any border line segments.")
  }
  
  combined_points_sfc <- do.call(c, all_sampled_points_list)
  border_points_sf <- st_sf(geometry = combined_points_sfc)
  border_points_sf$point_id <- 1:nrow(border_points_sf)
  
  cat(sprintf("Generated %d points along the border.\n", nrow(border_points_sf)))
  return(border_points_sf)
}

calculate_border_distance_rdd <- function(counties_centroids_sf, border_line_sf) {
  if (st_crs(counties_centroids_sf) != st_crs(border_line_sf)) {
    cat("CRS mismatch between counties and border. Transforming counties CRS.\n")
    counties_centroids_sf <- st_transform(counties_centroids_sf, st_crs(border_line_sf))
  }
  distances <- st_distance(counties_centroids_sf, border_line_sf)
  apply(distances, 1, min)
}

find_border_file_rdd <- function(default_path, search_dir, search_pattern) {
  if (file.exists(default_path)) return(default_path)
  
  cat("Default border file not found:", default_path, ". Searching in", search_dir, "...\n")
  alt_files <- list.files(search_dir, pattern = search_pattern, full.names = TRUE, recursive = TRUE)
  
  if (length(alt_files) > 0) {
    shp_alt_files <- grep("\\.shp$", alt_files, value = TRUE, ignore.case = TRUE)
    if (length(shp_alt_files) > 0) {
      cat("Found alternative border file:", shp_alt_files[1], "\n")
      return(shp_alt_files[1])
    }
    cat("Found alternative border file (non-shp):", alt_files[1], "\n")
    return(alt_files[1])
  }
  stop(paste("Border file not found at", default_path, "and no alternatives found."))
}

perform_rdd_analysis <- function(data_sf, outcome_var, point_geom, 
                                 include_slope, exclude_border, include_enslaved_cov, include_ph_cov) {
  
  result_row <- data.frame(point_id = point_geom$point_id, coef = NA, se = NA, p_value = NA,
                           n_treat = NA, n_control = NA, bw_left = NA, bw_right = NA)
  
  current_data <- data_sf
  current_data$original_row_id <- 1:nrow(current_data)
  
  tryCatch({
    if (exclude_border) {
      # For 50-point analysis, exclude border counties from both sides
      current_data <- current_data %>% filter(border_county != 1)
    }
    
    point_geom_sf <- st_sfc(point_geom$geometry, crs = st_crs(current_data))
    distances_to_point <- as.numeric(st_distance(current_data, point_geom_sf))
    current_data$scaled_dist <- distances_to_point / 1609.344  # Convert to miles
    current_data$scaled_dist[current_data$treatment == 0] <- -current_data$scaled_dist[current_data$treatment == 0]
    
    # Build covariate matrix
    covs_matrix <- NULL
    cov_list <- list()
    if (include_slope && "slope" %in% names(current_data) && "elevation" %in% names(current_data)) {
      cov_list$slope <- current_data$slope
      cov_list$elevation <- current_data$elevation
    }
    if (include_ph_cov && "ph" %in% names(current_data)) {
      cov_list$ph <- current_data$ph
    }
    if (include_enslaved_cov && "enslaved_per_white" %in% names(current_data)) {
      cov_list$enslaved_per_white <- current_data$enslaved_per_white
    }
    
    if (length(cov_list) > 0) {
      valid_covs <- list()
      for (cov_name in names(cov_list)) {
        if (!all(is.na(cov_list[[cov_name]]))) {
          valid_covs[[cov_name]] <- cov_list[[cov_name]]
        }
      }
      if (length(valid_covs) > 0) {
        covs_matrix <- as.matrix(bind_cols(valid_covs))
        row.names(covs_matrix) <- current_data$original_row_id
      }
    }
    
    # Filter data for RDD
    rdd_data <- current_data %>%
      filter(!is.na(.data[[outcome_var]]) & is.finite(.data[[outcome_var]]),
             !is.na(scaled_dist) & is.finite(scaled_dist))
    
    if (nrow(rdd_data) < 10) {
      warning(paste("Too few observations for point", point_geom$point_id))
      return(result_row)
    }
    
    n_treat <- sum(rdd_data$scaled_dist >= 0, na.rm = TRUE)
    n_control <- sum(rdd_data$scaled_dist < 0, na.rm = TRUE)
    
    if (n_treat < 5 || n_control < 5) {
      warning(paste("Insufficient observations for RDD at point", point_geom$point_id))
      return(result_row)
    }
    
    # Subset covariates matrix
    covs_filtered <- NULL
    if (!is.null(covs_matrix) && nrow(rdd_data) > 0) {
      ids_in_rdd <- rdd_data$original_row_id
      covs_filtered <- covs_matrix[as.character(ids_in_rdd), , drop = FALSE]
      
      if (nrow(covs_filtered) > 0) {
        all_na_cols <- apply(covs_filtered, 2, function(col) all(is.na(col)))
        covs_filtered <- covs_filtered[, !all_na_cols, drop = FALSE]
        if (ncol(covs_filtered) == 0) covs_filtered <- NULL
      } else {
        covs_filtered <- NULL
      }
    }
    
    # Run RDD
    rdd_result <- rdrobust(y = rdd_data[[outcome_var]], 
                           x = rdd_data$scaled_dist, 
                           c = 0, covs = covs_filtered)
    
    result_row$coef <- rdd_result$coef[1]
    result_row$se <- rdd_result$se[1]
    result_row$p_value <- rdd_result$pv[1]
    result_row$n_treat <- rdd_result$N_h[2]
    result_row$n_control <- rdd_result$N_h[1]
    result_row$bw_left <- ifelse(!is.null(rdd_result$bws), rdd_result$bws[1,1], NA)
    result_row$bw_right <- ifelse(!is.null(rdd_result$bws), rdd_result$bws[1,2], NA)
    
    # Add covariate coefficients if available
    if (!is.null(covs_filtered) && ncol(covs_filtered) > 0 && !is.null(rdd_result$beta_p_cov)) {
      cov_names_final <- colnames(covs_filtered)
      if (length(rdd_result$beta_p_cov) == length(cov_names_final)) {
        for (k in 1:length(cov_names_final)) {
          result_row[[paste0(cov_names_final[k], "_coef")]] <- rdd_result$beta_p_cov[k]
        }
      }
    }
    
    return(result_row)
    
  }, error = function(e) {
    warning(paste("Error in RDD for point", point_geom$point_id, ":", conditionMessage(e)))
    return(result_row)
  })
}

perform_rdd_robustness_analysis <- function(data_sf, outcome_var_for_rdd, 
                                            include_slope, exclude_border, include_enslaved_cov, 
                                            include_ph_cov, 
                                            bw_method, kernel_type) {
  
  result_row <- data.frame(coef = NA, se = NA, p_value = NA, n_treat = NA, n_control = NA,
                           bw_left = NA, bw_right = NA)
  
  current_data <- data_sf 
  current_data$original_row_id_for_cov_matching <- 1:nrow(current_data)
  
  tryCatch({
    if (exclude_border) {
      # Exclude border counties from both sides (same as 50-point analysis)
      current_data <- current_data %>% filter(border_county != 1)
    }
    
    current_data$scaled_dist <- current_data$border_dist / 1609.344 
    current_data$scaled_dist[current_data$treatment == 0] <- -current_data$scaled_dist[current_data$treatment == 0]
    
    # Covariate matrix construction
    covs_matrix_full_robust <- NULL
    cov_list_full_robust <- list()
    if (include_slope && "slope" %in% names(current_data) && "elevation" %in% names(current_data)) {
      cov_list_full_robust$slope <- current_data$slope
      cov_list_full_robust$elevation <- current_data$elevation
    }
    if (include_ph_cov && "ph" %in% names(current_data)) {
      cov_list_full_robust$ph <- current_data$ph
    }
    if (include_enslaved_cov && "enslaved_per_white" %in% names(current_data)) {
      cov_list_full_robust$enslaved_per_white <- current_data$enslaved_per_white
    }
    
    if (length(cov_list_full_robust) > 0) {
      valid_covs_full_robust <- list()
      for(cov_name_full_robust in names(cov_list_full_robust)){
        if(!all(is.na(cov_list_full_robust[[cov_name_full_robust]]))) {
          valid_covs_full_robust[[cov_name_full_robust]] <- cov_list_full_robust[[cov_name_full_robust]]
        }
      }
      if(length(valid_covs_full_robust) > 0) {
        covs_matrix_full_robust <- as.matrix(bind_cols(valid_covs_full_robust))
        row.names(covs_matrix_full_robust) <- current_data$original_row_id_for_cov_matching
      } else {
        covs_matrix_full_robust <- NULL
      }
    }
    
    rdd_input_data_robust <- current_data %>% 
      filter(!is.na(.data[[outcome_var_for_rdd]]) & is.finite(.data[[outcome_var_for_rdd]]),
             !is.na(scaled_dist) & is.finite(scaled_dist))
    
    if (nrow(rdd_input_data_robust) < 10) {
      warning("Too few overall observations after NA removal for robustness check")
      return(result_row)
    }
    
    n_treat_filt_robust <- sum(rdd_input_data_robust$scaled_dist >= 0, na.rm = TRUE)
    n_control_filt_robust <- sum(rdd_input_data_robust$scaled_dist < 0, na.rm = TRUE)
    
    if (n_treat_filt_robust < 5 || n_control_filt_robust < 5) {
      warning("Not enough observations after NA removal for RDD robustness check.")
      return(result_row)
    }
    
    covs_matrix_filtered_robust <- NULL
    if (!is.null(covs_matrix_full_robust) && nrow(rdd_input_data_robust) > 0) {
      ids_in_rdd_input_robust <- rdd_input_data_robust$original_row_id_for_cov_matching
      covs_matrix_filtered_robust <- covs_matrix_full_robust[as.character(ids_in_rdd_input_robust), , drop = FALSE]
      
      if(nrow(covs_matrix_filtered_robust) > 0){
        all_na_cols_subset_robust <- apply(covs_matrix_filtered_robust, 2, function(col) all(is.na(col)))
        covs_matrix_filtered_robust <- covs_matrix_filtered_robust[, !all_na_cols_subset_robust, drop = FALSE]
        if(ncol(covs_matrix_filtered_robust) == 0) covs_matrix_filtered_robust <- NULL
      } else {
        covs_matrix_filtered_robust <- NULL
      }
    }
    
    rdd_result <- rdrobust(y = rdd_input_data_robust[[outcome_var_for_rdd]], 
                           x = rdd_input_data_robust$scaled_dist, 
                           c = 0, covs = covs_matrix_filtered_robust,
                           bwselect = bw_method, kernel = kernel_type)
    
    result_row$coef <- rdd_result$coef[1]
    result_row$se <- rdd_result$se[1]
    result_row$p_value <- rdd_result$pv[1]
    result_row$n_treat <- rdd_result$N_h[2]
    result_row$n_control <- rdd_result$N_h[1]
    result_row$bw_left <- ifelse(!is.null(rdd_result$bws), rdd_result$bws[1,1], NA)
    result_row$bw_right <- ifelse(!is.null(rdd_result$bws), rdd_result$bws[1,2], NA)
    
    if (!is.null(covs_matrix_filtered_robust) && ncol(covs_matrix_filtered_robust) > 0 && !is.null(rdd_result$beta_p_cov)) {
      cov_names_final_robust <- colnames(covs_matrix_filtered_robust)
      if(length(rdd_result$beta_p_cov) == length(cov_names_final_robust)){
        for (k in 1:length(cov_names_final_robust)) {
          result_row[[paste0(cov_names_final_robust[k], "_coef")]] <- rdd_result$beta_p_cov[k]
        }
      }
    }
    return(result_row)
    
  }, error = function(e) {
    warning(paste("Error in RDD robustness check:", conditionMessage(e)))
    return(result_row)
  })
}

# --- PART 5: Run RDD Analysis ---

run_rdd_analysis_1860 <- function(database_path) {
  cat("\n=== Starting RDD Analysis for 1860 ===\n")
  
  # Load RDD database
  if (!file.exists(database_path)) {
    stop("RDD database not found:", database_path)
  }
  
  rdd_data <- read_csv(database_path, show_col_types = FALSE) %>%
    filter(year == 1860) %>%
    mutate(treatment = slavery_legal)
  
  # Load county geometries
  county_shp_dir <- "Data/US_county_1860/"
  shp_files <- list.files(county_shp_dir, pattern = "\\.shp$", full.names = TRUE)
  if (length(shp_files) == 0) {
    stop("No county shapefile found for 1860")
  }
  
  counties_geom <- st_read(shp_files[1], quiet = TRUE) %>%
    fix_geometries() %>%
    mutate(GISJOIN = as.character(GISJOIN)) %>%
    select(GISJOIN, geometry)
  
  # Join with RDD data
  rdd_data_sf <- counties_geom %>%
    inner_join(rdd_data, by = "GISJOIN") %>%
    st_as_sf()
  
  if (nrow(rdd_data_sf) == 0) {
    stop("No data after joining geometries with RDD database.")
  }
  
  # Load border shapefile
  default_border_path <- "Data/Border/1820_border/1820_border.shp"
  border_file <- find_border_file_rdd(default_border_path, "Data/Border", "1820.*border.*\\.shp$")
  border_sf <- load_and_fix_shapefile_rdd(border_file, "Main Border")
  
  # Generate border points
  border_points <- generate_border_points_rdd(border_sf, n_points = 50)
  
  # Calculate distances to border
  cat("Calculating distances to border...\n")
  rdd_centroids <- st_centroid(rdd_data_sf)
  target_crs <- st_crs(border_sf)
  rdd_centroids <- st_transform(rdd_centroids, target_crs)
  rdd_data_sf$border_dist <- calculate_border_distance_rdd(rdd_centroids, border_sf)
  
  # Define RDD specifications
  rdd_specs <- list(
    list(id = 1, include_slope = FALSE, exclude_border = FALSE, include_enslaved = FALSE, include_ph = FALSE, suffix = "_no_controls"),
    list(id = 2, include_slope = TRUE, exclude_border = FALSE, include_enslaved = FALSE, include_ph = TRUE, suffix = "_geo"),
    list(id = 3, include_slope = TRUE, exclude_border = FALSE, include_enslaved = TRUE, include_ph = TRUE, suffix = "_geo_enslaved"),
    list(id = 4, include_slope = TRUE, exclude_border = TRUE, include_enslaved = FALSE, include_ph = TRUE, suffix = "_geo_noborder"),
    list(id = 5, include_slope = TRUE, exclude_border = TRUE, include_enslaved = TRUE, include_ph = TRUE, suffix = "_geo_noborder_enslaved")
  )
  
  # Wealth outcomes
  wealth_outcomes <- list(
    list(name = "Mean_household", log = TRUE),
    list(name = "Median_household", log = TRUE),
    list(name = "Gini_household", log = FALSE)
  )
  
  # Occupation outcomes
  occupation_outcomes <- list(
    list(name = "Pct_heads_farmers_planters", log = FALSE),
    list(name = "Pct_heads_unskilled_laborers", log = FALSE)
  )
  
  current_year <- 1860
  
  # ===== WEALTH OUTCOMES - 50-POINT ANALYSIS =====
  cat("\n=== Running 50-Point Analysis for Wealth Outcomes ===\n")
  for (outcome_item in wealth_outcomes) {
    outcome_name <- outcome_item$name
    use_log <- outcome_item$log
    
    cat(sprintf("\n--- RDD Analysis for Wealth Outcome: %s ---\n", outcome_name))
    
    if (!outcome_name %in% names(rdd_data_sf)) {
      warning(paste("Outcome variable", outcome_name, "not found. Skipping."))
      next
    }
    
    analysis_data <- rdd_data_sf
    outcome_var <- "Y_rdd"
    
    if (use_log) {
      analysis_data[[outcome_var]] <- ifelse(analysis_data[[outcome_name]] > 0, 
                                             log(analysis_data[[outcome_name]]), NA_real_)
    } else {
      analysis_data[[outcome_var]] <- analysis_data[[outcome_name]]
    }
    
    analysis_data <- analysis_data %>%
      filter(!is.na(.data[[outcome_var]]) & is.finite(.data[[outcome_var]]))
    
    if (nrow(analysis_data) == 0) {
      warning(paste("No valid data for", outcome_name, "after filtering. Skipping."))
      next
    }
    
    # Run 50-point RDD for each specification
    for (spec in rdd_specs) {
      cat(sprintf("Running 50-point RDD for %s%s...\n", outcome_name, spec$suffix))
      
      pb <- progress_bar$new(
        format = paste("RDD:", outcome_name, spec$suffix, "[:bar] :percent eta: :eta"),
        total = nrow(border_points), clear = FALSE, width = 60
      )
      
      results_list <- purrr::map(1:nrow(border_points), function(i) {
        res <- perform_rdd_analysis(
          data_sf = analysis_data,
          outcome_var = outcome_var,
          point_geom = border_points[i,],
          include_slope = spec$include_slope,
          exclude_border = spec$exclude_border,
          include_enslaved_cov = spec$include_enslaved,
          include_ph_cov = spec$include_ph
        )
        pb$tick()
        return(res)
      })
      
      results_df <- bind_rows(results_list)
      results_sf <- border_points %>% left_join(results_df, by = "point_id")
      
      # Save results
      csv_filename <- paste0(outcome_name, spec$suffix, ".csv")
      gpkg_filename <- paste0(outcome_name, spec$suffix, ".gpkg")
      
      write_csv(results_df, file.path("Results/RDD_wealth/CSV", csv_filename))
      st_write(results_sf, file.path("Results/RDD_wealth/Geopackage", gpkg_filename), 
               delete_layer = TRUE, quiet = TRUE)
      
      cat("Saved 50-point RDD results for", outcome_name, spec$suffix, "\n")
    }
  }
  
  # ===== WEALTH OUTCOMES - ROBUSTNESS CHECKS =====
  cat("\n=== Running Robustness Checks for Wealth Outcomes ===\n")
  for (outcome_item in wealth_outcomes) {
    outcome_name <- outcome_item$name
    use_log <- outcome_item$log
    
    cat(sprintf("Running comprehensive RDD robustness checks for %s...\n", outcome_name))
    
    if (!outcome_name %in% names(rdd_data_sf)) {
      warning(paste("Outcome variable", outcome_name, "not found for robustness. Skipping."))
      next
    }
    
    analysis_data <- rdd_data_sf
    outcome_var <- "Y_rdd"
    
    if (use_log) {
      analysis_data[[outcome_var]] <- ifelse(analysis_data[[outcome_name]] > 0, 
                                             log(analysis_data[[outcome_name]]), NA_real_)
    } else {
      analysis_data[[outcome_var]] <- analysis_data[[outcome_name]]
    }
    
    analysis_data <- analysis_data %>%
      filter(!is.na(.data[[outcome_var]]) & is.finite(.data[[outcome_var]]))
    
    if (nrow(analysis_data) == 0) {
      warning(paste("No valid data for robustness check of", outcome_name, ". Skipping."))
      next
    }
    
    bw_methods_list <- c("mserd", "msetwo", "msesum", "cerrd", "certwo", "cersum")
    kernels_list <- c("triangular", "uniform", "epanechnikov")
    
    robustness_grid <- expand.grid(
      spec_id = sapply(rdd_specs, `[[`, "id"),
      bw_method = bw_methods_list,
      kernel = kernels_list,
      stringsAsFactors = FALSE
    )
    
    pb_robust <- progress_bar$new(
      format = paste("RDD Robustness:", outcome_name, "[:bar] :percent eta: :eta"),
      total = nrow(robustness_grid), clear = FALSE, width = 60
    )
    
    robustness_results_list <- vector("list", nrow(robustness_grid))
    for (i in 1:nrow(robustness_grid)) {
      current_spec_params <- robustness_grid[i,]
      spec_item_details <- rdd_specs[[which(sapply(rdd_specs, `[[`, "id") == current_spec_params$spec_id)]]
      
      res_robust <- perform_rdd_robustness_analysis(
        data_sf = analysis_data,
        outcome_var_for_rdd = outcome_var,
        include_slope = spec_item_details$include_slope,
        exclude_border = spec_item_details$exclude_border,
        include_enslaved_cov = spec_item_details$include_enslaved,
        include_ph_cov = spec_item_details$include_ph,
        bw_method = current_spec_params$bw_method,
        kernel_type = current_spec_params$kernel
      )
      res_robust$spec_suffix <- spec_item_details$suffix
      res_robust$bw_method <- current_spec_params$bw_method
      res_robust$kernel <- current_spec_params$kernel
      robustness_results_list[[i]] <- res_robust
      pb_robust$tick()
    }
    robustness_results_df <- bind_rows(robustness_results_list)
    
    # Save robustness results
    csv_robust_fname <- paste0(outcome_name, "_robustness.csv")
    write_csv(robustness_results_df, file.path("Results/RDD_wealth/CSV", csv_robust_fname))
    cat("Saved comprehensive RDD robustness check results for", outcome_name, "\n")
  }
  
  # ===== OCCUPATION OUTCOMES - 50-POINT ANALYSIS =====
  cat("\n=== Running 50-Point Analysis for Occupation Outcomes ===\n")
  for (outcome_item in occupation_outcomes) {
    outcome_name <- outcome_item$name
    use_log <- outcome_item$log
    
    cat(sprintf("\n--- RDD Analysis for Occupation Outcome: %s ---\n", outcome_name))
    
    if (!outcome_name %in% names(rdd_data_sf)) {
      warning(paste("Outcome variable", outcome_name, "not found. Skipping."))
      next
    }
    
    analysis_data <- rdd_data_sf
    outcome_var <- "Y_rdd"
    
    if (use_log) {
      analysis_data[[outcome_var]] <- ifelse(analysis_data[[outcome_name]] > 0, 
                                             log(analysis_data[[outcome_name]]), NA_real_)
    } else {
      analysis_data[[outcome_var]] <- analysis_data[[outcome_name]]
    }
    
    analysis_data <- analysis_data %>%
      filter(!is.na(.data[[outcome_var]]) & is.finite(.data[[outcome_var]]))
    
    if (nrow(analysis_data) == 0) {
      warning(paste("No valid data for", outcome_name, "after filtering. Skipping."))
      next
    }
    
    # Run 50-point RDD for each specification
    for (spec in rdd_specs) {
      cat(sprintf("Running 50-point RDD for %s%s...\n", outcome_name, spec$suffix))
      
      pb <- progress_bar$new(
        format = paste("RDD:", outcome_name, spec$suffix, "[:bar] :percent eta: :eta"),
        total = nrow(border_points), clear = FALSE, width = 60
      )
      
      results_list <- purrr::map(1:nrow(border_points), function(i) {
        res <- perform_rdd_analysis(
          data_sf = analysis_data,
          outcome_var = outcome_var,
          point_geom = border_points[i,],
          include_slope = spec$include_slope,
          exclude_border = spec$exclude_border,
          include_enslaved_cov = spec$include_enslaved,
          include_ph_cov = spec$include_ph
        )
        pb$tick()
        return(res)
      })
      
      results_df <- bind_rows(results_list)
      results_sf <- border_points %>% left_join(results_df, by = "point_id")
      
      # Save results
      csv_filename <- paste0(outcome_name, spec$suffix, ".csv")
      gpkg_filename <- paste0(outcome_name, spec$suffix, ".gpkg")
      
      write_csv(results_df, file.path("Results/RDD_occupations/CSV", csv_filename))
      st_write(results_sf, file.path("Results/RDD_occupations/Geopackage", gpkg_filename), 
               delete_layer = TRUE, quiet = TRUE)
      
      cat("Saved 50-point RDD results for", outcome_name, spec$suffix, "\n")
    }
  }
  
  # ===== OCCUPATION OUTCOMES - ROBUSTNESS CHECKS =====
  cat("\n=== Running Robustness Checks for Occupation Outcomes ===\n")
  for (outcome_item in occupation_outcomes) {
    outcome_name <- outcome_item$name
    use_log <- outcome_item$log
    
    cat(sprintf("Running comprehensive RDD robustness checks for %s...\n", outcome_name))
    
    if (!outcome_name %in% names(rdd_data_sf)) {
      warning(paste("Outcome variable", outcome_name, "not found for robustness. Skipping."))
      next
    }
    
    analysis_data <- rdd_data_sf
    outcome_var <- "Y_rdd"
    
    if (use_log) {
      analysis_data[[outcome_var]] <- ifelse(analysis_data[[outcome_name]] > 0, 
                                             log(analysis_data[[outcome_name]]), NA_real_)
    } else {
      analysis_data[[outcome_var]] <- analysis_data[[outcome_name]]
    }
    
    analysis_data <- analysis_data %>%
      filter(!is.na(.data[[outcome_var]]) & is.finite(.data[[outcome_var]]))
    
    if (nrow(analysis_data) == 0) {
      warning(paste("No valid data for robustness check of", outcome_name, ". Skipping."))
      next
    }
    
    bw_methods_list <- c("mserd", "msetwo", "msesum", "cerrd", "certwo", "cersum")
    kernels_list <- c("triangular", "uniform", "epanechnikov")
    
    robustness_grid <- expand.grid(
      spec_id = sapply(rdd_specs, `[[`, "id"),
      bw_method = bw_methods_list,
      kernel = kernels_list,
      stringsAsFactors = FALSE
    )
    
    pb_robust <- progress_bar$new(
      format = paste("RDD Robustness:", outcome_name, "[:bar] :percent eta: :eta"),
      total = nrow(robustness_grid), clear = FALSE, width = 60
    )
    
    robustness_results_list <- vector("list", nrow(robustness_grid))
    for (i in 1:nrow(robustness_grid)) {
      current_spec_params <- robustness_grid[i,]
      spec_item_details <- rdd_specs[[which(sapply(rdd_specs, `[[`, "id") == current_spec_params$spec_id)]]
      
      res_robust <- perform_rdd_robustness_analysis(
        data_sf = analysis_data,
        outcome_var_for_rdd = outcome_var,
        include_slope = spec_item_details$include_slope,
        exclude_border = spec_item_details$exclude_border,
        include_enslaved_cov = spec_item_details$include_enslaved,
        include_ph_cov = spec_item_details$include_ph,
        bw_method = current_spec_params$bw_method,
        kernel_type = current_spec_params$kernel
      )
      res_robust$spec_suffix <- spec_item_details$suffix
      res_robust$bw_method <- current_spec_params$bw_method
      res_robust$kernel <- current_spec_params$kernel
      robustness_results_list[[i]] <- res_robust
      pb_robust$tick()
    }
    robustness_results_df <- bind_rows(robustness_results_list)
    
    # Save robustness results
    csv_robust_fname <- paste0(current_year, "_", outcome_name, "_robustness.csv")
    write_csv(robustness_results_df, file.path("Results/RDD_occupations/CSV", csv_robust_fname))
    cat("Saved comprehensive RDD robustness check results for", outcome_name, "\n")
  }
  
  cat("\n=== RDD Analysis Completed ===\n")
}

# --- MAIN PIPELINE ---

main_pipeline <- function() {
  cat("====== STARTING COMBINED SPATIAL RDD PIPELINE FOR 1860 ======\n")
  
  # Step 1: Download IPUMS data if needed
  census_file <- "Data/census.csv"
  if (!file.exists(census_file)) {
    cat("Data/census.csv not found. Running IPUMS download...\n")
    cat("This requires an IPUMS API key.\n")
    download_ipums_data_1860()
  } else {
    cat("Found existing Data/census.csv. Skipping IPUMS download.\n")
  }
  
  # Step 2: Create RDD database if needed
  database_file <- "Data/RDD_Database/rdd_database_1860.csv"
  if (!file.exists(database_file)) {
    cat("RDD database not found. Creating database...\n")
    database_file <- create_rdd_database_1860()
  } else {
    cat("Found existing RDD database. Skipping database creation.\n")
  }
  
  # Step 3: Run RDD analysis
  cat("Starting RDD analysis...\n")
  run_rdd_analysis_1860(database_file)
  
  cat("\n====== COMBINED SPATIAL RDD PIPELINE COMPLETED SUCCESSFULLY! ======\n")
}

# Execute the pipeline
main_pipeline()