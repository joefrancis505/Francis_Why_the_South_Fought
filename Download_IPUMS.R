# Set a CRAN mirror
options(repos = c(CRAN = "https://cloud.r-project.org"))

# Install and load required packages
packages <- c("dplyr", "readr", "ipumsr")
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages])
}
invisible(lapply(packages, library, character.only = TRUE))

# --- Helper function to get most recent file ---
get_most_recent_file <- function(file_list) {
  if (length(file_list) == 0) {
    return(character(0))
  }
  
  if (length(file_list) == 1) {
    return(file_list[1])
  }
  
  # Get file info and sort by modification time (most recent first)
  file_info_df <- file.info(file_list)
  file_info_df$filepath <- file_list
  sorted_files <- file_info_df[order(file_info_df$mtime, decreasing = TRUE), ]
  
  return(sorted_files$filepath[1])
}

# --- API Key Function ---
get_api_key <- function() {
  api_key_env <- Sys.getenv("IPUMS_API_KEY")
  
  if (nzchar(api_key_env)) {
    cat("Using IPUMS API key from environment variable.\n")
    return(api_key_env)
  } 
  
  api_key <- readline(prompt = "Please enter your IPUMS API key: ")
  
  if (nzchar(api_key)) {
    Sys.setenv(IPUMS_API_KEY = api_key)
    cat("IPUMS API key set for this session.\n")
  } else {
    stop("API key is required to download IPUMS data.")
  }
  
  return(api_key)
}

# --- Create directories ---
for (dir in c("Data", "Downloads")) {
  if (!dir.exists(dir)) {
    dir.create(dir, recursive = TRUE)
    cat("Created directory:", dir, "\n")
  }
}

# Ensure shapefile directory exists
if (!dir.exists("Data/US_county_1860")) {
  dir.create("Data/US_county_1860", recursive = TRUE)
  cat("Created directory: Data/US_county_1860\n")
}

# --- Download IPUMS USA Data ---
cat("\n=== Downloading IPUMS USA Data ===\n")

# Get API key
api_key <- get_api_key()

# Set the default collection to USA
set_ipums_default_collection("usa")

# Define USA extract for 1860 Census data with specified variables
usa_extract <- define_extract_micro(
  collection = "usa",
  description = "1860 Census 100% Sample with Selected Variables",
  samples = "us1860c",
  variables = c("YEAR", "GQTYPE", "GQ", "SERIAL", "COUNTYNHG", "RELATE", "HHWT", "PERWT",
                "OCC1950", "AGE", "SEX", "REALPROP", "PERSPROP", "RACE", "BPL"),
  data_format = "fixed_width"
)

# Submit and wait for extract
cat("Submitting USA extract request...\n")
usa_submitted <- submit_extract(usa_extract, api_key = api_key)

cat("Waiting for USA extract to complete...\n")
usa_ready <- wait_for_extract(usa_submitted, api_key = api_key)

# Download extract to Downloads directory
cat("Downloading USA extract files...\n")
usa_files <- download_extract(
  usa_ready, 
  download_dir = "Downloads", 
  api_key = api_key, 
  overwrite = FALSE
)

# --- Process USA data ---
cat("\n=== Processing USA data ===\n")
usa_data_files <- list.files("Downloads", pattern = "usa_.*\\.(xml|ddi)$", full.names = TRUE)

if (length(usa_data_files) > 0) {
  # Get the most recent USA data file
  most_recent_usa_file <- get_most_recent_file(usa_data_files)
  cat("Found", length(usa_data_files), "USA data file(s).\n")
  cat("Using most recent USA data file:", basename(most_recent_usa_file), "\n")
  
  # Read the USA data
  usa_ddi <- read_ipums_ddi(most_recent_usa_file)
  usa_data <- read_ipums_micro(usa_ddi, verbose = FALSE)
  
  # Write to IPUMS.csv
  write_csv(usa_data, "Data/IPUMS.csv")
  cat("Successfully created Data/IPUMS.csv\n")
} else {
  cat("Could not find USA data files.\n")
}

# --- Download Shapefile ---
cat("\n=== Downloading NHGIS County Shapefile ===\n")

# Set the default IPUMS collection for NHGIS
set_ipums_default_collection("nhgis")

# Define NHGIS extract for 1860 County Tiger 2000 Shapefile
shapefile_extract <- define_extract_nhgis(
  description = "1860 County Tiger 2000 Shapefile",
  shapefiles = "us_county_1860_tl2000"
)

# Submit and wait for extract
cat("Submitting shapefile extract request...\n")
shapefile_submitted <- submit_extract(shapefile_extract, api_key = api_key)

cat("Waiting for shapefile extract to complete...\n")
shapefile_ready <- wait_for_extract(shapefile_submitted, api_key = api_key)

# Download extract to Downloads directory
cat("Downloading shapefile extract...\n")
shapefile_files <- download_extract(
  shapefile_ready, 
  download_dir = "Downloads", 
  api_key = api_key, 
  overwrite = FALSE
)

# --- Process downloaded shapefile (handling nested zips) ---
cat("\n=== Processing Shapefile ===\n")

# Find the downloaded shapefile ZIP (get most recent if multiple exist)
shapefile_zips <- list.files("Downloads", pattern = "nhgis.*\\.zip$", full.names = TRUE, recursive = FALSE)

if (length(shapefile_zips) > 0) {
  # Get the most recent shapefile ZIP
  most_recent_shapefile_zip <- get_most_recent_file(shapefile_zips)
  cat("Found", length(shapefile_zips), "shapefile zip(s).\n")
  cat("Using most recent shapefile zip:", basename(most_recent_shapefile_zip), "\n")
  
  # Create a temporary directory for extraction
  temp_dir <- file.path("Downloads", "temp_extract")
  if (!dir.exists(temp_dir)) {
    dir.create(temp_dir, recursive = TRUE)
  }
  
  # Step 1: Extract the outer ZIP
  cat("Extracting outer ZIP file...\n")
  utils::unzip(most_recent_shapefile_zip, exdir = temp_dir)
  
  # Step 2: Find the inner ZIP file (get most recent if multiple exist)
  inner_zips <- list.files(temp_dir, pattern = "\\.zip$", full.names = TRUE, recursive = TRUE)
  
  if (length(inner_zips) > 0) {
    # Get the most recent inner ZIP file
    most_recent_inner_zip <- get_most_recent_file(inner_zips)
    cat("Found", length(inner_zips), "inner ZIP file(s).\n")
    cat("Using most recent inner ZIP file:", basename(most_recent_inner_zip), "\n")
    
    # Step 3: Extract the inner ZIP
    inner_extract_dir <- file.path(temp_dir, "inner_extract")
    if (!dir.exists(inner_extract_dir)) {
      dir.create(inner_extract_dir, recursive = TRUE)
    }
    
    cat("Extracting inner ZIP file...\n")
    utils::unzip(most_recent_inner_zip, exdir = inner_extract_dir)
    
    # Step 4: Find the shapefile (get most recent if multiple exist)
    shp_files <- list.files(inner_extract_dir, pattern = "\\.shp$", full.names = TRUE, recursive = TRUE)
    
    if (length(shp_files) > 0) {
      # Get the most recent shapefile
      most_recent_shp_file <- get_most_recent_file(shp_files)
      cat("Found", length(shp_files), "shapefile(s).\n")
      cat("Using most recent shapefile:", basename(most_recent_shp_file), "\n")
      
      # Get the directory containing the shapefile
      shp_dir <- dirname(most_recent_shp_file)
      
      # Step 5: Copy all files from the shapefile directory to the target directory
      shapefile_files <- list.files(shp_dir, full.names = TRUE)
      
      for (file in shapefile_files) {
        file.copy(file, file.path("Data/US_county_1860", basename(file)), overwrite = TRUE)
      }
      
      cat("Copied", length(shapefile_files), "files to Data/US_county_1860/\n")
      
      # Check that essential files are copied
      target_files <- list.files("Data/US_county_1860")
      has_shp <- any(grepl("\\.shp$", target_files))
      has_shx <- any(grepl("\\.shx$", target_files))
      has_dbf <- any(grepl("\\.dbf$", target_files))
      
      if (has_shp && has_shx && has_dbf) {
        cat("Successfully copied all essential shapefile components\n")
      } else {
        cat("WARNING: Some essential shapefile components may be missing\n")
      }
    } else {
      cat("No shapefile found in the extracted inner ZIP\n")
    }
  } else {
    cat("No inner ZIP file found in the extracted files\n")
  }
} else {
  cat("Could not find shapefile ZIP in the Downloads directory\n")
}

# --- Clean up temporary files ---
temp_dir <- file.path("Downloads", "temp_extract")
if (dir.exists(temp_dir)) {
  unlink(temp_dir, recursive = TRUE)
  cat("Cleaned up temporary extraction directory\n")
}

cat("\n=== Data Processing Complete ===\n")
cat("The following files have been created:\n")
cat("1. Data/IPUMS.csv - Contains the microdata with the specified variables\n")

# Verify if shapefile extraction was successful
if (length(list.files("Data/US_county_1860")) > 0) {
  cat("2. Data/US_county_1860/ - Contains the 1860 Tiger 2000 shapefiles\n")
} else {
  cat("WARNING: Data/US_county_1860/ directory is empty. Shapefile extraction failed.\n")
}

cat("\nDownloaded files were preserved in the Downloads directory for reference.\n")