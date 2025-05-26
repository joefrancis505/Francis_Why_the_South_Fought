# Download_census.R

# Download IPUMS Data for Wealth Inequality RDD Analysis
# This script downloads only the necessary 1860 census data tables for the wealth RDD

# Set the working directory to the script's location
setwd(getSrcDirectory(function(dummy) {dummy}))

# Set a CRAN mirror
options(repos = c(CRAN = "https://cloud.r-project.org"))

# List of packages to install
packages <- c("dplyr", "readr", "ipumsr")

# Install packages not yet installed
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages])
}

# Load the packages
invisible(lapply(packages, library, character.only = TRUE))

# --- IPUMS API Setup ---

# Function to get API key from user or environment
get_api_key <- function() {
  api_key_env <- Sys.getenv("IPUMS_API_KEY")
  
  if (nzchar(api_key_env)) {
    cat("Using IPUMS API key found in environment variable IPUMS_API_KEY.\n")
    api_key <- api_key_env
  } else {
    ipums_url <- "https://account.ipums.org/api_keys"
    cat("This script requires an IPUMS API key, which can be obtained at", ipums_url, "\n")
    api_key <- readline(prompt = "Please enter your API key: ")
    
    # Set the key in the current session environment
    if (nzchar(api_key)) {
      Sys.setenv(IPUMS_API_KEY = api_key)
      cat("IPUMS API key has been set for this session.\n")
    }
  }
  
  if (!nzchar(api_key)) {
    stop("API key is required to download NHGIS data. Exiting.")
  }
  cat("IPUMS API key accepted for this session.\n")
  
  return(api_key)
}

# Set the default IPUMS collection
set_ipums_default_collection("nhgis")

# Get API key
api_key <- get_api_key()

# Create necessary directories
# Note: Shapefile download removed, so Data/Shapefiles/1860 creation is removed.
# It's assumed Data/US_county_1860/US_county_1860.shp is already available.
required_dirs <- c(
  "Data/IPUMS_Wealth_RDD" 
)

# Create each directory if it doesn't exist
for (dir_path in required_dirs) {
  if (!dir.exists(dir_path)) {
    dir.create(dir_path, recursive = TRUE)
    cat("Created directory:", dir_path, "\n")
  }
}

# Function to download 1860 population data tables
download_1860_population_data <- function(api_key) {
  cat("\n=== Downloading 1860 Population Data Tables for Wealth RDD ===\n")
  
  # Define download directory
  download_dir <- "Data/IPUMS_Wealth_RDD"
  
  # We only need the population dataset for 1860
  # Specifically tables NT1 and NT6 which contain:
  # - NT1: Total population (AG3001)
  # - NT6: Race by status (AH3002, AH3003, AH3005 for black and enslaved populations)
  pop_specs <- ds_spec(
    "1860_cPAX", 
    data_tables = c("NT1", "NT6"),
    geog_levels = "county"
  )
  
  # Define extract (shapefile download removed)
  extract_def <- define_extract_nhgis(
    description = "1860 population data tables for wealth inequality RDD",
    datasets = list(pop_specs)
    # shapefiles = NULL # Explicitly no shapefiles from this extract
  )
  
  # Submit and wait for extract
  cat("Submitting extract request...\n")
  submitted_extract <- submit_extract(extract_def, api_key = api_key)
  
  cat("Waiting for extract to complete...\n")
  ready_extract <- wait_for_extract(submitted_extract, api_key = api_key)
  
  # Download extract
  cat("Downloading extract files...\n")
  downloaded_files <- download_extract(
    ready_extract, 
    download_dir = download_dir, 
    api_key = api_key, 
    overwrite = FALSE
  )
  
  # Extract the downloaded zip file (which now only contains CSV data)
  for (zip_file in downloaded_files) {
    if (file.exists(zip_file) && grepl("\\.zip$", zip_file)) {
      cat("Extracting:", basename(zip_file), "\n")
      # The main zip file from IPUMS for data tables is typically named something like nhgisXXXX_csv.zip
      # It will extract into a folder like nhgisXXXX_csv within download_dir
      utils::unzip(zip_file, exdir = download_dir) 
    }
  }
  
  # There should be no nested zips if we only download tables and no shapefiles.
  # If IPUMS structure changes, this might need review.
  # The following loop for nested zips might be redundant for table-only downloads.
  nested_zips <- list.files(download_dir, pattern = "\\.zip$", full.names = TRUE, recursive = TRUE)
  for (zip_file in nested_zips) {
    # This condition might need adjustment based on actual IPUMS output for table-only extracts
    if (!grepl("nhgis\\d+_csv\\.zip$", basename(zip_file))) { # Skip the main download zip if already processed
      extract_dir <- file.path(download_dir, tools::file_path_sans_ext(basename(zip_file)))
      if (!dir.exists(extract_dir)) {
        cat("Extracting nested zip (if any):", basename(zip_file), "\n")
        dir.create(extract_dir, recursive = TRUE)
        utils::unzip(zip_file, exdir = extract_dir)
      }
    }
  }
  
  cat("Data tables downloaded and extracted.\n")
  return(download_dir)
}

# Function to process and save census data
process_census_data <- function(download_dir) {
  cat("\n=== Processing Census Data ===\n")
  
  # Find CSV files within the downloaded and extracted structure
  # IPUMS typically puts CSVs in a subdirectory named after the extract, e.g., "nhgis0001_csv"
  csv_subdir <- list.files(download_dir, pattern = "nhgis\\d+_csv$", full.names = TRUE, include.dirs = TRUE)
  
  csv_files <- c()
  if (length(csv_subdir) > 0) {
    # Prefer files from the specific extract subdirectory if it exists
    csv_files <- list.files(csv_subdir[1], pattern = "\\.csv$", full.names = TRUE, recursive = TRUE)
  } else {
    # Fallback to searching the main download_dir (less common for IPUMS table structure)
    csv_files <- list.files(download_dir, pattern = "\\.csv$", full.names = TRUE, recursive = TRUE)
  }
  
  csv_files <- csv_files[!grepl("codebook", csv_files, ignore.case = TRUE)]
  
  if (length(csv_files) == 0) {
    stop("No CSV data files found in downloaded data at ", download_dir, 
         ". Check IPUMS extract structure or download success.")
  }
  
  all_data <- NULL
  for (csv_file in csv_files) {
    cat("Reading:", basename(csv_file), "\n")
    data <- read_csv(csv_file, show_col_types = FALSE)
    
    if (is.null(all_data)) {
      all_data <- data
    } else {
      # Join by GISJOIN, keeping only numeric columns from additional tables
      # Ensure GISJOIN is character for robust joining
      if("GISJOIN" %in% names(data)) data$GISJOIN <- as.character(data$GISJOIN)
      if("GISJOIN" %in% names(all_data)) all_data$GISJOIN <- as.character(all_data$GISJOIN)
      
      data_to_join <- data %>% 
        select(any_of(c("GISJOIN")), where(is.numeric)) # Use any_of for GISJOIN robustness
      
      # If GISJOIN is not in data_to_join (e.g., a table without it), skip join for this file
      if ("GISJOIN" %in% names(data_to_join)) {
        all_data <- all_data %>%
          left_join(data_to_join, by = "GISJOIN")
      } else {
        cat("Skipping join for", basename(csv_file), "as it does not contain GISJOIN.\n")
      }
    }
  }
  
  if (is.null(all_data) || !"GISJOIN" %in% names(all_data)) {
    stop("Failed to load or combine data, or GISJOIN is missing.")
  }
  
  # Process the data to create the variables we need
  # AG3001: Total population (from NT1)
  # AH3002, AH3003, AH3005: Race/status components (from NT6)
  
  # Ensure required columns exist, fill with NA if not (e.g., if a table failed to load)
  required_cols <- c("AG3001", "AH3002", "AH3003", "AH3005")
  for (col_name in required_cols) {
    if (!col_name %in% names(all_data)) {
      cat("Warning: Column", col_name, "not found in loaded data. Will be NA.\n")
      all_data[[col_name]] <- NA_real_
    }
  }
  
  processed_data <- all_data %>%
    transmute(
      GISJOIN,
      year = 1860,
      census_pop = AG3001,
      black = rowSums(select(., all_of(c("AH3002", "AH3003", "AH3005"))), na.rm = TRUE), # Summing, assumes NA means 0 for sum
      enslaved = AH3003,
      pc_black = ifelse(census_pop > 0 & !is.na(black) & !is.na(census_pop), black / census_pop * 100, NA_real_),
      pc_enslaved = ifelse(census_pop > 0 & !is.na(enslaved) & !is.na(census_pop), enslaved / census_pop * 100, NA_real_)
    )
  
  # Save processed data
  output_file <- "Data/census_1860_wealth_rdd.csv"
  write_csv(processed_data, output_file)
  
  cat("\nProcessed census data saved to:", output_file, "\n")
  cat("Total counties:", nrow(processed_data), "\n")
  cat("Variables created: census_pop, black, enslaved, pc_black, pc_enslaved\n")
  
  # Print summary statistics
  cat("\nSummary statistics:\n")
  cat("Average population per county:", round(mean(processed_data$census_pop, na.rm = TRUE)), "\n")
  cat("Average % black:", round(mean(processed_data$pc_black, na.rm = TRUE), 2), "%\n")
  cat("Average % enslaved:", round(mean(processed_data$pc_enslaved, na.rm = TRUE), 2), "%\n")
  
  return(output_file)
}

# Main function
main <- function() {
  cat("=== IPUMS Data Downloader for Wealth Inequality RDD ===\n")
  cat("This script downloads only the 1860 population data tables needed for the analysis.\n")
  cat("It assumes the county shapefile (Data/US_county_1860/US_county_1860.shp) is already available.\n")
  
  # Download data tables
  download_dir <- download_1860_population_data(api_key)
  
  # Shapefile organization is removed as shapefiles are not downloaded by this script.
  
  # Process and save census data
  census_file <- process_census_data(download_dir)
  
  cat("\n=== Download Complete ===\n")
  cat("The following file has been created:\n")
  cat("1. Census data in", census_file, "\n")
  cat("\nEnsure the 1860 county shapefile is available at 'Data/US_county_1860/US_county_1860.shp'.\n")
  cat("You can now run Spatial_RDD_database.R to prepare the RDD database.\n")
}

# Run main function
main()