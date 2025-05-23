# Set the working directory to the script's location
setwd(tryCatch(getSrcDirectory(function(dummy) {dummy}), error = function(e) "."))

# Set a CRAN mirror
options(repos = c(CRAN = "https://cloud.r-project.org"))

# Install and load required packages
packages <- c("data.table", "knitr", "sf")
installed_packages <- packages %in% rownames(installed.packages())
if (any(installed_packages == FALSE)) {
  install.packages(packages[!installed_packages])
}
invisible(lapply(packages, library, character.only = TRUE))

# --- Basic configuration ---
WEALTH_CATEGORY <- "TOTAL"  # Analyze total wealth (real + personal property)
AGE_FILTER <- list(min = 18) # Only include adults aged 18+

# --- Define Census Regions ---
# Create a data table mapping states to census regions and divisions
census_regions <- data.table(
  STATE = c(
    "Connecticut", "Maine", "Massachusetts", "New Hampshire", "Rhode Island", "Vermont",
    "New Jersey", "New York", "Pennsylvania",
    "Indiana", "Illinois", "Michigan", "Ohio", "Wisconsin",
    "Iowa", "Kansas", "Minnesota", "Missouri", "Nebraska", "North Dakota", "South Dakota", "Kansas Territory", "Nebraska Territory",
    "Delaware", "District of Columbia", "Florida", "Georgia", "Maryland",
    "North Carolina", "South Carolina", "Virginia", "West Virginia",
    "Alabama", "Kentucky", "Mississippi", "Tennessee",
    "Arkansas", "Louisiana", "Oklahoma", "Texas",
    "Arizona", "Colorado", "Idaho", "New Mexico", "Montana", "Utah", "Nevada", "Wyoming", "New Mexico Territory", "Utah Territory",
    "Alaska", "California", "Hawaii", "Oregon", "Washington", "Washington Territory"
  ),
  REGION = c(
    rep("Northeast", 9), rep("Midwest", 14), rep("South", 17), rep("West", 16)
  ),
  DIVISION = c(
    rep("New England", 6), rep("Middle Atlantic", 3),
    rep("East North Central", 5), rep("West North Central", 9),
    rep("South Atlantic", 9), rep("East South Central", 4), rep("West South Central", 4),
    rep("Mountain", 10), rep("Pacific", 6)
  )
)
# Add a macro-region field (North = Northeast + Midwest)
census_regions[, MACRO_REGION := ifelse(REGION %in% c("Northeast", "Midwest"), "North", REGION)]

# --- Define Confederate States ---
confederate_states <- c(
  "South Carolina", "Mississippi", "Florida", "Alabama", "Georgia", 
  "Louisiana", "Texas", "Virginia", "Arkansas", "Tennessee", "North Carolina"
)

# --- Load OCC1950 Code Mapping ---
# Maps occupation codes to descriptive names
occ1950_mapping_file <- "Data/OCC1950_codes.csv"
occ1950_map <- NULL
if (file.exists(occ1950_mapping_file)) {
  tryCatch({
    occ1950_map <- fread(occ1950_mapping_file, colClasses = list(character = "Code")) 
    if ("Code" %in% names(occ1950_map) && "Occupation" %in% names(occ1950_map)) {
      setnames(occ1950_map, c("Code", "Occupation"), c("OCC1950_code_char", "Occupation_Name"))
      occ1950_map[, OCC1950 := as.numeric(OCC1950_code_char)]
      occ1950_map <- occ1950_map[!is.na(OCC1950), .(OCC1950, Occupation_Name)]
      if(nrow(occ1950_map) == 0) occ1950_map <- NULL
    } else {
      cat("Warning: 'Code' or 'Occupation' column not found in OCC1950_codes.csv.\n")
      occ1950_map <- NULL
    }
  }, error = function(e) {
    cat("Error loading OCC1950_codes.csv:", e$message, "\n")
    occ1950_map <- NULL
  })
} else {
  cat("Warning: OCC1950_codes.csv not found.\n")
}

# --- Define regions for analysis ---
region_definitions_main <- list( 
  list(name = "United States", filter_col = NULL, filter_val = NULL),
  list(name = "North", filter_col = "MACRO_REGION", filter_val = "North"),
  list(name = "Northeast", filter_col = "REGION", filter_val = "Northeast"),
  list(name = "New England", filter_col = "DIVISION", filter_val = "New England"),
  list(name = "Middle Atlantic", filter_col = "DIVISION", filter_val = "Middle Atlantic"),
  list(name = "Midwest", filter_col = "REGION", filter_val = "Midwest"),
  list(name = "East North Central", filter_col = "DIVISION", filter_val = "East North Central"),
  list(name = "West North Central", filter_col = "DIVISION", filter_val = "West North Central"),
  list(name = "South", filter_col = "MACRO_REGION", filter_val = "South"),
  list(name = "South Atlantic", filter_col = "DIVISION", filter_val = "South Atlantic"),
  list(name = "East South Central", filter_col = "DIVISION", filter_val = "East South Central"),
  list(name = "West South Central", filter_col = "DIVISION", filter_val = "West South Central"),
  list(name = "West", filter_col = "MACRO_REGION", filter_val = "West"),
  list(name = "Mountain", filter_col = "DIVISION", filter_val = "Mountain"),
  list(name = "Pacific", filter_col = "DIVISION", filter_val = "Pacific")
)

# --- Helper Functions ---
# Calculate weighted quantiles (used for median wealth)
weighted_quantile <- function(x, probs, weights = NULL) {
  if (is.null(weights)) weights <- rep(1, length(x))
  nas <- is.na(x) | is.na(weights)
  x <- x[!nas]
  weights <- weights[!nas]
  if (length(x) == 0) return(rep(NA_real_, length(probs)))
  ord <- order(x)
  x <- x[ord]
  weights <- weights[ord]
  weights <- weights / sum(weights)
  cum_weights <- cumsum(weights)
  
  sapply(probs, function(p) {
    if (p <= 0) return(x[1])
    if (p >= 1) return(x[length(x)])
    idx <- findInterval(p, cum_weights, left.open = FALSE, rightmost.closed = FALSE)
    if (idx == 0) return(x[1])
    if (idx == length(x)) return(x[length(x)])
    
    w1 <- cum_weights[idx]
    w2 <- cum_weights[idx + 1]
    x1 <- x[idx]
    x2 <- x[idx + 1]
    
    if (w1 == w2) return(x1)
    return(x1 + (x2 - x1) * (p - w1) / (w2 - w1))
  })
}

# Calculate Gini coefficient for wealth inequality
gini <- function(x, weights = NULL) {
  if (is.null(weights)) weights <- rep(1, length(x))
  nas <- is.na(x) | is.na(weights)
  x <- x[!nas]
  weights <- weights[!nas]
  if (length(x) <= 1) return(NA_real_)
  if (all(x == 0)) return(0)
  if (any(x < 0)) {
    warning("Gini calculated with negative values present.")
  }
  ord <- order(x)
  x <- x[ord]
  weights <- weights[ord]
  weights <- weights / sum(weights)
  cum_weights <- cumsum(weights)
  cum_values_weighted <- cumsum(x * weights)
  
  total_weighted_sum = sum(x * weights)
  if (total_weighted_sum == 0 && any(x > 0)) {
    return(NA_real_)
  }
  if (total_weighted_sum == 0) return(0)
  
  lorenz_y <- cum_values_weighted / total_weighted_sum
  area_under_curve <- sum(weights * (c(0, lorenz_y[-length(lorenz_y)]) + lorenz_y) / 2)
  G <- 1 - 2 * area_under_curve
  return(max(0, min(1, G)))
}

# Apply age and sex filters to the data
apply_demographic_filters <- function(data, age_filter = NULL, sex_filter = NULL) {
  filtered_data <- copy(data)
  if (!is.null(sex_filter)) {
    filtered_data <- filtered_data[SEX %in% sex_filter]
  }
  if (!is.null(age_filter)) {
    min_age <- age_filter$min %||% -Inf
    max_age <- age_filter$max %||% Inf
    filtered_data <- filtered_data[AGE >= min_age & AGE <= max_age]
  }
  return(filtered_data)
}
`%||%` <- function(a, b) if (is.null(a)) b else a

# Define fixed occupation order for consistent tables
occ_order <- c(
  "Farm Subtotal",
  "Farmers and planters",
  "Farm laborers (total)",
  "Farm laborers (family)",
  "Farm laborers (other)",
  "Non-Farm Subtotal",
  "Professional and technical",
  "Managers and proprietors",
  "Clerical and kindred",
  "Sales workers",
  "Craftsmen",
  "Operatives",
  "Household servants",
  "Other service workers",
  "Laborers",
  "Total"
)

# Create occupation order mapping for consistent sorting
occupation_order_map <- data.table(
  occupation = occ_order,
  order_value = 1:length(occ_order)
)

# Classify individuals into agricultural occupations
process_agricultural <- function(data) {
  data_copy <- copy(data)
  # Create farm occupation categories based on OCC1950 codes
  data_copy[, occupation := fcase(
    OCC1950 %in% c(100, 123), "Farmers and planters",
    OCC1950 %in% c(810, 820, 830, 840) & RELATE >= 2 & RELATE <= 10, "Farm laborers (family)",
    OCC1950 %in% c(810, 820, 830, 840), "Farm laborers (other)",
    default = NA_character_
  )]
  return(data_copy[!is.na(occupation)])
}

# Classify individuals into non-agricultural occupations
process_nonagricultural <- function(data) {
  data_copy <- copy(data)
  # Create non-farm occupation categories based on OCC1950 codes
  data_copy[, occupation := fcase(
    OCC1950 >= 0 & OCC1950 <= 99, "Professional and technical",
    OCC1950 >= 200 & OCC1950 <= 290, "Managers and proprietors",
    OCC1950 >= 300 & OCC1950 <= 390, "Clerical and kindred",
    OCC1950 >= 400 & OCC1950 <= 490, "Sales workers",
    OCC1950 >= 500 & OCC1950 <= 595, "Craftsmen",
    OCC1950 >= 600 & OCC1950 <= 690, "Operatives",
    OCC1950 >= 700 & OCC1950 <= 720, "Household servants",
    OCC1950 >= 730 & OCC1950 <= 790, "Other service workers",
    OCC1950 >= 910 & OCC1950 <= 970, "Laborers",
    default = NA_character_
  )]
  return(data_copy[!is.na(occupation)])
}

# Create a wealth distribution table for Appendix Table A.1
create_wealth_dist_table_for_A1 <- function(data, wealth_var = "WEALTH_TOTAL") {
  wealth_brackets <- c(0, 100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 999996, Inf)
  bracket_labels <- c(
    "$0", "$1-100", "$101-500", "$501-1,000", "$1,001-5,000", 
    "$5,001-10,000", "$10,001-50,000", "$50,001-100,000", 
    "$100,001-500,000", "$500,001-999,996", "$999,997+"
  )
  
  data_copy <- copy(data)
  data_copy[, wealth_bracket := cut(
    get(wealth_var), 
    breaks = wealth_brackets, 
    labels = bracket_labels[-length(bracket_labels)],
    right = FALSE,
    include.lowest = TRUE
  )]
  data_copy[get(wealth_var) >= 999997, wealth_bracket := bracket_labels[length(bracket_labels)]]
  
  # Count individuals in each wealth bracket
  unweighted_counts <- data_copy[, .(
    Count = .N,
    Pct_of_Total_num = (.N / nrow(data_copy)) * 100
  ), by = wealth_bracket]
  
  unweighted_counts[, Pct_of_Total := sprintf("%.2f%%", Pct_of_Total_num)]
  unweighted_counts[, Pct_of_Total_num := NULL] 
  
  # Ensure all brackets appear in the table even if they have 0 count
  all_brackets_dt <- data.table(wealth_bracket = factor(bracket_labels, levels = bracket_labels))
  result <- merge(all_brackets_dt, unweighted_counts, by = "wealth_bracket", all.x = TRUE)
  
  result[is.na(Count), Count := 0]
  result[is.na(Pct_of_Total), Pct_of_Total := "0.00%"]
  
  setorder(result, wealth_bracket)
  setnames(result, "wealth_bracket", "Wealth Bracket")
  return(result[, .(`Wealth Bracket`, Count, Pct_of_Total)])
}

# Create household wealth distribution table by region for Table 8
create_household_wealth_dist_table8 <- function(data, wealth_var = "WEALTH_TOTAL", weight_var = "HHWT") {
  wealth_brackets <- c(0, 1, 101, 501, 1001, 5001, 10001, 50001, 100000, Inf)
  bracket_labels <- c(
    "$0", "$1-100", "$101-500", "$501-1,000", "$1,001-5,000", 
    "$5,001-10,000", "$10,001-50,000", "$50,001-100,000", "$100,000+"
  )
  
  # Filter to North and South only
  data_filtered <- data[MACRO_REGION %in% c("North", "South")]
  
  if (nrow(data_filtered) == 0) {
    # Return empty table with proper structure
    empty_result <- data.table(
      `Wealth Bracket` = bracket_labels,
      North = rep(0, length(bracket_labels)),
      South = rep(0, length(bracket_labels))
    )
    return(empty_result)
  }
  
  # Create wealth brackets
  data_copy <- copy(data_filtered)
  data_copy[, wealth_bracket := cut(
    get(wealth_var), 
    breaks = wealth_brackets, 
    labels = bracket_labels,
    right = FALSE,
    include.lowest = TRUE
  )]
  
  # Calculate weighted percentages by region
  region_totals <- data_copy[, .(total_weight = sum(get(weight_var), na.rm = TRUE)), by = MACRO_REGION]
  
  bracket_counts <- data_copy[!is.na(wealth_bracket), .(
    bracket_weight = sum(get(weight_var), na.rm = TRUE)
  ), by = .(MACRO_REGION, wealth_bracket)]
  
  # Merge with totals to calculate percentages
  bracket_pcts <- merge(bracket_counts, region_totals, by = "MACRO_REGION")
  bracket_pcts[, percentage := (bracket_weight / total_weight) * 100]
  
  # Reshape to wide format
  result_wide <- dcast(bracket_pcts, wealth_bracket ~ MACRO_REGION, value.var = "percentage", fill = 0)
  
  # Ensure all brackets are present
  all_brackets_dt <- data.table(wealth_bracket = factor(bracket_labels, levels = bracket_labels))
  result <- merge(all_brackets_dt, result_wide, by = "wealth_bracket", all.x = TRUE)
  
  # Fill missing values with 0
  for (col in c("North", "South")) {
    if (col %in% names(result)) {
      result[is.na(get(col)), (col) := 0]
    } else {
      result[, (col) := 0]
    }
  }
  
  # Ensure proper ordering and column names
  setorder(result, wealth_bracket)
  setnames(result, "wealth_bracket", "Wealth Bracket")
  result <- result[, .(`Wealth Bracket`, North, South)]
  
  return(result)
}

# Analyze topcoded individuals by region for Table A.3
analyze_topcoded_by_region_for_A3 <- function(topcoded_data_regional, total_topcoded_count_overall, geo_var_name) {
  if (nrow(topcoded_data_regional) == 0) {
    return(data.table(Region = character(), Top_coded_Count = integer(), Pct_of_All_Top_coded = numeric()))
  }
  
  analysis <- copy(topcoded_data_regional)
  setnames(analysis, geo_var_name, "Region")
  setnames(analysis, "N", "Top_coded_Count")
  
  if (total_topcoded_count_overall > 0) {
    analysis[, Pct_of_All_Top_coded := (Top_coded_Count / total_topcoded_count_overall) * 100]
  } else {
    analysis[, Pct_of_All_Top_coded := 0]
  }
  return(analysis[, .(Region, Top_coded_Count, Pct_of_All_Top_coded)])
}

# --- Load and Process Data ---
cat("Loading data...\n")
# Select only the columns we need from the census data
cols_to_keep <- c("SERIAL", "COUNTYNHG", "RELATE", "HHWT", "PERWT",
                  "OCC1950", "AGE", "SEX", "GQTYPE", "RACE", "BPL",
                  "REALPROP", "PERSPROP")

# Check if IPUMS.csv exists
if (!file.exists("Data/IPUMS.csv")) {
  stop("Error: Data/IPUMS.csv not found. Please ensure the data file is in the correct location.")
}

ipums_micro <- fread("Data/IPUMS.csv", select = cols_to_keep)

# Convert variables to numeric
ipums_micro[, REALPROP := as.numeric(REALPROP)]
ipums_micro[, PERSPROP := as.numeric(PERSPROP)]
ipums_micro[, OCC1950 := as.numeric(OCC1950)]
ipums_micro[, GQTYPE := as.numeric(GQTYPE)]
ipums_micro[, RACE := as.numeric(RACE)]
ipums_micro[, BPL := as.numeric(BPL)]

# Load county attributes to get state information
if (!file.exists("Data/county_attributes.csv")) {
  stop("Error: Data/county_attributes.csv not found. Please ensure the data file is in the correct location.")
}
county_attributes <- fread("Data/county_attributes.csv", select = c("GISJOIN_1860", "STATENAM"))

# Filter out missing or invalid county codes
ipums_1860 <- ipums_micro[!is.na(COUNTYNHG) & COUNTYNHG != "9999999"]
ipums_1860[, GISJOIN := paste0("G", sprintf("%07d", as.numeric(COUNTYNHG)))]

# Free memory
rm(ipums_micro); gc()

# Match counties to states and regions
state_lookup <- unique(county_attributes[, .(GISJOIN = GISJOIN_1860, STATENAM)])
ipums_1860 <- merge(ipums_1860, state_lookup, by = "GISJOIN", all.x = TRUE)
ipums_1860 <- merge(ipums_1860, census_regions, by.x = "STATENAM", by.y = "STATE", all.x = TRUE)

# Create household ID and calculate total wealth (real + personal property) for complete dataset
ipums_1860[, HH_ID := as.character(SERIAL)]
ipums_1860[, WEALTH_TOTAL := REALPROP + PERSPROP]

# --- Calculate Tables A.1-A.3 from Complete Dataset (ALL individuals, ALL races, ALL GQTYPE) ---
cat("\nCalculating Appendix Tables A.1-A.3 from complete dataset...\n")

# Identify top-coded individuals from complete dataset
topcoded_individuals_complete <- ipums_1860[REALPROP >= 999997 | PERSPROP >= 999997]
n_topcoded_individuals_complete <- nrow(topcoded_individuals_complete)

cat(sprintf("  - Identified %s top-coded individuals\n", format(n_topcoded_individuals_complete, big.mark = ",")))

# Create wealth distribution table with top-coded values for Table A.1 (COMPLETE DATASET)
table_a1_data <- create_wealth_dist_table_for_A1(ipums_1860, "WEALTH_TOTAL")

# --- Prepare data for Table A.2: Occupations with Most Top-coded Individuals (COMPLETE DATASET) ---
table_a2_data <- data.table(Occupation = character(), Top_coded_Count = integer(), Pct_of_All_Top_coded = character())

if (n_topcoded_individuals_complete > 0) {
  # Count top-coded individuals by occupation code
  raw_occ_counts <- topcoded_individuals_complete[!is.na(OCC1950), .N, by = OCC1950] 
  setnames(raw_occ_counts, "N", "Top_coded_Count")
  
  # Map occupation codes to names if possible
  if (!is.null(occ1950_map) && nrow(occ1950_map) > 0) {
    raw_occ_counts[, OCC1950 := as.numeric(OCC1950)] 
    merged_counts <- merge(raw_occ_counts, occ1950_map, by = "OCC1950", all.x = TRUE)
    merged_counts[is.na(Occupation_Name), Occupation_Name := paste("Code", OCC1950, "(No desc.)")]
    merged_counts <- merged_counts[!is.na(OCC1950)] 
    setnames(merged_counts, "Occupation_Name", "Occupation")
  } else {
    merged_counts <- raw_occ_counts
    merged_counts[, Occupation := paste("OCC1950 Code:", OCC1950)]
    merged_counts <- merged_counts[!is.na(OCC1950)]
  }
  setorder(merged_counts, -Top_coded_Count)
  
  # Take top 19 occupations and group the rest as "Other"
  if (nrow(merged_counts) > 20) {
    top_19_occs <- head(merged_counts, 19)
    other_occs_sum <- sum(merged_counts[20:nrow(merged_counts)]$Top_coded_Count)
    other_row <- data.table(
      Occupation = "Other occupations", 
      Top_coded_Count = other_occs_sum
    )
    table_a2_calc <- rbindlist(list(top_19_occs[, .(Occupation, Top_coded_Count)], 
                                    other_row[, .(Occupation, Top_coded_Count)]))
  } else {
    table_a2_calc <- merged_counts[, .(Occupation, Top_coded_Count)]
  }
  
  # Calculate percentage of all top-coded
  if (nrow(table_a2_calc) > 0 && n_topcoded_individuals_complete > 0) {
    table_a2_calc[, Pct_of_All_Top_coded_num := (Top_coded_Count / n_topcoded_individuals_complete) * 100]
    table_a2_calc[, Pct_of_All_Top_coded := sprintf("%.2f%%", Pct_of_All_Top_coded_num)]
    table_a2_calc[, Pct_of_All_Top_coded_num := NULL]
  } else if (nrow(table_a2_calc) > 0) {
    table_a2_calc[, Pct_of_All_Top_coded := "0.00%"]
  }
  table_a2_data <- table_a2_calc
}

# --- Prepare data for Table A.3: Regional Distribution of Top-coded Individuals (COMPLETE DATASET) ---
topcoded_counts_by_region_complete <- topcoded_individuals_complete[!is.na(REGION), .N, by = REGION]
topcoded_counts_by_division_complete <- topcoded_individuals_complete[!is.na(DIVISION), .N, by = DIVISION]
topcoded_counts_by_macro_complete <- topcoded_individuals_complete[!is.na(MACRO_REGION), .N, by = MACRO_REGION]

regions_ordered <- sapply(region_definitions_main, `[[`, "name")

region_analysis_A3_complete <- analyze_topcoded_by_region_for_A3(topcoded_counts_by_region_complete, n_topcoded_individuals_complete, "REGION")
division_analysis_A3_complete <- analyze_topcoded_by_region_for_A3(topcoded_counts_by_division_complete, n_topcoded_individuals_complete, "DIVISION")
macro_analysis_A3_complete <- analyze_topcoded_by_region_for_A3(topcoded_counts_by_macro_complete, n_topcoded_individuals_complete, "MACRO_REGION")

# Prepare data for Table A.3
table_a3_data_list <- list()
for (region_name_iter in regions_ordered) {
  region_row_a3 <- data.table()
  if (region_name_iter == "United States") {
    region_row_a3 <- data.table(
      Region = "United States",
      Top_coded_Count = n_topcoded_individuals_complete,
      Pct_of_All_Top_coded = fifelse(n_topcoded_individuals_complete > 0, 100.00, 0.00)
    )
  } else {
    found_in_macro <- macro_analysis_A3_complete[Region == region_name_iter]
    found_in_region <- region_analysis_A3_complete[Region == region_name_iter]
    found_in_division <- division_analysis_A3_complete[Region == region_name_iter]
    
    if (nrow(found_in_macro) > 0) {
      region_row_a3 <- found_in_macro
    } else if (nrow(found_in_region) > 0) {
      region_row_a3 <- found_in_region
    } else if (nrow(found_in_division) > 0) {
      region_row_a3 <- found_in_division
    }
  }
  if (nrow(region_row_a3) > 0) {
    table_a3_data_list[[length(table_a3_data_list) + 1]] <- region_row_a3
  } else { 
    table_a3_data_list[[length(table_a3_data_list) + 1]] <- data.table(Region = region_name_iter, Top_coded_Count = 0, Pct_of_All_Top_coded = 0)
  }
}
table_a3_data <- rbindlist(table_a3_data_list, use.names = TRUE, fill = TRUE)
if (nrow(table_a3_data) > 0) {
  table_a3_data <- table_a3_data[match(regions_ordered, Region)]
  table_a3_data[is.na(Top_coded_Count), Top_coded_Count := 0]
  table_a3_data[is.na(Pct_of_All_Top_coded), Pct_of_All_Top_coded := 0.00]
}

# --- Calculate Table 4: Incarceration Rates for Adult Men by Race and Region ---
cat("\nCalculating Table 4: Incarceration Rates...\n")

# Get all adult men aged 18+ (both in households and institutions) BEFORE any filtering
all_adult_men_for_incarceration <- ipums_1860[SEX == 1 & AGE >= 18 & !is.na(GQTYPE) & !is.na(RACE)]

# Function to calculate incarceration rates by race for a specific region
calculate_incarceration_rates_by_race_region <- function(data) {
  data_copy <- copy(data)
  
  if (nrow(data_copy) == 0) {
    return(data.table(
      race_category = c("White_native", "White_immigrant", "White_total", "Black"),
      incarceration_rate = rep(0, 4)
    ))
  }
  
  # Calculate each category using consistent logic
  results_list <- list()
  
  # White native (born in US)
  white_native_data <- data_copy[RACE == 1 & BPL < 100 & !is.na(BPL)]
  native_total <- sum(white_native_data$PERWT, na.rm = TRUE)
  native_incarcerated <- sum(white_native_data[GQTYPE == 2, PERWT], na.rm = TRUE)
  native_rate <- ifelse(native_total > 0, (native_incarcerated / native_total) * 100, 0)
  
  # White immigrant (born outside US)  
  white_immigrant_data <- data_copy[RACE == 1 & BPL >= 100 & !is.na(BPL)]
  immigrant_total <- sum(white_immigrant_data$PERWT, na.rm = TRUE)
  immigrant_incarcerated <- sum(white_immigrant_data[GQTYPE == 2, PERWT], na.rm = TRUE)
  immigrant_rate <- ifelse(immigrant_total > 0, (immigrant_incarcerated / immigrant_total) * 100, 0)
  
  # White total (all white, calculated by combining native + immigrant + missing BPL)
  white_total_data <- data_copy[RACE == 1]
  total_white_pop <- sum(white_total_data$PERWT, na.rm = TRUE)
  total_white_incarcerated <- sum(white_total_data[GQTYPE == 2, PERWT], na.rm = TRUE)
  total_white_rate <- ifelse(total_white_pop > 0, (total_white_incarcerated / total_white_pop) * 100, 0)
  
  # Black
  black_data <- data_copy[RACE == 2]
  black_total <- sum(black_data$PERWT, na.rm = TRUE)
  black_incarcerated <- sum(black_data[GQTYPE == 2, PERWT], na.rm = TRUE)
  black_rate <- ifelse(black_total > 0, (black_incarcerated / black_total) * 100, 0)
  
  # Return results
  result <- data.table(
    race_category = c("White_native", "White_immigrant", "White_total", "Black"),
    incarceration_rate = round(c(native_rate, immigrant_rate, total_white_rate, black_rate), 3)
  )
  
  return(result)
}

# Function to calculate incarceration rates across all regions
analyze_incarceration_by_region <- function(data, current_region_definitions) {
  results_list <- lapply(current_region_definitions, function(def) {
    subset_data <- data
    if (!is.null(def$parent_region_filter_col)) {
      if (def$parent_region_filter_col %in% names(subset_data)) {
        subset_data <- subset_data[get(def$parent_region_filter_col) == def$parent_region_filter_val]
      } else {
        subset_data <- subset_data[0,]
      }
    }
    if (!is.null(def$filter_col)) {
      if (def$filter_col %in% names(subset_data)) {
        subset_data <- subset_data[get(def$filter_col) == def$filter_val]
      } else {
        subset_data <- subset_data[0,]
      }
    }
    
    race_stats <- calculate_incarceration_rates_by_race_region(subset_data)
    race_stats[, Region := def$name]
    return(race_stats)
  })
  
  all_results <- rbindlist(results_list, use.names = TRUE, fill = TRUE)
  return(all_results)
}

# Calculate incarceration rates by race and region
incarceration_by_race_region <- analyze_incarceration_by_region(all_adult_men_for_incarceration, region_definitions_main)

# Reshape to wide format for table (regions as rows, race categories as columns)
table4_data <- dcast(incarceration_by_race_region, Region ~ race_category, value.var = "incarceration_rate", fill = 0)

# Ensure proper ordering of regions
table4_data <- table4_data[match(regions_ordered, Region)]

# Reorder columns to match expected header order: Region, White_native, White_immigrant, White_total, Black
expected_col_order <- c("Region", "White_native", "White_immigrant", "White_total", "Black")
existing_cols <- intersect(expected_col_order, names(table4_data))
table4_data <- table4_data[, ..existing_cols]

# Rename columns
rename_cols <- intersect(c("White_native", "White_immigrant", "White_total", "Black"), names(table4_data))
if (length(rename_cols) > 0) {
  new_names <- c("White_native" = "White (native)", "White_immigrant" = "White (immigrant)", 
                 "White_total" = "White (total)", "Black" = "Black")[rename_cols]
  setnames(table4_data, rename_cols, new_names)
}

# --- Apply GQTYPE = 0 filter for household analysis ---
cat("\nApplying filters for household analysis...\n")
n_before_gq_filter <- nrow(ipums_1860)
ipums_1860 <- ipums_1860[GQTYPE == 0]
n_after_gq_filter <- nrow(ipums_1860)
cat(sprintf("  - Applied GQTYPE = 0 filter: %s observations remain\n", 
            format(n_after_gq_filter, big.mark = ",")))

# --- Identify top-coded households for Tables 1-3, 5-8 analysis ---
# Identify households with top-coded individuals (for removal from household analysis)
topcoded_households_for_removal <- ipums_1860[REALPROP >= 999997 | PERSPROP >= 999997]
topcoded_serials_for_removal <- topcoded_households_for_removal[, unique(SERIAL)]
n_topcoded_households_for_removal <- length(topcoded_serials_for_removal)

cat(sprintf("  - Identified %s households with top-coded individuals to remove\n", 
            format(n_topcoded_households_for_removal, big.mark = ",")))

# --- Apply White Head of Household filter for Tables 1-3, 5-8 ---
n_before_race_filter <- nrow(ipums_1860)

# Get households with white heads (RELATE == 1 & RACE == 1)
white_head_households <- ipums_1860[RELATE == 1 & RACE == 1, unique(SERIAL)]
n_white_head_households <- length(white_head_households)

# Keep all members of households with white heads
ipums_1860 <- ipums_1860[SERIAL %in% white_head_households]
n_after_race_filter <- nrow(ipums_1860)

cat(sprintf("  - Applied white head of household filter: %s observations remain\n", 
            format(n_after_race_filter, big.mark = ",")))

# --- Remove households with top-coded individuals (from white-headed households only) ---
# Remove individuals from households with top-coded members (for Tables 1-3, 5-8 analysis)
ipums_1860_no_topcoded <- ipums_1860[!SERIAL %in% topcoded_serials_for_removal]
n_final <- nrow(ipums_1860_no_topcoded)
cat(sprintf("  - Removed top-coded households: %s observations remain\n",
            format(n_final, big.mark = ",")))

# Calculate total household size
household_total_size <- ipums_1860_no_topcoded[, .N, by = HH_ID]
setnames(household_total_size, "N", "TOTAL_MEMBERS")

# Filter to adults (age 18+) and ensure we only analyze individuals from white-headed households
ipums_1860_adults_no_topcoded <- apply_demographic_filters(ipums_1860_no_topcoded, age_filter = AGE_FILTER)
cat(sprintf("  - Applied age filter (18+): %s observations remain\n", 
            format(nrow(ipums_1860_adults_no_topcoded), big.mark = ",")))

# Split by gender (SEX=1 for men, SEX=2 for women) - only including those from white-headed households
men_18 <- ipums_1860_adults_no_topcoded[SEX == 1]
women_18 <- ipums_1860_adults_no_topcoded[SEX == 2]

# Aggregate wealth at household level
household_wealth_all_members <- ipums_1860_no_topcoded[, .(
  REALPROP_HH_ALLMEM = sum(REALPROP, na.rm = TRUE),
  PERSPROP_HH_ALLMEM = sum(PERSPROP, na.rm = TRUE),
  TOTAL_WEALTH_HH_ALLMEM = sum(WEALTH_TOTAL, na.rm = TRUE),
  HHWT = first(HHWT), 
  REGION = first(REGION), 
  DIVISION = first(DIVISION),
  MACRO_REGION = first(MACRO_REGION),
  GISJOIN = first(GISJOIN),
  STATENAM = first(STATENAM)
), by = HH_ID]

# Calculate per capita household wealth
household_wealth <- merge(household_wealth_all_members, household_total_size, by = "HH_ID")
household_wealth[, WEALTH_TOTAL := TOTAL_WEALTH_HH_ALLMEM / TOTAL_MEMBERS]
household_wealth[, IS_CONFEDERATE_STATE := STATENAM %in% confederate_states]

# Function to calculate wealth statistics for a geographic region
analyze_geography <- function(data, geo_name, weight_var) {
  if (nrow(data) == 0) {
    return(data.table(Region = geo_name, population = 0, mean_wealth = NA_real_, median_wealth = NA_real_, gini = NA_real_))
  }
  stats <- data[, .(
    population = sum(get(weight_var), na.rm = TRUE),
    mean_wealth = round(weighted.mean(WEALTH_TOTAL, get(weight_var), na.rm = TRUE), 0),
    median_wealth = round(weighted_quantile(WEALTH_TOTAL, 0.5, weights = get(weight_var)), 0),
    gini = round(gini(WEALTH_TOTAL, get(weight_var)), 3)
  )]
  stats[, Region := geo_name]
  return(stats)
}

cat("\nCalculating regional wealth statistics...\n")

# Function to analyze wealth statistics across multiple regions
analyze_all_regions_dt <- function(data, weight_var, pop_name, current_region_definitions) {
  results_list <- lapply(current_region_definitions, function(def) {
    subset_data <- data
    if (!is.null(def$parent_region_filter_col)) {
      if (def$parent_region_filter_col %in% names(subset_data)) {
        subset_data <- subset_data[get(def$parent_region_filter_col) == def$parent_region_filter_val]
      } else {
        subset_data <- subset_data[0,]
      }
    }
    if (!is.null(def$filter_col)) {
      if (def$filter_col %in% names(subset_data)) {
        subset_data <- subset_data[get(def$filter_col) == def$filter_val]
      } else {
        subset_data <- subset_data[0,]
      }
    }
    
    stats <- analyze_geography(subset_data, def$name, weight_var)
    stats$Region <- def$name 
    return(stats)
  })
  all_results <- rbindlist(results_list, use.names = TRUE, fill = TRUE)
  all_results[, Population_type := pop_name]
  return(all_results)
}

# Calculate wealth statistics by region for men, women, and households
men_results <- analyze_all_regions_dt(men_18, "PERWT", "Men", region_definitions_main)
women_results <- analyze_all_regions_dt(women_18, "PERWT", "Women", region_definitions_main)
household_results <- analyze_all_regions_dt(household_wealth, "HHWT", "Households", region_definitions_main)

# --- Prepare for Table A.4 (Confederate vs Non-Confederate states) ---
region_definitions_A4 <- list(
  list(name = "United States", filter_col = NULL, filter_val = NULL),
  list(name = "North", filter_col = "MACRO_REGION", filter_val = "North"),
  list(name = "South", filter_col = "MACRO_REGION", filter_val = "South"),
  list(name = "South (Confederate)", filter_col = "IS_CONFEDERATE_STATE", filter_val = TRUE, 
       parent_region_filter_col = "MACRO_REGION", parent_region_filter_val = "South"),
  list(name = "South (Non-Confederate)", filter_col = "IS_CONFEDERATE_STATE", filter_val = FALSE, 
       parent_region_filter_col = "MACRO_REGION", parent_region_filter_val = "South"),
  list(name = "West", filter_col = "MACRO_REGION", filter_val = "West")
)
regions_ordered_A4 <- sapply(region_definitions_A4, `[[`, "name")
household_results_A4 <- analyze_all_regions_dt(household_wealth, "HHWT", "Households_A4", region_definitions_A4)

# --- Prepare for Tables A.5a and A.5b (Farm vs Non-Farm households) ---
# Define occupation codes for farm occupations
farm_occ_codes <- c(100, 123, 810, 820, 830, 840)
non_farm_occ_condition <- expression(
  (OCC1950 >= 0 & OCC1950 <= 99) | (OCC1950 >= 200 & OCC1950 <= 290) |
    (OCC1950 >= 300 & OCC1950 <= 390) | (OCC1950 >= 400 & OCC1950 <= 490) |
    (OCC1950 >= 500 & OCC1950 <= 595) | (OCC1950 >= 600 & OCC1950 <= 690) |
    (OCC1950 >= 700 & OCC1950 <= 720) | (OCC1950 >= 730 & OCC1950 <= 790) |
    (OCC1950 >= 910 & OCC1950 <= 970)
)

# Identify households with farm vs non-farm heads
temp_head_data <- ipums_1860_no_topcoded[RELATE == 1, .(HH_ID, OCC1950)]
temp_head_data[, head_occ_cat := fcase(
  OCC1950 %in% farm_occ_codes, "Farm",
  eval(non_farm_occ_condition), "Non-Farm",
  default = NA_character_
)]
hh_ids_farm_head <- temp_head_data[head_occ_cat == "Farm", unique(HH_ID)]
hh_ids_non_farm_head <- temp_head_data[head_occ_cat == "Non-Farm", unique(HH_ID)]
rm(temp_head_data)

# Calculate wealth statistics for farm and non-farm headed households
household_wealth_farm_head <- household_wealth[HH_ID %in% hh_ids_farm_head]
household_wealth_non_farm_head <- household_wealth[HH_ID %in% hh_ids_non_farm_head]

results_A5a <- analyze_all_regions_dt(household_wealth_farm_head, "HHWT", "Households (Farm Head)", region_definitions_main)
results_A5b <- analyze_all_regions_dt(household_wealth_non_farm_head, "HHWT", "Households (Non-Farm Head)", region_definitions_main)

# --- Prepare Table 8: Household Wealth Distribution by Region ---
cat("\nCreating Table 8: Household Wealth Distribution...\n")
table8_data <- create_household_wealth_dist_table8(household_wealth, "WEALTH_TOTAL", "HHWT")

cat("\nCalculating occupational statistics...\n")

# Function to analyze occupational structure and wealth by occupation with consistent ordering
run_occupational_analysis <- function(filtered_data_dt, region_name) {
  if (nrow(filtered_data_dt) == 0) {
    empty_result <- data.table(
      occupation = occ_order,
      total = rep(0, length(occ_order)),
      mean_wealth = rep(NA_real_, length(occ_order)),
      median_wealth = rep(NA_real_, length(occ_order)),
      section = rep(NA_character_, length(occ_order)),
      percentage = rep(0, length(occ_order)),
      order_value = 1:length(occ_order)
    )
    return(empty_result)
  }
  
  # Process agricultural and non-agricultural occupations separately
  processed_data <- filtered_data_dt[, .(SERIAL, OCC1950, RELATE, WEALTH_TOTAL, PERWT)]
  ag_results <- process_agricultural(processed_data)
  nonag_results <- process_nonagricultural(processed_data)
  
  # Combine results with section indicator
  combined_data <- rbindlist(list(
    ag_results[, section := "Farm"],
    nonag_results[, section := "Non-farm"]
  ), use.names = TRUE, fill = TRUE)
  
  if (nrow(combined_data) == 0) {
    empty_result <- data.table(
      occupation = occ_order,
      total = rep(0, length(occ_order)),
      mean_wealth = rep(NA_real_, length(occ_order)),
      median_wealth = rep(NA_real_, length(occ_order)),
      section = rep(NA_character_, length(occ_order)),
      percentage = rep(0, length(occ_order)),
      order_value = 1:length(occ_order)
    )
    return(empty_result)
  }
  
  # Calculate statistics by occupation
  occupation_stats <- combined_data[, .(
    total = .N,
    mean_wealth = weighted.mean(WEALTH_TOTAL, PERWT, na.rm = TRUE),
    median_wealth = weighted_quantile(WEALTH_TOTAL, 0.5, PERWT)
  ), by = .(section, occupation)]
  
  # Create aggregate farm laborers category
  farm_laborers_family <- combined_data[occupation == "Farm laborers (family)"]
  farm_laborers_other <- combined_data[occupation == "Farm laborers (other)"]
  farm_laborers_all <- rbindlist(list(farm_laborers_family, farm_laborers_other), use.names = TRUE, fill = TRUE)
  
  farm_laborers_total_stats <- data.table()
  if (nrow(farm_laborers_all) > 0) {
    farm_laborers_total_stats <- farm_laborers_all[, .(
      section = "Farm", occupation = "Farm laborers (total)", total = .N,
      mean_wealth = weighted.mean(WEALTH_TOTAL, PERWT, na.rm = TRUE),
      median_wealth = weighted_quantile(WEALTH_TOTAL, 0.5, PERWT)
    )]
  }
  
  # Create farm subtotal
  farm_data <- combined_data[section == "Farm"]
  farm_subtotal <- if(nrow(farm_data) > 0) farm_data[, .(
    section = "Farm", occupation = "Farm Subtotal", total = .N,
    mean_wealth = weighted.mean(WEALTH_TOTAL, PERWT, na.rm = TRUE),
    median_wealth = weighted_quantile(WEALTH_TOTAL, 0.5, PERWT))] else data.table()
  
  # Create non-farm subtotal
  nonfarm_data <- combined_data[section == "Non-farm"]
  nonfarm_subtotal <- if(nrow(nonfarm_data) > 0) nonfarm_data[, .(
    section = "Non-farm", occupation = "Non-Farm Subtotal", total = .N,
    mean_wealth = weighted.mean(WEALTH_TOTAL, PERWT, na.rm = TRUE),
    median_wealth = weighted_quantile(WEALTH_TOTAL, 0.5, PERWT))] else data.table()
  
  # Create overall total
  overall_total <- if(nrow(combined_data) > 0) combined_data[, .(
    section = "Total", occupation = "Total", total = .N,
    mean_wealth = weighted.mean(WEALTH_TOTAL, PERWT, na.rm = TRUE),
    median_wealth = weighted_quantile(WEALTH_TOTAL, 0.5, PERWT))] else data.table()
  
  # Combine all occupation categories and calculate percentages
  final_results <- rbindlist(list(occupation_stats, farm_laborers_total_stats, 
                                  farm_subtotal, nonfarm_subtotal, overall_total), 
                             use.names = TRUE, fill = TRUE)
  final_results <- final_results[!is.na(occupation)]
  
  # Add order values for consistent sorting
  final_results <- merge(final_results, occupation_order_map, by = "occupation", all.x = TRUE)
  
  total_individuals_in_combined = nrow(combined_data)
  if (total_individuals_in_combined > 0) {
    final_results[, percentage := 100 * total / total_individuals_in_combined]
  } else {
    final_results[, percentage := 0]
  }
  
  # Round the results
  final_results[, mean_wealth := round(mean_wealth, 0)]
  final_results[, median_wealth := round(median_wealth, 0)]
  final_results[, percentage := round(percentage, 1)]
  
  # Sort by the order value
  setorder(final_results, order_value)
  
  # Fill in missing occupations with default values to ensure consistent rows
  all_occs <- data.table(occupation = occ_order, order_value = 1:length(occ_order))
  final_results <- merge(all_occs, final_results, by = c("occupation", "order_value"), all.x = TRUE)
  final_results[is.na(total), total := 0]
  final_results[is.na(percentage), percentage := 0]
  setorder(final_results, order_value)
  
  return(final_results)
}

# Run occupational analysis for North/South men and women
north_men_results_occ <- run_occupational_analysis(men_18[MACRO_REGION == "North"], "North Men")
south_men_results_occ <- run_occupational_analysis(men_18[MACRO_REGION == "South"], "South Men")
north_women_results_occ <- run_occupational_analysis(women_18[MACRO_REGION == "North"], "North Women")
south_women_results_occ <- run_occupational_analysis(women_18[MACRO_REGION == "South"], "South Women")

cat("\nCreating tables...\n")

# Function to create regional wealth tables
create_regional_table <- function(col_name_men, col_name_women, col_name_hh) {
  dt_men <- men_results[Region %in% regions_ordered, .SD, .SDcols = c("Region", col_name_men)]
  setnames(dt_men, col_name_men, "Men")
  dt_women <- women_results[Region %in% regions_ordered, .SD, .SDcols = c("Region", col_name_women)]
  setnames(dt_women, col_name_women, "Women")
  dt_hh <- household_results[Region %in% regions_ordered, .SD, .SDcols = c("Region", col_name_hh)]
  setnames(dt_hh, col_name_hh, "Households")
  
  base_table <- data.table(Region = regions_ordered)
  merged_table <- merge(base_table, dt_men, by = "Region", all.x = TRUE)
  merged_table <- merge(merged_table, dt_women, by = "Region", all.x = TRUE)
  merged_table <- merge(merged_table, dt_hh, by = "Region", all.x = TRUE)
  
  merged_table <- merged_table[match(regions_ordered, Region)]
  return(as.data.frame(merged_table))
}

# Create main wealth statistics tables
table1 <- create_regional_table("mean_wealth", "mean_wealth", "mean_wealth")
table2 <- create_regional_table("median_wealth", "median_wealth", "median_wealth")
table3 <- create_regional_table("gini", "gini", "gini")

# Prepare occupation data for tables - ensuring consistent ordering
prepare_occ_data_for_table <- function(occ_results_dt, value_col_name, region_col_name) {
  # Check if input data exists and has required columns
  if (is.null(occ_results_dt) || nrow(occ_results_dt) == 0 || 
      !value_col_name %in% names(occ_results_dt)) {
    # Create a template with all occupations in correct order
    res_dt <- data.table(occupation = occ_order, order_value = 1:length(occ_order))
    res_dt[, (region_col_name) := NA_real_]
    if (value_col_name %in% c("percentage", "total")) res_dt[[region_col_name]] <- 0
    return(res_dt)
  }
  
  # Extract the relevant columns and rename for the region
  selected_dt <- occ_results_dt[, .(occupation, order_value, value = get(value_col_name))]
  setnames(selected_dt, "value", region_col_name)
  
  # Ensure all categories are present and ordered correctly
  base_occ_dt <- data.table(occupation = occ_order, order_value = 1:length(occ_order))
  merged_dt <- merge(base_occ_dt, selected_dt, by = c("occupation", "order_value"), all.x = TRUE)
  
  # Replace NA values with 0 for numeric columns
  if (is.numeric(merged_dt[[region_col_name]])) {
    merged_dt[is.na(get(region_col_name)), (region_col_name) := 0]
  }
  
  # Sort by the order value for consistent ordering
  setorder(merged_dt, order_value)
  return(merged_dt)
}

# Create tables comparing North/South by occupation with consistent ordering
create_merged_occ_table <- function(north_res, south_res, value_col) {
  north_data <- prepare_occ_data_for_table(north_res, value_col, "North")
  south_data <- prepare_occ_data_for_table(south_res, value_col, "South")
  
  # Create base table with all occupations in correct order
  base_occ_dt <- data.table(occupation = occ_order, order_value = 1:length(occ_order))
  
  # Merge north and south data, preserving all occupations
  merged <- merge(base_occ_dt, north_data, by = c("occupation", "order_value"), all.x = TRUE)
  merged <- merge(merged, south_data, by = c("occupation", "order_value"), all.x = TRUE)
  
  # Replace NA values with 0 for numeric columns
  numeric_cols <- names(merged)[sapply(merged, is.numeric)]
  for (col in numeric_cols) {
    if (!col %in% c("occupation", "order_value")) {
      merged[is.na(get(col)), (col) := 0]
    }
  }
  
  # Sort by the order value for consistent ordering
  setorder(merged, order_value)
  
  # Remove the order column from the final output
  merged[, order_value := NULL]
  
  return(as.data.frame(merged))
}

# Create occupation tables (now numbered 5, 6, 7)
table5_men <- create_merged_occ_table(north_men_results_occ, south_men_results_occ, "percentage")
table5_women <- create_merged_occ_table(north_women_results_occ, south_women_results_occ, "percentage")
table6_men <- create_merged_occ_table(north_men_results_occ, south_men_results_occ, "mean_wealth")
table6_women <- create_merged_occ_table(north_women_results_occ, south_women_results_occ, "mean_wealth")
table7_men <- create_merged_occ_table(north_men_results_occ, south_men_results_occ, "median_wealth")
table7_women <- create_merged_occ_table(north_women_results_occ, south_women_results_occ, "median_wealth")

# --- Create data for Tables A.4, A.5a, A.5b ---
create_summary_table_A4_A5 <- function(results_data, ordered_regions) {
  target_cols <- c("Region", "Mean Wealth", "Median Wealth", "Gini Coefficient")
  template <- data.table(Region = ordered_regions)
  for(col_name in target_cols[-1]) template[, (col_name) := NA_real_]
  
  if (nrow(results_data) > 0) {
    mean_dt   <- results_data[, .(Region, Value = mean_wealth)][, Metric := "Mean Wealth"]
    median_dt <- results_data[, .(Region, Value = median_wealth)][, Metric := "Median Wealth"]
    gini_dt   <- results_data[, .(Region, Value = gini)][, Metric := "Gini Coefficient"]
    
    combined_dt <- rbindlist(list(mean_dt, median_dt, gini_dt), use.names = TRUE, fill = TRUE)
    
    if(nrow(combined_dt) > 0){
      table_data <- dcast(combined_dt, Region ~ Metric, value.var = "Value")
      table_data <- merge(template[, .(Region)], table_data, by = "Region", all.x = TRUE)
    } else {
      table_data <- template
    }
  } else {
    table_data <- template
  }
  
  table_data <- table_data[match(ordered_regions, Region)] 
  setcolorder(table_data, target_cols)
  
  return(as.data.frame(table_data))
}

# Create specialized household tables
table_A4_data <- create_summary_table_A4_A5(household_results_A4, regions_ordered_A4)
table_A5a_data <- create_summary_table_A4_A5(results_A5a, regions_ordered)
table_A5b_data <- create_summary_table_A4_A5(results_A5b, regions_ordered)

# Create output directory
output_dir <- "Results"
if (!dir.exists(output_dir)) {
  dir.create(output_dir, recursive = TRUE)
}

# --- Create county-level maps ---
cat("\nCreating geopackage...\n")
# Calculate county-level statistics for households
county_household_stats <- household_wealth[, .(
  Mean_household = weighted.mean(WEALTH_TOTAL, HHWT, na.rm = TRUE),
  Median_household = weighted_quantile(WEALTH_TOTAL, 0.5, weights = HHWT),
  Gini_household = gini(WEALTH_TOTAL, HHWT),
  observations_hh = .N
), by = .(GISJOIN)]

# Calculate county-level statistics for men
county_male_stats <- men_18[, .(
  Mean_men = weighted.mean(WEALTH_TOTAL, PERWT, na.rm = TRUE),
  Median_men = weighted_quantile(WEALTH_TOTAL, 0.5, weights = PERWT),
  Gini_men = gini(WEALTH_TOTAL, PERWT),
  observations_men = .N
), by = .(GISJOIN)]

# Combine county statistics
county_stats_combined <- merge(county_household_stats, county_male_stats, by = "GISJOIN", all = TRUE)
county_stats_combined <- merge(county_stats_combined, state_lookup, by = "GISJOIN", all.x = TRUE)

# Load county shapefile and merge with statistics
shapefile_path <- "Data/US_county_1860/US_county_1860.shp" 
if(file.exists(shapefile_path)){
  tryCatch({
    counties_sf <- st_read(shapefile_path, quiet = TRUE)
    counties_with_wealth <- merge(counties_sf, county_stats_combined, by = "GISJOIN", all.x = TRUE)
    output_gpkg_path <- file.path(output_dir, "Maps.gpkg")
    st_write(counties_with_wealth, output_gpkg_path, driver = "GPKG", delete_layer = TRUE, quiet = TRUE)
    cat("  - Geopackage successfully created\n")
  }, error = function(e) {
    cat("  - Error creating geopackage:", e$message, "\n")
  })
} else {
  cat("  - County shapefile not found. Skipping geopackage creation.\n")
}

# --- Write tables to text file ---
output_file_path <- file.path(output_dir, "Replication.txt")
sink(output_file_path)

# Prepare tables A.4, A.5a, and A.5b with properly rounded Gini values
table_A4_data_fixed <- as.data.frame(table_A4_data)
if ("Gini Coefficient" %in% colnames(table_A4_data_fixed)) {
  table_A4_data_fixed$`Gini Coefficient` <- round(as.numeric(table_A4_data_fixed$`Gini Coefficient`), 3)
}

table_A5a_data_fixed <- as.data.frame(table_A5a_data)
if ("Gini Coefficient" %in% colnames(table_A5a_data_fixed)) {
  table_A5a_data_fixed$`Gini Coefficient` <- round(as.numeric(table_A5a_data_fixed$`Gini Coefficient`), 3)
}

table_A5b_data_fixed <- as.data.frame(table_A5b_data)
if ("Gini Coefficient" %in% colnames(table_A5b_data_fixed)) {
  table_A5b_data_fixed$`Gini Coefficient` <- round(as.numeric(table_A5b_data_fixed$`Gini Coefficient`), 3)
}

# Define tables to print with consistent naming (renumbered to reflect Table 8 → Table 4)
tables_to_print <- list(
  list(title = "TABLE 1: Mean Wealth, 1860 (White-Headed Households, Age 18+)", table = table1, digits = 0),
  list(title = "TABLE 2: Median Wealth, 1860 (White-Headed Households, Age 18+)", table = table2, digits = 0),
  list(title = "TABLE 3: Gini Coefficients for Wealth, 1860 (White-Headed Households, Age 18+)", table = table3, digits = 3),
  list(title = "TABLE 4: Incarceration Rates for Adult Men by Race and Region (Age 18+)", table = as.data.frame(table4_data), digits = 3),
  list(title = "TABLE 5a: Occupational Structure, 1860 - Men (%, White-Headed Households, Age 18+)", table = table5_men, digits = 1),
  list(title = "TABLE 5b: Occupational Structure, 1860 - Women (%, White-Headed Households, Age 18+)", table = table5_women, digits = 1),
  list(title = "TABLE 6a: Mean Wealth per Capita by Occupational Category, 1860 - Men (White-Headed Households, Age 18+)", table = table6_men, digits = 0),
  list(title = "TABLE 6b: Mean Wealth per Capita by Occupational Category, 1860 - Women (White-Headed Households, Age 18+)", table = table6_women, digits = 0),
  list(title = "TABLE 7a: Median Wealth per Capita by Occupational Category, 1860 - Men (White-Headed Households, Age 18+)", table = table7_men, digits = 0),
  list(title = "TABLE 7b: Median Wealth per Capita by Occupational Category, 1860 - Women (White-Headed Households, Age 18+)", table = table7_women, digits = 0),
  list(title = "TABLE 8: Household Wealth Distribution by Region (%, White-Headed Households, Age 18+)", table = as.data.frame(table8_data), digits = 1),
  list(title = "TABLE A.1: Wealth Distribution Including Top-coded Values (Complete Dataset)", table = as.data.frame(table_a1_data), digits = 0),
  list(title = "TABLE A.2: Occupations with Most Top-coded Individuals (Complete Dataset)", table = as.data.frame(table_a2_data), digits = 0),
  list(title = "TABLE A.3: Regional Distribution of Top-coded Individuals (Complete Dataset)", table = as.data.frame(table_a3_data), digits = c(0,0,2)),
  list(title = "TABLE A.4: Household Wealth Statistics by Region (South disaggregated, White-Headed Households)", table = table_A4_data_fixed, digits = c(0,0,0,3)), 
  list(title = "TABLE A.5a: Household Wealth Statistics (Farm Household Heads, White-Headed Households)", table = table_A5a_data_fixed, digits = c(0,0,0,3)), 
  list(title = "TABLE A.5b: Household Wealth Statistics (Non-Farm Household Heads, White-Headed Households)", table = table_A5b_data_fixed, digits = c(0,0,0,3)) 
)

# Print all tables to the output file
for (item in tables_to_print) {
  cat("===============================\n")
  cat(item$title, "\n")
  cat("===============================\n")
  if (is.null(item$table) || nrow(item$table) == 0) {
    cat("No data available for this table.\n")
  } else {
    current_table_df <- as.data.frame(item$table)
    
    # Handle special column names for specific tables
    if (item$title == "TABLE A.1: Wealth Distribution Including Top-coded Values (Complete Dataset)") {
      colnames(current_table_df) <- c("Wealth Bracket", "Count", "Pct of Total")
    } else if (item$title == "TABLE A.2: Occupations with Most Top-coded Individuals (Complete Dataset)") { 
      colnames(current_table_df) <- c("Occupation", "Top-coded Count", "Pct of All Top-coded")
    } else if (item$title == "TABLE A.3: Regional Distribution of Top-coded Individuals (Complete Dataset)") { 
      colnames(current_table_df) <- c("Region", "Top-coded Count", "Pct of All Top-coded")
    } else if (grepl("TABLE 8:", item$title)) {
      colnames(current_table_df) <- c("Wealth Bracket", "North (%)", "South (%)")
    } else if (item$title == "TABLE 4: Incarceration Rates for Adult Men by Race and Region (Age 18+)") {
      colnames(current_table_df) <- c("Region", "White (native) (%)", "White (immigrant) (%)", "White (total) (%)", "Black (%)")
    }
    
    # Determine kable digits
    kable_digits <- item$digits
    if (length(item$digits) == 1 && ncol(current_table_df) > 1 && !is.null(colnames(current_table_df)) && colnames(current_table_df)[1] %in% c("Region", "occupation", "Wealth Bracket", "Occupation", "Race Category")) {
      if (length(item$digits) > 1 && length(item$digits) == ncol(current_table_df) -1 ) { 
        kable_digits <- c(0, item$digits) 
      } else if (length(item$digits) == ncol(current_table_df)) {
        # Digits specified for all columns, use as is
      } else { 
        numeric_cols <- sapply(current_table_df, is.numeric)
        temp_digits <- rep(NA, ncol(current_table_df))
        temp_digits[numeric_cols] <- item$digits[1]
        temp_digits[!numeric_cols] <- 0 
        kable_digits <- temp_digits
      }
    } else if (length(item$digits) == 1) {
      kable_digits <- item$digits[1] 
    }
    
    # Adjust digits for specific tables
    if (item$title %in% c("TABLE A.4: Household Wealth Statistics by Region (South disaggregated, White-Headed Households)",
                          "TABLE A.5a: Household Wealth Statistics (Farm Household Heads, White-Headed Households)",
                          "TABLE A.5b: Household Wealth Statistics (Non-Farm Household Heads, White-Headed Households)")) {
      idx_gini <- which(colnames(current_table_df) == "Gini Coefficient")
      idx_mean <- which(colnames(current_table_df) == "Mean Wealth")
      idx_median <- which(colnames(current_table_df) == "Median Wealth")
      
      temp_digits_vec <- rep(0, ncol(current_table_df)) 
      if(length(idx_mean) > 0) temp_digits_vec[idx_mean] <- 0
      if(length(idx_median) > 0) temp_digits_vec[idx_median] <- 0
      if(length(idx_gini) > 0) temp_digits_vec[idx_gini] <- 3
      kable_digits <- temp_digits_vec
    } else if (item$title == "TABLE A.3: Regional Distribution of Top-coded Individuals (Complete Dataset)"){
      idx_pct <- which(colnames(current_table_df) == "Pct of All Top-coded")
      temp_digits_vec <- rep(0, ncol(current_table_df))
      if(length(idx_pct) > 0) temp_digits_vec[idx_pct] <- 2
      kable_digits <- temp_digits_vec
    } else if (grepl("TABLE 8:", item$title)) {
      kable_digits <- c(0, 1, 1)
    } else if (item$title == "TABLE 4: Incarceration Rates for Adult Men by Race and Region (Age 18+)") {
      kable_digits <- c(0, 3, 3, 3, 3)
    }
    
    print(knitr::kable(current_table_df, format = "simple", digits = kable_digits, row.names = FALSE))
  }
  cat("\n\n")
}

sink()
cat("\nAll tables successfully saved to:", output_file_path, "\n")