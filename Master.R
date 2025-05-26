# Set the working directory to the script’s location
setwd(getSrcDirectory(function(dummy) {dummy}))

# Clear console
cat("\014")

source("Download_IPUMS.R")
source("Wealth.R")
source("Download_NHGIS.R")
source("Spatial_RDD.R")