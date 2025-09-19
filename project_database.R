################################################################################
# Title: Load CSVs into SQLite, Clean Column Names & List Table Structures
# Description: Writes all CSVs from 'data/' folder to DB, cleans column names,
#              and prints table schemas. Speaker Themes.csv is handled separately.
################################################################################

# Load libraries
library(here)
library(DBI)
library(RSQLite)
library(dplyr)
library(readr)
library(tools)
library(glue)
library(janitor)
library(stringr)
library(lubridate)

# Define path to the database
db_path <- here("science_projects.sqlite")

# Connect to the database
con <- dbConnect(SQLite(), dbname = db_path)

# Read all CSV files from the 'data' folder
data_dir <- here("data")
csv_files <- list.files(data_dir, pattern = "\\.csv$", full.names = TRUE)

cat("📂 Found CSV files:\n")
print(csv_files)

# Write each CSV to the database with cleaned column names
for (csv_path in csv_files) {
  table_name <- file_path_sans_ext(basename(csv_path)) %>% make.names()
  
  cat(glue("\n📥 Loading '{basename(csv_path)}' into table '{table_name}'...\n"))
  
  data <- read_csv(csv_path, show_col_types = FALSE) %>%
    janitor::clean_names()
  
  dbWriteTable(con, table_name, data, overwrite = TRUE)
  
  cat(glue("✅ Table '{table_name}' written to database with cleaned column names.\n"))
}


# Explicitly handle Speaker Themes.csv
speaker_themes_path <- file.path(data_dir, "Speaker Themes.csv")
if (file.exists(speaker_themes_path)) {
  cat(glue("\n📥 Loading 'Speaker Themes.csv' into table 'speaker_themes'...\n"))
  
  speaker_themes <- read_csv(speaker_themes_path, show_col_types = FALSE) %>%
    janitor::clean_names() %>%
    mutate(
      presentation_date = mdy(presentation_date),
      presentation_time = as.character(presentation_time)
    )
  
  dbWriteTable(con, "speaker_themes", speaker_themes, overwrite = TRUE)
  
  cat("✅ Table 'speaker_themes' written to database with cleaned column names.\n")
} else {
  cat("⚠️ 'Speaker Themes.csv' not found in data directory.\n")
}

# List all tables and their columns
tables <- dbListTables(con)
cat("\n📋 Tables and their columns:\n")

for (table_name in tables) {
  columns <- dbListFields(con, table_name)
  cat(glue("\n🔹 Table: {table_name}\n"))
  cat("   Columns:\n")
  print(columns)
}

# Disconnect
dbDisconnect(con)

# ✅ Generate Quarto pages and project list
cat("\n📝 Generating Quarto pages and project list...\n")
