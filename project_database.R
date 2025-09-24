################################################################################
# Title: Load CSVs into SQLite, Clean Column Names & List Table Structures
# Description: Ingests all CSVs from 'data/' folder into SQLite DB, cleans
#              column names, parses dates where applicable, and prints schemas.
################################################################################

# 📦 Load libraries
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

# 🗂️ Define path to the database
db_path <- here("science_projects.sqlite")

# 🔌 Connect to the database
con <- dbConnect(SQLite(), dbname = db_path)

# 📁 Read all CSV files from the 'data' folder
data_dir <- here("data")
csv_files <- list.files(data_dir, pattern = "\\.csv$", full.names = TRUE)

cat("📂 Found CSV files:\n")
print(csv_files)

# 📤 Write each CSV to the database with cleaned column names
for (csv_path in csv_files) {
  file_name <- basename(csv_path)
  table_name <- file_path_sans_ext(file_name) %>% make.names()
  
  cat(glue("\n📥 Loading '{file_name}' into table '{table_name}'...\n"))
  
  tryCatch({
    # Read and clean
    data <- read_csv(csv_path, show_col_types = FALSE) %>%
      janitor::clean_names()
    
    # Coerce key columns to character if present
    if ("project_id" %in% names(data)) {
      data <- data %>% mutate(project_id = as.character(project_id))
    }
    if ("session" %in% names(data)) {
      data <- data %>% mutate(session = as.character(session))
    }
    
    if (tolower(file_name) == "session_info.csv" && "date" %in% names(data)) {
      if (is.numeric(data$date)) {
        # Excel-style serial numbers
        data <- data %>%
          mutate(date = as.Date(date, origin = "1899-12-30"))
      } else {
        # Text-formatted dates like "12/03/2025"
        data <- data %>%
          mutate(date = mdy(date))
      }
    }
    
    # Write to database
    dbWriteTable(con, table_name, data, overwrite = TRUE)
    
    cat(glue("✅ Table '{table_name}' written to database.\n"))
  }, error = function(e) {
    cat(glue("❌ Error loading '{file_name}': {e$message}\n"))
  })
}

# 📋 List all tables and their columns
tables <- dbListTables(con)
cat("\n📋 Tables and their columns:\n")

for (table_name in tables) {
  columns <- dbListFields(con, table_name)
  cat(glue("\n🔹 Table: {table_name}\n"))
  cat("   Columns:\n")
  print(columns)
}

# 🔌 Disconnect from the database
dbDisconnect(con)

# 📝 Ready for Quarto page generation
cat("\n📝 Quarto page generation and indexing can now begin...\n")
