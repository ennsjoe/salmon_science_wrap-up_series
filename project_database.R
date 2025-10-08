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
library(data.table)

# 🗂️ Define path to the database
db_path <- here("science_projects.sqlite")

# 🔌 Connect to the database
con <- dbConnect(SQLite(), dbname = db_path)

# 📁 Read all CSV files from the 'data' folder
data_dir <- here("data")
csv_files <- list.files(data_dir, pattern = "\\.csv$", full.names = TRUE)

# 📤 Write each CSV to the database with cleaned column names
for (csv_path in csv_files) {
  file_name <- basename(csv_path)
  table_name <- file_path_sans_ext(file_name) %>% make.names()
  
  cat(glue("\n📥 Loading '{file_name}' into table '{table_name}'...\n"))
  
  tryCatch({
    # Read CSV and clean column names
    data <- fread(csv_path) %>%
      janitor::clean_names()
    
    # Coerce key columns
    if ("project_id" %in% names(data)) {
      data[, project_id := as.character(project_id)]
    }
    if ("session" %in% names(data)) {
      data[, session := as.character(session)]
    }
    
    # Generalized date parsing
    date_cols <- grep("date", names(data), ignore.case = TRUE, value = TRUE)
    for (col in date_cols) {
      if (inherits(data[[col]], "Date")) {
        next
      } else if (is.numeric(data[[col]])) {
        if (all(data[[col]] > 20000 & data[[col]] < 50000, na.rm = TRUE)) {
          data[[col]] <- as.Date(data[[col]], origin = "1970-01-01")
        }
      } else {
        parsed <- suppressWarnings(parse_date_time(data[[col]], orders = c("mdy", "ymd", "dmy")))
        data[[col]] <- parsed
      }
    }
    
    # Write to database
    dbWriteTable(con, table_name, as.data.frame(data), overwrite = TRUE)
    
    cat(glue("✅ Table '{table_name}' written to database.\n"))
  }, error = function(e) {
    cat(glue("❌ Error loading '{file_name}': {e$message}\n"))
  })
}

# 📊 Summarize table structures
tables <- dbListTables(con)

cat("Summary of tables in science_projects.sqlite:\n\n")
for (tbl in tables) {
  row_count <- dbGetQuery(con, glue("SELECT COUNT(*) AS count FROM \"{tbl}\""))$count
  schema <- dbGetQuery(con, glue("PRAGMA table_info(\"{tbl}\")"))
  
  cat(glue("Table: {tbl}\n"))
  cat(glue("Row count: {row_count}\n"))
  cat("Columns:\n")
  for (i in seq_len(nrow(schema))) {
    cat(glue("  - {schema$name[i]} ({schema$type[i]})\n"))
  }
  cat("\n-----------------------------\n\n")
}

# 🔌 Disconnect from the database
dbDisconnect(con)
