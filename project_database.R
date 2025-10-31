################################################################################
# Title: Load CSVs into SQLite, Clean Column Names & List Table Structures
# Description: Ingests all CSVs from 'data/' folder into SQLite DB, cleans
#              column names, parses dates where applicable, and prints schemas.
#              Also loads PDFs from data/PSSI_bulletin/ into database.
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

# 📄 Load PDFs from data/PSSI_bulletin/ folder
cat("\n📄 Loading PDFs from PSSI_bulletin folder...\n")
pdf_dir <- here("data", "PSSI_bulletin")

if (dir.exists(pdf_dir)) {
  pdf_files <- list.files(pdf_dir, pattern = "\\.pdf$", full.names = TRUE)
  
  if (length(pdf_files) > 0) {
    # Create table for PDFs
    pdf_data_list <- list()
    
    for (pdf_path in pdf_files) {
      file_name <- basename(pdf_path)
      # Extract project_id from filename (e.g., "2493.pdf" -> "2493")
      project_id <- file_path_sans_ext(file_name)
      
      tryCatch({
        # Read PDF as binary
        pdf_binary <- readBin(pdf_path, "raw", file.info(pdf_path)$size)
        
        # Store in list
        pdf_data_list[[length(pdf_data_list) + 1]] <- data.frame(
          project_id = project_id,
          pdf_data = I(list(pdf_binary)),
          stringsAsFactors = FALSE
        )
        
        cat(glue("  ✓ Loaded PDF for project {project_id}\n"))
      }, error = function(e) {
        cat(glue("  ❌ Error loading '{file_name}': {e$message}\n"))
      })
    }
    
    # Combine all PDF data
    if (length(pdf_data_list) > 0) {
      pdf_table <- bind_rows(pdf_data_list)
      
      # Write to database
      dbWriteTable(con, "PSSI_bulletins", pdf_table, overwrite = TRUE)
      
      cat(glue("✅ Loaded {nrow(pdf_table)} PDFs into 'PSSI_bulletins' table\n"))
    }
  } else {
    cat("⚠️  No PDF files found in PSSI_bulletin folder\n")
  }
} else {
  cat("⚠️  PSSI_bulletin folder not found at: {pdf_dir}\n")
  cat("   Creating empty PSSI_bulletins table...\n")
  
  # Create empty table structure
  empty_pdf_table <- data.frame(
    project_id = character(),
    pdf_data = I(list()),
    stringsAsFactors = FALSE
  )
  dbWriteTable(con, "PSSI_bulletins", empty_pdf_table, overwrite = TRUE)
}

# Load banner images from data/ folder
cat("\nLoading banner images from data folder...\n")
banner_files <- list.files(data_dir, pattern = "\\.(png|jpg|jpeg)$", 
                           full.names = TRUE, ignore.case = TRUE)

if (length(banner_files) > 0) {
  banner_data_list <- list()
  
  for (banner_path in banner_files) {
    file_name <- basename(banner_path)
    
    tryCatch({
      # Read image as binary
      image_binary <- readBin(banner_path, "raw", file.info(banner_path)$size)
      
      # Get file extension
      file_ext <- tolower(tools::file_ext(file_name))
      
      # Store in list
      banner_data_list[[length(banner_data_list) + 1]] <- data.frame(
        file_name = file_name,
        file_type = file_ext,
        image_data = I(list(image_binary)),
        upload_date = as.character(Sys.Date()),
        file_size = file.info(banner_path)$size,
        stringsAsFactors = FALSE
      )
      
      cat(glue("  Success: Loaded banner image: {file_name}\n"))
    }, error = function(e) {
      cat(glue("  Error loading '{file_name}': {e$message}\n"))
    })
  }
  
  # Combine all banner data
  if (length(banner_data_list) > 0) {
    banner_table <- bind_rows(banner_data_list)
    
    # Write to database
    dbWriteTable(con, "banner_images", banner_table, overwrite = TRUE)
    
    cat(glue("Success: Loaded {nrow(banner_table)} banner image(s) into 'banner_images' table\n"))
  }
} else {
  cat("Warning: No banner images found in data folder\n")
  cat("   Creating empty banner_images table...\n")
  
  # Create empty table structure
  empty_banner_table <- data.frame(
    file_name = character(),
    file_type = character(),
    image_data = I(list()),
    upload_date = character(),
    file_size = numeric(),
    stringsAsFactors = FALSE
  )
  dbWriteTable(con, "banner_images", empty_banner_table, overwrite = TRUE)
}

# 📊 Summarize table structures
tables <- dbListTables(con)

cat("\n📋 Summary of tables in science_projects.sqlite:\n\n")
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

cat("✨ Database build complete!\n")
