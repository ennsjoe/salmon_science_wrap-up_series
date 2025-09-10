################################################################################
# Title: Summarize Science Projects Database
# Description: Connects to the SQLite database and summarizes the 'projects' table
################################################################################

# Load libraries
library(here)
library(DBI)
library(RSQLite)
library(dplyr)

# Define path to the database
db_path <- here("science_projects.sqlite")

# Connect to the database
con <- dbConnect(SQLite(), dbname = db_path)

# Check available tables
tables <- dbListTables(con)
cat("📋 Tables in the database:\n")
print(tables)

# If 'projects' table exists, summarize it
if ("projects" %in% tables) {
  # Load data into R
  projects <- dbReadTable(con, "projects")
  
  # Basic summary
  cat("\n🔍 Summary of 'projects' table:\n")
  cat("Number of rows:", nrow(projects), "\n")
  cat("Number of columns:", ncol(projects), "\n")
  cat("Column names:\n")
  print(colnames(projects))
  
  # Show first few rows
  cat("\n📌 Sample rows:\n")
  print(head(projects, 5))
  
  # Optional: summarize a key column (e.g., category or status)
  if ("Category" %in% colnames(projects)) {
    cat("\n📊 Projects by Category:\n")
    print(table(projects$Category))
  }
  
  if ("Status" %in% colnames(projects)) {
    cat("\n📊 Projects by Status:\n")
    print(table(projects$Status))
  }
  
} else {
  cat("\n⚠️ Table 'projects' not found in the database.\n")
}

# Disconnect
dbDisconnect(con)
