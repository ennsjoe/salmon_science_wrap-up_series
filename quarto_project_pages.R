################################################################################
# Title: Load CSVs into SQLite, Clean Column Names & Generate Quarto Pages
# Description: Ingests CSVs, parses dates/times, builds project pages and index
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
library(lubridate)
library(stringr)
library(fs)

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
      presentation_time = case_when(
        str_to_upper(presentation_time) == "AM" ~ "09:00",
        str_to_upper(presentation_time) == "PM" ~ "13:00",
        TRUE ~ presentation_time
      ),
      datetime = suppressWarnings(ymd_hm(paste(presentation_date, presentation_time)))
    )
  
  dbWriteTable(con, "speaker_themes", speaker_themes, overwrite = TRUE)
  
  cat("✅ Table 'speaker_themes' written to database with parsed datetime.\n")
} else {
  cat("⚠️ 'Speaker Themes.csv' not found in data directory.\n")
}

# Load joined project data
required_tables <- c("Science.PSSI.Projects", "project.export..long.", "speaker_themes")
available_tables <- dbListTables(con)
missing_tables <- setdiff(required_tables, available_tables)

if (length(missing_tables) > 0) {
  stop(glue("Missing required tables: {paste(missing_tables, collapse = ', ')}"))
}

overview <- dbReadTable(con, "Science.PSSI.Projects")
details  <- dbReadTable(con, "project.export..long.")
themes   <- dbReadTable(con, "speaker_themes")
dbDisconnect(con)

projects <- overview %>%
  left_join(details, by = "project_id") %>%
  left_join(themes, by = "project_id") %>%
  filter(!is.na(project_id), project_id != "")

# Create output directory
pages_dir <- here("pages")
dir_create(pages_dir)

# Helper: sanitize filenames
sanitize_filename <- function(x) gsub("[^a-zA-Z0-9_-]", "_", x)

# Generate individual .qmd pages
for (i in seq_len(nrow(projects))) {
  row <- projects[i, ]
  file_id <- sanitize_filename(row[["project_id"]])
  if (is.na(file_id) || file_id == "") next
  
  file_path <- file.path(pages_dir, paste0(file_id, ".qmd"))
  
  title    <- ifelse(is.na(row[["title.x"]]), "Untitled Project", row[["title.x"]])
  lead     <- ifelse(is.na(row[["project_leads.x"]]), "N/A", row[["project_leads.x"]])
  division <- ifelse(is.na(row[["division.x"]]), "N/A", row[["division.x"]])
  section  <- ifelse(is.na(row[["section.x"]]), "N/A", row[["section.x"]])
  summary  <- ifelse(is.na(row[["project_overview_jde"]]), "No description available.", row[["project_overview_jde"]])
  pillar   <- ifelse(is.na(row[["pssi_pillar"]]), "Unspecified", row[["pssi_pillar"]])
  speaker_theme <- ifelse(is.na(row[["speaker_themes"]]), "Uncategorized", row[["speaker_themes"]])
  activities <- ifelse(is.na(row[["year_specific_priorities"]]), "Not Listed", row[["year_specific_priorities"]])
  
  presentation <- tryCatch({
    if (is.na(row[["datetime"]])) {
      "TBD"
    } else {
      format(as.POSIXct(row[["datetime"]]), "%B %d, %Y at %I:%M %p")
    }
  }, error = function(e) {
    "TBD"
  })
  
  page_content <- glue(
    "---\n",
    "title: \"{title}\"\n",
    "description: \"{summary}\"\n",
    "author: \"{lead}\"\n",
    "toc: true\n",
    "---\n\n",
    "## 📋 Project Summary\n\n",
    "**Lead(s):** {lead}  \n",
    "**Division:** {division}  \n",
    "**Section:** {section}  \n",
    "**PSSI Pillar:** {pillar}  \n",
    "**Speaker Theme:** {speaker_theme}  \n",
    "**Presentation:** {presentation}  \n\n",
    "**Overview:**  \n{summary}   \n\n",
    "**Activities:**  \n{activities}\n\n",
    "[⬅ Back to Home](../index.qmd)\n"
  )
  
  writeLines(page_content, file_path)
}

# Build index.qmd
cat("🧭 Building index.qmd...\n")

index_md <- c(
  "---",
  'title: "🌊 Pacific Salmon Science Speaker Series"',
  'description: "A curated collection of salmon science projects grouped by speaker themes, supporting salmon conservation and research in British Columbia."',
  'author: "PSSI Implementation Team"',
  'format: html',
  'toc: false',
  "---",
  "",
  "## 🗓️ Upcoming Talks by Theme",
  ""
)

# Load speaker themes again for presentation listing
con <- dbConnect(SQLite(), dbname = db_path)
themes_raw <- dbReadTable(con, "speaker_themes")
dbDisconnect(con)

presentations <- themes_raw %>%
  mutate(
    presentation_date = as.Date(presentation_date),
    presentation_time = case_when(
      str_to_upper(presentation_time) == "AM" ~ "09:00 AM",
      str_to_upper(presentation_time) == "PM" ~ "01:00 PM",
      TRUE ~ presentation_time
    ),
    datetime = paste(format(presentation_date, "%B %d, %Y"), presentation_time)
  ) %>%
  arrange(presentation_date, presentation_time)

presentations_by_theme <- split(presentations, presentations$speaker_themes)

for (theme_name in names(presentations_by_theme)) {
  theme_presentations <- presentations_by_theme[[theme_name]]
  
  index_md <- c(index_md, glue("### {theme_name}"), "")
  
  for (i in seq_len(nrow(theme_presentations))) {
    row <- theme_presentations[i, ]
    session <- row[["session"]]
    presenters <- row[["presenters"]]
    datetime <- row[["datetime"]]
    
    index_md <- c(index_md, glue("- **{datetime}** (Session {session}) – {presenters}"))
  }
  
  index_md <- c(index_md, "")
}

# Group projects by speaker theme
projects_by_theme <- split(projects, projects[["speaker_themes"]])
index_md <- c(index_md, "## 🐟 Salmon Science Projects", "")

for (theme_name in names(projects_by_theme)) {
  theme_projects <- projects_by_theme[[theme_name]] %>%
    distinct(project_id, title.x, .keep_all = TRUE)
  
  index_md <- c(index_md, glue("### {theme_name}"), "")
  
  for (i in seq_len(nrow(theme_projects))) {
    row <- theme_projects[i, ]
    file_id <- sanitize_filename(row[["project_id"]])
    title   <- ifelse(is.na(row[["title.x"]]), "Untitled Project", row[["title.x"]])
    
    index_md <- c(index_md, glue("- [{title}](pages/{file_id}.qmd)"))
  }
  
  index_md <- c(index_md, "")
}

writeLines(index_md, here("index.qmd"))

# Write CNAME file for GitHub Pages custom domain
writeLines("www.pacificsalmonscience.ca", "CNAME")

# Render the Quarto site
system("quarto render")

# Automate Git commit and push
system("git add .")
system("git commit -m \"Auto-update site content\"")
system("git push origin main")

cat("✅ Quarto pages, index.qmd, and verification file generated successfully.\n")
cat("🔗 Remember to check the GitHub repository to ensure changes are reflected.\n")
cat("🌐 Visit https://www.pacificsalmonscience.ca to see the updated site!\n")
