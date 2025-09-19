################################################################################
# Title: Load CSVs into SQLite, Clean Column Names & Generate Quarto Pages
# Description: Ingests CSVs, parses dates, joins project data, and builds pages
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
library(fs)

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
  table_name <- file_path_sans_ext(basename(csv_path)) %>% make.names()
  
  cat(glue("\n📥 Loading '{basename(csv_path)}' into table '{table_name}'...\n"))
  
  tryCatch({
    data <- read_csv(csv_path, show_col_types = FALSE) %>%
      janitor::clean_names()
    
    dbWriteTable(con, table_name, data, overwrite = TRUE)
    
    cat(glue("✅ Table '{table_name}' written to database with cleaned column names.\n"))
  }, error = function(e) {
    cat(glue("❌ Error loading '{basename(csv_path)}': {e$message}\n"))
  })
}

# 🎯 Explicitly handle 'Speaker Themes.csv'
speaker_themes_path <- file.path(data_dir, "Speaker Themes.csv")
if (file.exists(speaker_themes_path)) {
  cat(glue("\n📥 Loading 'Speaker Themes.csv' into table 'speaker_themes'...\n"))
  
  tryCatch({
    speaker_themes <- read_csv(speaker_themes_path, show_col_types = FALSE) %>%
      janitor::clean_names() %>%
      mutate(
        presentation_date = mdy(presentation_date),
        datetime = presentation_date  # Use date only
      )
    
    dbWriteTable(con, "speaker_themes", speaker_themes, overwrite = TRUE)
    
    cat("✅ Table 'speaker_themes' written to database with parsed presentation_date.\n")
  }, error = function(e) {
    cat(glue("❌ Error processing 'Speaker Themes.csv': {e$message}\n"))
  })
} else {
  cat("⚠️ 'Speaker Themes.csv' not found in data directory.\n")
}

# 📌 Load and join project data
required_tables <- c("Science.PSSI.Projects", "project.export..long.", "speaker_themes")
available_tables <- dbListTables(con)
missing_tables <- setdiff(required_tables, available_tables)

if (length(missing_tables) > 0) {
  stop(glue("❌ Missing required tables: {paste(missing_tables, collapse = ', ')}"))
}

overview <- dbReadTable(con, "Science.PSSI.Projects")
details  <- dbReadTable(con, "project.export..long.")
themes   <- dbReadTable(con, "speaker_themes")
dbDisconnect(con)

projects <- overview %>%
  left_join(details, by = "project_id") %>%
  left_join(themes, by = "project_id") %>%
  filter(!is.na(project_id), project_id != "")

# 📂 Create output directory
pages_dir <- here("pages")
dir_create(pages_dir)

# 🧼 Helper: sanitize filenames
sanitize_filename <- function(x) gsub("[^a-zA-Z0-9_-]", "_", x)

# 📝 Generate individual .qmd pages
for (i in seq_len(nrow(projects))) {
  row <- projects[i, ]
  file_id <- sanitize_filename(row[["project_id"]])
  if (is.na(file_id) || file_id == "") next
  
  file_path <- file.path(pages_dir, paste0(file_id, ".qmd"))
  
  title    <- row[["title.x"]] %||% "Untitled Project"
  lead     <- row[["project_leads.x"]] %||% "N/A"
  division <- row[["division.x"]] %||% "N/A"
  section  <- row[["section.x"]] %||% "N/A"
  summary  <- row[["project_overview_jde"]] %||% "No description available."
  pillar   <- row[["pssi_pillar"]] %||% "Unspecified"
  speaker_theme <- row[["speaker_themes"]] %||% "Uncategorized"
  activities <- row[["year_specific_priorities"]] %||% "Not Listed"
  
  presentation <- tryCatch({
    if (is.na(row[["datetime"]])) {
      "TBD"
    } else {
      format(as.Date(row[["datetime"]]), "%B %d, %Y")
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

# 🧭 Build index.qmd grouped by date and theme
cat("🧭 Building index.qmd grouped by date and theme...\n")

# 🔄 Reconnect to database to fetch latest speaker themes
con <- dbConnect(SQLite(), dbname = db_path)
themes_raw <- dbReadTable(con, "speaker_themes")
dbDisconnect(con)

# 📌 Prepare distinct project titles
project_titles <- projects %>%
  select(project_id, title.x) %>%
  distinct(project_id, .keep_all = TRUE)

# 🧠 Prepare presentation metadata
presentations <- themes_raw %>%
  mutate(
    presentation_date = as.Date(presentation_date)
  ) %>%
  group_by(project_id, speaker_themes, presentation_date) %>%
  summarise(
    presenters = paste(unique(presenters), collapse = ", "),
    .groups = "drop"
  ) %>%
  distinct(project_id, speaker_themes, presentation_date, .keep_all = TRUE) %>%
  left_join(project_titles, by = "project_id") %>%
  mutate(
    file_id = sanitize_filename(project_id),
    project_link = ifelse(
      !is.na(title.x),
      glue("[{title.x}](pages/{file_id}.qmd)"),
      "Untitled Project"
    )
  ) %>%
  arrange(presentation_date)

# 🧱 Initialize index content
index_md <- c(
  "---",
  'title: "🌊 Pacific Salmon Science Speaker Series"',
  'description: "40+ online presentations, 8 sessions over 4 days, reporting on the results of the most current salmon science research through the PSSI and BCSRIF programs."',
  'author: "PSSI Implementation Team"',
  'format: html',
  'toc: false',
  "---",
  "",
  "## 🗓️ Upcoming Talks by Date",
  ""
)

# 📆 Group presentations by date
presentations_by_date <- split(presentations, presentations$presentation_date)

# 🧩 Build index content by date and theme
for (date_key in sort(names(presentations_by_date))) {
  date_presentations <- presentations_by_date[[date_key]]
  formatted_date <- format(as.Date(date_key), "%B %d, %Y")
  
  index_md <- c(index_md, glue("## 📅 {formatted_date}"), "")
  
  # 🎨 Group by theme
  date_presentations <- date_presentations %>%
    mutate(theme_session = glue("{speaker_themes}")) %>%
    group_by(theme_session) %>%
    group_split()
  
  for (group in date_presentations) {
    theme_session <- unique(group$theme_session)
    index_md <- c(index_md, glue("### 🐟 {theme_session}"), "")
    
    for (i in seq_len(nrow(group))) {
      row <- group[i, ]
      project_link <- row$project_link
      presenters <- row$presenters %||% "Presenters TBD"
      
      index_md <- c(index_md, glue("- {project_link} | {presenters}"))
    }
    
    index_md <- c(index_md, "")
  }
}

# 📝 Write index.qmd to disk
writeLines(index_md, here("index.qmd"))

# Write CNAME file for GitHub Pages custom domain
writeLines("www.pacificsalmonscience.ca", "CNAME")

# Render the Quarto site
system("quarto render")

# Automate Git commit and push
system("git add .")
system("git commit -m \"Removed session column\"")
system("git push origin main")

cat("✅ Quarto pages, index.qmd, and verification file generated successfully.\n")
cat("🔗 Remember to check the GitHub repository to ensure changes are reflected.\n")
cat("🌐 Visit https://www.pacificsalmonscience.ca to see the updated site!\n")
