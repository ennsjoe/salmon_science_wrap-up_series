################################################################################
# Title: Generate Quarto Pages from Joined Science PSSI Tables
# Description: Joins overview, detail, and theme tables by 'project_id' and builds site
################################################################################

library(here)
library(DBI)
library(RSQLite)
library(dplyr)
library(fs)
library(glue)

# Connect to SQLite database
db_path <- here("science_projects.sqlite")
con <- dbConnect(SQLite(), dbname = db_path)

# Check required tables
required_tables <- c("Science.PSSI.Projects", "project.export..long.", "speaker_themes")
available_tables <- dbListTables(con)
missing_tables <- setdiff(required_tables, available_tables)

if (length(missing_tables) > 0) {
  stop(glue("Missing required tables: {paste(missing_tables, collapse = ', ')}"))
}

# Load tables
overview <- dbReadTable(con, "Science.PSSI.Projects")
details  <- dbReadTable(con, "project.export..long.")
themes   <- dbReadTable(con, "speaker_themes")
dbDisconnect(con)

# Join and filter
projects <- overview %>%
  left_join(details, by = "project_id") %>%
  left_join(themes, by = "project_id") %>%
  filter(!is.na(project_id), project_id != "")

# Create output directory
pages_dir <- here("pages")
dir_create(pages_dir)

# Helper: sanitize filenames
sanitize_filename <- function(x) gsub("[^a-zA-Z0-9_-]", "_", x)

# Generate individual .qmd pages with SEO metadata
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
  speaker_themes <- ifelse(is.na(row[["speaker_themes"]]), "Uncategorized", row[["speaker_themes"]])
  activities <- ifelse(is.na(row[["year_specific_priorities"]]), "Not Listed", row[["year_specific_priorities"]])
  
  page_content <- glue(
    "---\n",
    "title: \"{title}\"\n",
    "description: \"{summary}\"\n",
    "author: \"{lead}\"\n",
    "keywords: [\"salmon science\", \"{pillar}\", \"{speaker_themes}\", \"{division}\", \"{section}\"]\n",
    "---\n\n",
    "## 🧬 Project Summary\n\n",
    "**Lead(s):** {lead}  \n",
    "**Division:** {division}  \n",
    "**Section:** {section}  \n",
    "**PSSI Pillar:** {pillar}  \n",
    "**Speaker Theme:** {speaker_themes}  \n\n",
    "**Overview:**  \n{summary}   \n\n",
    "**Activities:**  \n{activities}\n\n",
    "[⬅ Back to Home](../index.qmd)\n"
  )
  
  writeLines(page_content, file_path)
}

# Group by speaker_themes
if (!"speaker_themes" %in% colnames(projects)) {
  print(colnames(projects))
  stop("Column 'speaker_themes' not found.")
}

projects_by_theme <- split(projects, projects[["speaker_themes"]])

# Write dynamic project list to separate markdown file
project_list_md <- c(
  "## 🐟 Salmon Science Projects",
  "",
  "Explore projects grouped by speaker theme below.",
  ""
)

for (theme_name in names(projects_by_theme)) {
  theme_projects <- projects_by_theme[[theme_name]] %>%
    distinct(project_id, title.x, .keep_all = TRUE)
  
  project_list_md <- c(project_list_md, glue("### {theme_name}"), "")
  
  for (i in seq_len(nrow(theme_projects))) {
    row <- theme_projects[i, ]
    file_id <- sanitize_filename(row[["project_id"]])
    title   <- ifelse(is.na(row[["title.x"]]), "Untitled Project", row[["title.x"]])
    
    project_list_md <- c(project_list_md, glue("- [{title}](pages/{file_id}.qmd)"))
  }
  
  project_list_md <- c(project_list_md, "")
}

writeLines(project_list_md, here("project_list.md"))

cat("✅ Quarto pages and project list markdown generated successfully.\n")

