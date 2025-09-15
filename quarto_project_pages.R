################################################################################
# Title: Generate Quarto Pages and Homepage from Science PSSI Tables
# Description: Joins overview, detail, and theme tables by 'project_id',
#              generates individual project pages, and builds full index.qmd
################################################################################

# Load libraries
library(here)
library(DBI)
library(RSQLite)
library(dplyr)
library(fs)
library(glue)
library(janitor)

# Connect to SQLite database
db_path <- here("science_projects.sqlite")
con <- dbConnect(SQLite(), dbname = db_path)

# Validate required tables
required_tables <- c("Science.PSSI.Projects", "project.export..long.", "speaker_themes")
available_tables <- dbListTables(con)
missing_tables <- setdiff(required_tables, available_tables)

if (length(missing_tables) > 0) {
  stop(glue("❌ Missing required tables: {paste(missing_tables, collapse = ', ')}"))
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

# Create output directory for pages
pages_dir <- here("pages")
dir_create(pages_dir)

# Helper: sanitize filenames
sanitize_filename <- function(x) gsub("[^a-zA-Z0-9_-]", "_", x)

# Generate individual .qmd pages
cat("📄 Generating project pages...\n")
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
  theme    <- ifelse(is.na(row[["speaker_themes"]]), "Uncategorized", row[["speaker_themes"]])
  activities <- ifelse(is.na(row[["year_specific_priorities"]]), "Not Listed", row[["year_specific_priorities"]])
  
  page_content <- glue(
    "---\n",
    "title: \"{title}\"\n",
    "description: \"{summary}\"\n",
    "author: \"{lead}\"\n",
    "keywords: [\"salmon science\", \"{pillar}\", \"{theme}\", \"{division}\", \"{section}\"]\n",
    "---\n\n",
    "## 🧬 Project Summary\n\n",
    "**Lead(s):** {lead}  \n",
    "**Division:** {division}  \n",
    "**Section:** {section}  \n",
    "**PSSI Pillar:** {pillar}  \n",
    "**Speaker Theme:** {theme}  \n\n",
    "**Overview:**  \n{summary}   \n\n",
    "**Activities:**  \n{activities}\n\n",
    "[⬅ Back to Home](../index.qmd)\n"
  )
  
  writeLines(page_content, file_path)
}

# Group projects by theme
projects_by_theme <- split(projects, projects[["speaker_themes"]])

# Build full index.qmd
cat("🧭 Building index.qmd...\n")
index_md <- c(
  "---",
  'title: "Pacific Salmon Science Speaker Series"',
  'description: "A curated collection of salmon science projects grouped by speaker themes..."',
  'author: "Pacific Salmon Science Initiative"',
  'format: html',
  'page-layout: custom',
  "---",
  "",
  '::: {.panel-sidebar}',
  "## 🗓️ Upcoming Talks",
  "",
  "- **Sept 20, 2025** – Dr. Jane Salmon: *Migration Talk*",
  "- **Sept 22, 2025** – Dr. Alex River: *Sockeye Genetics*",
  "- **Sept 25, 2025** – Dr. Maya Stream: *Habitat Restoration*",
  "- **Oct 2, 2025** – Dr. Leo Waters: *Climate Impacts on Salmon*",
  "- **Oct 9, 2025** – Dr. Nina Estuary: *Estuarine Ecology*",
  "",
  "Explore the full project list grouped by speaker themes below.",
  ":::",
  "",
  "## 🐟 Salmon Science Projects",
  "",
  "Explore projects grouped by speaker theme below.",
  ""
)

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

cat("✅ Site build complete: index.qmd and project pages generated.\n")
