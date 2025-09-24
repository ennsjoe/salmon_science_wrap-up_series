################################################################################
# Title: Query SQLite Tables & Generate Quarto Pages
# Description: Connects to SQLite, validates tables, joins project data,
#              and builds Quarto pages and index.
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

# 📌 Validate required tables
required_tables <- c("Science.PSSI.Projects", "project.export..long.", "Speaker.Themes", "session_info")
available_tables <- dbListTables(con)
missing_tables <- setdiff(required_tables, available_tables)

if (length(missing_tables) > 0) {
  stop(glue("❌ Missing required tables: {paste(missing_tables, collapse = ', ')}"))
}

# 📦 Load and coerce key columns
overview <- dbReadTable(con, "Science.PSSI.Projects") %>%
  mutate(project_id = as.character(project_id), session = as.character(session))

details <- dbReadTable(con, "project.export..long.") %>%
  mutate(project_id = as.character(project_id), session = as.character(session))

themes <- dbReadTable(con, "Speaker.Themes") %>%
  mutate(project_id = as.character(project_id), session = as.character(session))

sessions <- dbReadTable(con, "session_info") %>%
  mutate(session = as.character(session), date = as.Date(date))

# 🔌 Disconnect from the database
dbDisconnect(con)

# 🔗 Join all project data
projects <- overview %>%
  left_join(details, by = "project_id") %>%
  left_join(themes, by = "project_id") %>%
  left_join(sessions, by = "session") %>%
  filter(!is.na(project_id), project_id != "")

# 📂 Create output directory
pages_dir <- here("pages")
dir_create(pages_dir)

# 🧼 Helper: sanitize filenames
sanitize_filename <- function(x) gsub("[^a-zA-Z0-9_-]", "_", x)

################################################################################
# 📝 Generate individual .qmd pages
for (i in seq_len(nrow(projects))) {
  row <- projects[i, ]
  file_id <- sanitize_filename(row[["project_id"]])
  if (is.na(file_id) || file_id == "") next
  
  file_path <- file.path(pages_dir, paste0(file_id, ".qmd"))
  
  title     <- row[["title.x"]] %||% "Untitled Project"
  lead      <- row[["project_leads.x"]] %||% "N/A"
  division  <- row[["division.x"]] %||% "N/A"
  section   <- row[["section.x"]] %||% "N/A"
  summary   <- row[["project_overview_jde"]] %||% "No description available."
  pillar    <- row[["pssi_pillar"]] %||% "Unspecified"
  session   <- row[["session"]] %||% "Uncategorized"
  activities <- row[["year_specific_priorities"]] %||% "Not Listed"
  
  presentation <- tryCatch({
    if (is.na(row[["date"]])) {
      "TBD"
    } else {
      format(as.Date(row[["date"]]), "%B %d, %Y")
    }
  }, error = function(e) {
    "TBD"
  })
  
  presenters <- row[["speakers"]] %||% "Presenters TBD"
  hosts      <- row[["hosts"]] %||% "Hosts TBD"
  
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
    "**Session:** {session}  \n",
    "**Presentation Date:** {presentation}  \n",
    "**Speakers:** {presenters}  \n",
    "**Hosts:** {hosts}  \n\n",
    "**Overview:**  \n{summary}   \n\n",
    "**Activities:**  \n{activities}\n\n",
    "[⬅ Back to Home](../index.qmd)\n"
  )
  
  writeLines(page_content, file_path)
}

################################################################################
# 🧭 Build index.qmd grouped by date and session
cat("🧭 Building index.qmd grouped by date and session...\n")

# 🔄 Reconnect to database to fetch latest session info and project metadata
con <- dbConnect(SQLite(), dbname = db_path)
sessions <- dbReadTable(con, "session_info") %>%
  mutate(
    session = as.character(session),
    hosts = as.character(hosts),
    date = mdy(date)
  )
overview <- dbReadTable(con, "Science.PSSI.Projects") %>%
  mutate(project_id = as.character(project_id), session = as.character(session))
dbDisconnect(con)

# 🔗 Join sessions to projects
session_projects <- sessions %>%
  left_join(overview, by = "session") %>%
  filter(!is.na(project_id), project_id != "") %>%
  mutate(
    presentation_date = as.Date(date),
    file_id = sanitize_filename(project_id),
    project_link = ifelse(
      !is.na(title),
      glue("[{title}](pages/{file_id}.qmd)"),
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
presentations_by_date <- split(session_projects, session_projects$presentation_date)

# 🧩 Build index content by date and session
for (date_key in sort(names(presentations_by_date))) {
  date_presentations <- presentations_by_date[[date_key]]
  formatted_date <- format(as.Date(date_key), "%B %d, %Y")
  
  index_md <- c(index_md, glue("## 📅 {formatted_date}"), "")
  
  # 🎨 Group by session
  date_presentations <- date_presentations %>%
    group_by(session) %>%
    group_split()
  
  for (group in date_presentations) {
    session_title <- unique(group$session)
    host_names <- unique(group$hosts) %||% "Hosts TBD"
    
    index_md <- c(index_md, glue("### 🐟 {session_title}"), glue("_Hosted by: {host_names}_"), "")
    
    for (i in seq_len(nrow(group))) {
      row <- group[i, ]
      project_link <- row$project_link
      presenters <- row$project_leads %||% "Presenters TBD"
      
      index_md <- c(index_md, glue("- {project_link} | {presenters}"))
    }
    
    index_md <- c(index_md, "")
  }
}

# 📝 Write index.qmd to disk
writeLines(index_md, here("index.qmd"))

# 🌐 Write CNAME file for GitHub Pages custom domain
writeLines("www.pacificsalmonscience.ca", "CNAME")

# 🚀 Render the Quarto site and push to GitHub
system("quarto render")
system("git add .")
system("git commit -m \"Debugging date formatting\"")
system("git push origin main")
