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
Speaker.Themes <- dbReadTable(con, "Speaker.Themes")
bcsrif_projects <- dbReadTable(con, "BCSRIF.Project.List.September.2025") %>%
  janitor::clean_names() %>%
  rename(project_id = project_number) %>%
  mutate(project_id = as.character(project_id))
available_tables <- dbListTables(con)
missing_tables <- setdiff(required_tables, available_tables)

if (length(missing_tables) > 0) {
  stop(glue("❌ Missing required tables: {paste(missing_tables, collapse = ', ')}"))
}

# 🧼 Helper: normalize session names
normalize_session <- function(x) {
  x %>%
    tolower() %>%
    str_replace_all("[^a-z0-9]+", " ") %>%
    str_squish()
}

sanitize_filename <- function(x) {
  gsub("[^a-zA-Z0-9_-]", "_", x)
}

# 📥 Load and clean tables
projects <- dbReadTable(con, "Science.PSSI.Projects") %>%
  mutate(project_id = as.character(project_id))

speakers <- dbReadTable(con, "Speaker.Themes") %>%
  mutate(
    project_id = as.character(project_id),
    session = normalize_session(session)
  )

sessions_raw <- dbReadTable(con, "session_info")

print(head(sessions_raw$date))
str(sessions_raw$date)

# 🧠 Detect and parse date format
sessions <- sessions_raw %>%
  mutate(
    session = normalize_session(session),
    date = as.Date(date, origin = "1899-12-30")  # Converts 46001 → "2025-12-08"
  )

# 🔍 Check for session mismatches
mismatched_sessions <- anti_join(speakers, sessions, by = "session") %>% distinct(session)
if (nrow(mismatched_sessions) > 0) {
  cat("⚠️ Mismatched session names:\n")
  print(mismatched_sessions)
}

# 🔗 Join speakers to sessions
speaker_sessions <- speakers %>%
  left_join(sessions, by = "session")

# 🔗 Join to project metadata
session_projects <- speaker_sessions %>%
  left_join(projects, by = "project_id") %>%
  filter(!is.na(project_id), project_id != "") %>%
  mutate(
    presentation_date = date,
    file_id = sanitize_filename(project_id),
    project_link = ifelse(
      !is.na(title),
      glue("[{title}](pages/{file_id}.qmd)"),
      "Untitled Project"
    )
  ) %>%
  arrange(presentation_date)

session_projects <- session_projects %>%
  left_join(bcsrif_projects, by = "project_id")

#------------------------------
sessions_raw <- dbReadTable(con, "session_info")
print(sessions_raw$date)
str(sessions_raw$date)
#-------------------------------------

# 🔌 Disconnect from the database
dbDisconnect(con)

# 📂 Create output directory
pages_dir <- here("pages")
dir_create(pages_dir)

# 🧼 Helper: sanitize filenames
sanitize_filename <- function(x) gsub("[^a-zA-Z0-9_-]", "_", x)

# 🎯 Filter to speaker series projects only
speaker_projects <- session_projects %>%
  filter(project_id %in% Speaker.Themes$project_id)

# 🧠 Aggregate all metadata by project_id
aggregated_projects <- speaker_projects %>%
  group_by(project_id) %>%
  summarise(
    title = first(title),
    project_name = first(project_name),
    project_leads = paste(unique(na.omit(project_leads)), collapse = "; "),
    recipient = paste(unique(na.omit(recipient)), collapse = "; "),
    division = paste(unique(na.omit(division)), collapse = "; "),
    section = paste(unique(na.omit(section)), collapse = "; "),
    project_overview_jde = first(project_overview_jde),
    description_short = first(description_short),
    pssi_pillar = paste(unique(na.omit(pssi_pillar)), collapse = "; "),
    program_pillar = paste(unique(na.omit(program_pillar)), collapse = "; "),
    session = paste(unique(na.omit(session)), collapse = "; "),
    year_specific_priorities = if ("year_specific_priorities" %in% names(speaker_projects)) paste(unique(na.omit(year_specific_priorities)), collapse = "; ") else "Not Listed",
    speakers = paste(unique(na.omit(speakers)), collapse = "; "),
    hosts = paste(unique(na.omit(hosts)), collapse = "; "),
    presentation_date = paste(unique(format(presentation_date, "%B %d, %Y")), collapse = "; "),
    species_group = paste(unique(na.omit(species_group)), collapse = "; "),
    location_of_project = paste(unique(na.omit(location_of_project)), collapse = "; "),
    agreement_start_date = first(agreement_start_date),
    agreement_end_date = first(agreement_end_date),
    list_of_partners_or_collaborators = paste(unique(na.omit(list_of_partners_or_collaborators)), collapse = "; "),
    .groups = "drop"
  )

# 📝 Generate individual .qmd pages for each speaker project
for (i in seq_len(nrow(aggregated_projects))) {
  row <- aggregated_projects[i, ]
  file_id <- sanitize_filename(row[["project_id"]])
  if (is.na(file_id) || file_id == "") next
  
  file_path <- file.path(pages_dir, paste0(file_id, ".qmd"))
  
  # PSSI fields
  title       <- row[["title"]] %||% row[["project_name"]] %||% "Untitled Project"
  lead        <- row[["project_leads"]] %||% row[["recipient"]] %||% "N/A"
  division    <- row[["division"]] %||% "N/A"
  section     <- row[["section"]] %||% "N/A"
  summary     <- row[["project_overview_jde"]] %||% row[["description_short"]] %||% "No description available."
  pillar      <- row[["pssi_pillar"]] %||% row[["program_pillar"]] %||% "Unspecified"
  session     <- row[["session"]] %||% "Uncategorized"
  activities  <- row[["year_specific_priorities"]] %||% "Not Listed"
  presenters  <- row[["speakers"]] %||% "Presenters TBD"
  hosts       <- row[["hosts"]] %||% "Hosts TBD"
  
  # BCSRIF fields
  species     <- row[["species_group"]] %||% "Not specified"
  location    <- row[["location_of_project"]] %||% "Unknown"
  partners    <- row[["list_of_partners_or_collaborators"]] %||% "None listed"
  start_date  <- row[["agreement_start_date"]]
  end_date    <- row[["agreement_end_date"]]
  
  # Format agreement dates
  start_fmt <- if (!is.na(start_date)) format(as.Date(start_date, origin = "1970-01-01"), "%B %d, %Y") else "TBD"
  end_fmt   <- if (!is.na(end_date)) format(as.Date(end_date, origin = "1970-01-01"), "%B %d, %Y") else "TBD"
  
  # 📝 Compose page content
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
    "**Session(s):** {session}  \n",
    "**Presentation Date(s):** {row[['presentation_date']]}  \n",
    "**Speakers:** {presenters}  \n",
    "**Hosts:** {hosts}  \n\n",
    "**Overview:**  \n{summary}   \n\n",
    "**Activities:**  \n{activities}\n\n",
    "## 🧬 BCSRIF Metadata\n\n",
    "**Species Group:** {species}  \n",
    "**Location:** {location}  \n",
    "**Agreement Period:** {start_fmt} to {end_fmt}  \n",
    "**Partners/Collaborators:** {partners}  \n\n",
    "[⬅ Back to Home](../index.qmd)\n"
  )
  
  writeLines(page_content, file_path)
}

cat("🧭 Building index.qmd grouped by date and session...\n")

cat("🧭 Building index.qmd grouped by date and session...\n")

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

# 🎯 Filter to speaker-series projects only
speaker_projects <- session_projects %>%
  filter(project_id %in% Speaker.Themes$project_id) %>%
  filter(!is.na(presentation_date)) %>%
  mutate(presentation_date = as.Date(presentation_date)) %>%
  arrange(presentation_date)

# 📆 Group presentations by date
presentations_by_date <- split(speaker_projects, speaker_projects$presentation_date)

# 🧩 Build index content
for (date_key in names(presentations_by_date)) {
  date_presentations <- presentations_by_date[[date_key]]
  formatted_date <- format(as.Date(date_key), "%B %d, %Y")
  
  index_md <- c(index_md, glue("## 📅 {formatted_date}"), "")
  
  sessions <- date_presentations %>%
    group_by(session) %>%
    group_split()
  
  for (group in sessions) {
    session_title <- unique(group$session) %||% "Uncategorized"
    host_names <- paste(unique(na.omit(group$hosts)), collapse = "; ") %||% "Hosts TBD"
    
    index_md <- c(index_md, glue("### 🐟 {session_title}"), glue("_Hosted by: {host_names}_"), "")
    
    # 🔗 Group by project_id to avoid duplicates
    projects <- group %>%
      group_by(project_id) %>%
      summarise(
        project_link = first(project_link),
        presenters = {
          p <- paste(unique(na.omit(project_leads)), collapse = "; ")
          if (p == "") paste(unique(na.omit(recipient)), collapse = "; ") else p
        },
        source_emoji = if (any(!is.na(program_pillar))) "🧬" else "🌊",
        .groups = "drop"
      )
    
    for (i in seq_len(nrow(projects))) {
      row <- projects[i, ]
      index_md <- c(index_md, glue("- {row$source_emoji} {row$project_link} | {row$presenters}"))
    }
    
    index_md <- c(index_md, "")
  }
}

# 📝 Write index.qmd
writeLines(index_md, here("index.qmd"))

# 🌐 Write CNAME file
writeLines("www.pacificsalmonscience.ca", "CNAME")

# 🚀 Render and push site
system("quarto render")
system("git add .")
system("git commit -m \"Keeping projects to unique project_id only\"")
system("git push origin main")
