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
    str_squish() %>%
    str_to_title()
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

# 🧠 Detect and parse date format----
sessions <- sessions_raw %>%
  mutate(
    session = normalize_session(session),
    date = as.Date(date, origin = "1899-12-30")  # Converts 46001 → "2025-12-08"
  )

# 🔗 Join logic----
session_projects <- speakers %>%
  left_join(sessions, by = "session") %>%
  left_join(projects, by = "project_id") %>%
  left_join(bcsrif_projects, by = "project_id") %>%
  filter(!is.na(project_id), project_id != "") %>%
  mutate(
    title = coalesce(title, project_name),
    overview = coalesce(project_overview_jde, description_short),
    presentation_date = date,
    file_id = sanitize_filename(project_id),
    project_link = glue("[{title}](pages/{file_id}.qmd)")
  ) %>%
  arrange(presentation_date) %>%
  distinct(project_id, session, .keep_all = TRUE)

# 🔌 Disconnect from the database
dbDisconnect(con)

# 📂 Create output directory----
pages_dir <- here("pages")
dir_create(pages_dir)

# 🧼 Helper: sanitize filenames----
sanitize_filename <- function(x) gsub("[^a-zA-Z0-9_-]", "_", x)

# 🎯 Filter to speaker series projects only
speaker_ids <- Speaker.Themes %>%
  distinct(project_id) %>%
  pull(project_id)

speaker_projects <- session_projects %>%
  filter(project_id %in% speaker_ids)

# 🧠 Aggregate metadata----
aggregated_projects <- speaker_projects %>%
  group_by(project_id) %>%
  summarise(
    title = coalesce(first(title), first(project_name), "Untitled Project"),
    project_leads = paste(unique(na.omit(project_leads)), collapse = "; "),
    recipient = paste(unique(na.omit(recipient)), collapse = "; "),
    division = paste(unique(na.omit(division)), collapse = "; "),
    section = paste(unique(na.omit(section)), collapse = "; "),
    overview = paste(unique(na.omit(overview)), collapse = "; "),
    description_short = first(description_short),
    pssi_pillar = paste(unique(na.omit(pssi_pillar)), collapse = "; "),
    program_pillar = paste(unique(na.omit(program_pillar)), collapse = "; "),
    session = paste(unique(na.omit(session)), collapse = "; "),
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

# 📝 .qmd pages generation----
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
  overview    <- row[["overview"]] %||% "No description available."
  pillar      <- row[["pssi_pillar"]] %||% row[["program_pillar"]] %||% "Unspecified"
  session     <- row[["session"]] %||% "Uncategorized"
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
    "**Overview:**  \n{overview}   \n\n"
  )
  
  writeLines(page_content, file_path)
}

cat(glue("✅ Generated {nrow(aggregated_projects)} project pages.\n"))

# 🎯 Filter to speaker-series projects only----
speaker_projects <- session_projects %>%
  filter(project_id %in% Speaker.Themes$project_id) %>%
  filter(!is.na(presentation_date)) %>%
  mutate(presentation_date = as.Date(presentation_date)) %>%
  arrange(presentation_date)

# 📅 Filter December 2025 sessions----
december_sessions <- speaker_projects %>%
  filter(month(presentation_date) == 12, year(presentation_date) == 2025) %>%
  mutate(day = day(presentation_date)) %>%
  select(day, title)

# 🧱 Build calendar HTML
calendar_html <- c(
  "<table style='border-collapse: collapse; width: 100%; text-align: center;'>",
  "<caption><strong>December 2025</strong></caption>",
  "<tr>",
  paste0("<th style='padding: 5px;'>", c("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"), "</th>"),
  "</tr>"
)

first_day <- as.Date("2025-12-01")
start_weekday <- as.POSIXlt(first_day)$wday

days <- rep("", start_weekday)
for (d in 1:31) {
  label <- as.character(d)
  sessions <- december_sessions %>% filter(day == d)
  if (nrow(sessions) > 0) {
    label <- glue("<div style='background-color:#e0f7fa; border-radius:4px; padding:4px;'><strong>{d}</strong><br>{paste(sessions$title, collapse='<br>')}</div>")
  }
  days <- c(days, label)
}
while (length(days) %% 7 != 0) {
  days <- c(days, "")
}
weeks <- split(days, ceiling(seq_along(days)/7))
for (week in weeks) {
  calendar_html <- c(calendar_html, "<tr>")
  for (cell in week) {
    calendar_html <- c(calendar_html, glue("<td style='border: 1px solid #ccc; padding: 8px; vertical-align: top;'>{cell}</td>"))
  }
  calendar_html <- c(calendar_html, "</tr>")
}
calendar_html <- c(calendar_html, "</table>")

# 🧭 Index.qmd development----
index_md <- c(
  "---",
  'title: "🌊 Pacific Salmon Science Symposium"',
  'description: "30+ online presentations, 8 sessions over 4 days, reporting on the results of the most current salmon science research through the PSSI and BCSRIF programs."',
  'author: "PSSI Implementation Team"',
  'format: html',
  'toc: false',
  "---",
  "",
  "## 🗓️ December 2025 Calendar Overview",
  ""
)

index_md <- c(index_md, "::: {.calendar}", calendar_html, ":::", "")

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
    
    # Get session description
    session_description <- sessions_raw %>%
      filter(normalize_session(session) == session_title) %>%
      pull(description) %>%
      unique() %>%
      na.omit()
    
    desc_text <- if (length(session_description) > 0) session_description[1] else ""
    
    index_md <- c(index_md, glue("### 🐟 {session_title}"), desc_text, "")
    
    # 🔗 Group by project_id to avoid duplicates
    projects <- group %>%
      group_by(project_id) %>%
      summarise(
        title = first(title),
        file_id = sanitize_filename(project_id),
        project_link = glue("[{title}](pages/{file_id}.qmd)"),
        presenters = {
          p <- paste(unique(na.omit(project_leads)), collapse = "; ")
          if (p == "") paste(unique(na.omit(recipient)), collapse = "; ") else p
        },
        source_emoji = if (any(!is.na(program_pillar))) "🌱" else "🌊",
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

calendar_md <- glue(
  "---\n",
  "title: \"📅 Session Calendar\"\n",
  "description: \"Interactive calendar of presentation dates\"\n",
  "format: html\n",
  "---\n\n",
  "## 🗓️ Calendar of Sessions\n\n",
  "<div id='calendar'></div>\n\n",
  "<link href='https://cdn.jsdelivr.net/npm/fullcalendar@6.1.8/index.global.min.css' rel='stylesheet' />\n",
  "<script src='https://cdn.jsdelivr.net/npm/fullcalendar@6.1.8/index.global.min.js'></script>\n\n",
  "<script>\n",
  "  document.addEventListener('DOMContentLoaded', function() {{\n",
  "    var calendarEl = document.getElementById('calendar');\n",
  "    var calendar = new FullCalendar.Calendar(calendarEl, {{\n",
  "      initialView: 'dayGridMonth',\n",
  "      events: {calendar_js}\n",
  "    }});\n",
  "    calendar.render();\n",
  "  }});\n",
  "</script>\n"
)

writeLines(calendar_md, here("calendar.qmd"))

# 🌐 Write CNAME file
writeLines("www.pacificsalmonscience.ca", "CNAME")

# 🚀 Render and push site
system("quarto render")
system("git add .")
system("git commit -m \"Keeping projects to unique project_id only\"")
system("git push origin main")


# 🌐 Write CNAME file
writeLines("www.pacificsalmonscience.ca", "CNAME")

# 🚀 Render and push site
system("quarto render")
system("git add .")
system("git commit -m \"Changed formatting of session names\"")
system("git push origin main")
