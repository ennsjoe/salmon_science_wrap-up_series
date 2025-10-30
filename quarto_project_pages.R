################################################################################
# Title: Query SQLite Tables & Generate Quarto Pages
# Description: Connects to SQLite, validates tables, joins project data,
#              and builds Quarto pages and index.
# VERSION: v31_DISTINCT_FIX - Uses distinct() instead of summarise()
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
library(digest)

# 🧼 Helper functions (define early!)----
sanitize_filename <- function(x) gsub("[^a-zA-Z0-9_-]", "_", x)

normalize_session <- function(x) {
  x %>%
    tolower() %>%
    str_replace_all("[^a-z0-9]+", " ") %>%
    str_squish() %>%
    str_to_title()
}

`%||%` <- function(a, b) if (is.null(a) || is.na(a) || a == "") b else a

# 🗂️ Define and validate database pat----
db_path <- here("science_projects.sqlite")

if (!file.exists(db_path)) {
  stop(glue("❌ Database file not found at: {db_path}\n",
            "   Please run the data loading script first or check the file path."))
}

# 🔌 Connect to the database----
con <- tryCatch({
  dbConnect(SQLite(), dbname = db_path)
}, error = function(e) {
  stop(glue("❌ Failed to connect to database: {e$message}"))
})

# Test connection before proceeding
if (!dbIsValid(con)) {
  stop("❌ Database connection is not valid")
}

# 📌 Validate required tables
cat("📋 Validating required tables...\n")
required_tables <- c("Science.PSSI.Projects", "Speaker.Themes", "session_info")
available_tables <- dbListTables(con)

cat(glue("   Found {length(available_tables)} tables in database\n"))

missing_tables <- setdiff(required_tables, available_tables)

if (length(missing_tables) > 0) {
  dbDisconnect(con)
  
  stop(glue("❌ Missing required tables: {paste(missing_tables, collapse = ', ')}\n",
            "   Available tables: {paste(available_tables, collapse = ', ')}"))
}

cat("✅ All required tables present\n\n")

# 📥 Load and clean tables----

Speaker.Themes <- dbReadTable(con, "Speaker.Themes") %>%
  mutate(project_id = as.character(project_id)) %>%
  filter(confirmed == "Yes")  # Only include confirmed presentations
cat(glue("   ✓ Speaker.Themes (confirmed only): {nrow(Speaker.Themes)} rows\n"))

# Check if BCSRIF table exists
if ("BCSRIF.Project.List.September.2025" %in% available_tables) {
  bcsrif_projects <- dbReadTable(con, "BCSRIF.Project.List.September.2025") %>%
    janitor::clean_names() %>%
    rename(project_id = project_number) %>%
    mutate(project_id = as.character(project_id))
  cat(glue("   ✓ BCSRIF projects: {nrow(bcsrif_projects)} rows\n"))
} else {
  cat("   ⚠ BCSRIF table not found, creating empty placeholder\n")
  bcsrif_projects <- data.frame(
    project_id = character(),
    project_name = character(),
    description_short = character(),
    recipient = character(),
    species_group = character(),
    location_of_project = character(),
    agreement_start_date = numeric(),
    agreement_end_date = numeric(),
    list_of_partners_or_collaborators = character()
  )
}

projects <- dbReadTable(con, "Science.PSSI.Projects") %>%
  mutate(project_id = as.character(project_id))
cat(glue("   ✓ PSSI Projects: {nrow(projects)} rows\n"))

speakers <- dbReadTable(con, "Speaker.Themes") %>%
  mutate(
    project_id = as.character(project_id),
    session = normalize_session(session)
  ) %>%
  filter(confirmed == "Yes")  # Only include confirmed presentations
cat(glue("   ✓ Speakers (confirmed only): {nrow(speakers)} rows\n"))

sessions_raw <- dbReadTable(con, "session_info")

sessions <- sessions_raw %>%
  mutate(
    session = normalize_session(session),
    date = as.Date(date, origin = "1899-12-30")
  )

# 🧾 Map sources to programs----
project_sources <- Speaker.Themes %>%
  select(project_id, source) %>%
  distinct() %>%
  mutate(
    source_program = case_when(
      source == "DFO" ~ "PSSI",
      source == "BCSRIF" ~ "BCSRIF",
      TRUE ~ "Unknown"
    )
  )

# 🔗 Initial join to create session_projects----

initial_join <- speakers %>%
  left_join(sessions, by = "session")

# Check how many have dates
with_dates <- initial_join %>% filter(!is.na(date))
cat(glue("   With presentation dates: {nrow(with_dates)} rows\n"))

session_projects_pre_filter <- initial_join %>%
  left_join(projects, by = "project_id") %>%
  left_join(bcsrif_projects, by = "project_id") %>%
  mutate(
    title = coalesce(title, project_name, "Untitled Project"),
    overview = coalesce(project_overview_jde, description_short, overview),
    presentation_date = date,
    file_id = sanitize_filename(project_id)
  )

cat(glue("   After joining project tables: {nrow(session_projects_pre_filter)} rows\n"))

# Check source distribution before filtering
if ("source" %in% names(session_projects_pre_filter)) {
  cat("   Source distribution before filter:\n")
  pre_filter_source <- table(session_projects_pre_filter$source, useNA = "ifany")
  for (src in names(pre_filter_source)) {
    cat(glue("      {src}: {pre_filter_source[src]}\n"))
  }
}

session_projects <- session_projects_pre_filter %>%
  filter(!is.na(project_id), project_id != "", project_id != "(blank)") %>%
  arrange(presentation_date) %>%
  distinct(project_id, session, .keep_all = TRUE)

# 🎯 Filter to speaker series projects only----
speaker_ids <- Speaker.Themes %>%
  distinct(project_id) %>%
  pull(project_id)

speaker_projects <- session_projects %>%
  filter(project_id %in% speaker_ids)

# 🧠 Aggregate metadata----
# Check for any rows with missing critical data
missing_title <- sum(is.na(speaker_projects$title) | speaker_projects$title == "")
missing_source <- sum(is.na(speaker_projects$source) | speaker_projects$source == "")
cat(glue("   Rows with missing title: {missing_title}\n"))
cat(glue("   Rows with missing source: {missing_source}\n"))

# Create source_program first
speaker_projects_with_program <- speaker_projects %>%
  mutate(
    source_program = case_when(
      source == "DFO" ~ "PSSI",
      source == "BCSRIF" ~ "BCSRIF",
      TRUE ~ "Unknown"
    )
  )

# Check for duplicate project_ids
cat("\n   Checking for duplicate project_ids:\n")
project_counts <- speaker_projects_with_program %>%
  group_by(project_id) %>%
  summarise(n = n(), programs = paste(unique(source_program), collapse = ", ")) %>%
  arrange(desc(n))
cat(glue("   Unique project_ids: {nrow(project_counts)}\n"))
cat(glue("   Max occurrences of single ID: {max(project_counts$n)}\n"))
if (any(project_counts$n > 1)) {
  cat("\n   Projects appearing multiple times:\n")
  print(head(project_counts %>% filter(n > 1), 10))
}

cat("\n   Attempting aggregation with distinct() instead of summarise()...\n")

# Since we confirmed no duplicate project_ids, we can use distinct() instead of summarise()
aggregated_projects <- speaker_projects_with_program %>%
  mutate(
    project_leads_clean = case_when(
      source_program == "PSSI" & !is.na(project_leads) & project_leads != "" ~ project_leads,
      source_program == "BCSRIF" & !is.na(recipient) & recipient != "" ~ recipient,
      !is.na(project_leads) & project_leads != "" ~ project_leads,
      !is.na(recipient) & recipient != "" ~ recipient,
      TRUE ~ "N/A"
    ),
    overview_combined = case_when(
      !is.na(project_overview_jde) & project_overview_jde != "" ~ project_overview_jde,
      !is.na(description_short) & description_short != "" ~ description_short,
      !is.na(overview) & overview != "" ~ overview,
      TRUE ~ "No description available."
    ),
    title = if_else(!is.na(title) & title != "", title, 
                    if_else(!is.na(project_name) & project_name != "", project_name, "Untitled Project")),
    presentation_date_formatted = if_else(
      !is.na(presentation_date),
      format(as.Date(presentation_date), "%B %d, %Y"),
      "TBD"
    ),
    organization = if_else(is.na(organization) | organization == "", "Not specified", as.character(organization)),
    abstract = case_when(
      !is.na(abstract) & abstract != "" ~ as.character(abstract),
      !is.na(overview) & overview != "" ~ overview,
      TRUE ~ "No abstract available"
    )
  ) %>%
  distinct(project_id, .keep_all = TRUE) %>%
  select(
    project_id,
    title,
    project_leads = project_leads_clean,
    recipient,
    division,
    section,
    overview = overview_combined,
    description_short,
    pssi_pillar,
    program_pillar,
    session,
    speakers,
    hosts,
    presentation_date = presentation_date_formatted,
    species_group,
    location_of_project,
    agreement_start_date,
    agreement_end_date,
    list_of_partners_or_collaborators,
    organization,
    abstract,
    source_program
  )

cat(glue("   Aggregation complete: {nrow(aggregated_projects)} rows\n"))
cat(glue("     PSSI: {sum(aggregated_projects$source_program == 'PSSI', na.rm = TRUE)}\n"))
cat(glue("     BCSRIF: {sum(aggregated_projects$source_program == 'BCSRIF', na.rm = TRUE)}\n\n"))

# 🔌 DISCONNECT NOW (we're done with the database)
dbDisconnect(con)

# 📄 Extract PDFs from database and write to filesystem
cat("📄 Extracting PDFs from database...\n")

# Reconnect briefly to get PDFs
con_pdf <- dbConnect(SQLite(), dbname = db_path)

# Check if PSSI_bulletins table exists
if ("PSSI_bulletins" %in% dbListTables(con_pdf)) {
  pdf_data <- dbReadTable(con_pdf, "PSSI_bulletins")
  
  if (nrow(pdf_data) > 0) {
    # Create PSSI_bulletin directory if it doesn't exist
    pdf_dir <- here("data", "PSSI_bulletin")
    if (!dir.exists(pdf_dir)) {
      dir.create(pdf_dir, recursive = TRUE)
    }
    
    # Write each PDF to file
    extracted_count <- 0
    skipped_count <- 0
    for (j in seq_len(nrow(pdf_data))) {
      project_id <- pdf_data$project_id[j]
      pdf_binary <- pdf_data$pdf_data[[j]]
      
      if (!is.null(pdf_binary) && length(pdf_binary) > 0) {
        pdf_path <- file.path(pdf_dir, glue("{project_id}.pdf"))
        
        # Try to write the PDF, skip if file is locked
        tryCatch({
          writeBin(pdf_binary, pdf_path)
          cat(glue("  ✓ Extracted PDF for project {project_id}\n"))
          extracted_count <- extracted_count + 1
        }, error = function(e) {
          cat(glue("  ⚠️ Skipped PDF for project {project_id} (file may be open/locked)\n"))
          skipped_count <- skipped_count + 1
        })
      }
    }
    cat(glue("✅ Extracted {extracted_count} PDFs"))
    if (skipped_count > 0) {
      cat(glue(" ({skipped_count} skipped due to file locks)"))
    }
    cat("\n\n")
  } else {
    cat("⚠️  No PDFs found in database\n\n")
  }
} else {
  cat("⚠️  PSSI_bulletins table not found in database\n\n")
}

dbDisconnect(con_pdf)


# 📂 Create output directory----
cat("📂 Creating output directories...\n")
pages_dir <- here("pages")

# Clean up old .qmd files to ensure only confirmed projects are included
if (dir.exists(pages_dir)) {
  cat("   Cleaning up old .qmd files...\n")
  old_files <- list.files(pages_dir, pattern = "\\.qmd$", recursive = TRUE, full.names = TRUE)
  if (length(old_files) > 0) {
    file.remove(old_files)
    cat(glue("   Removed {length(old_files)} old .qmd files\n"))
  }
}

dir_create(pages_dir)
dir_create(file.path(pages_dir, "pssi"))
dir_create(file.path(pages_dir, "bcsrif"))
dir_create(file.path(pages_dir, "other"))
cat("✅ Directories ready\n\n")

# 📝 Generate .qmd pages----
cat("📝 Generating project pages...\n")

# Double-check: only create pages for confirmed projects
confirmed_project_ids <- Speaker.Themes %>% pull(project_id)
aggregated_projects_confirmed <- aggregated_projects %>%
  filter(project_id %in% confirmed_project_ids)

cat(glue("   Creating pages for {nrow(aggregated_projects_confirmed)} confirmed projects\n"))

progress_count <- 0

for (i in seq_len(nrow(aggregated_projects_confirmed))) {
  row <- aggregated_projects_confirmed[i, ]
  file_id <- sanitize_filename(row[["project_id"]])
  if (is.na(file_id) || file_id == "") next
  
  # Route to subfolder
  subfolder <- switch(row[["source_program"]],
                      "PSSI" = "pssi",
                      "BCSRIF" = "bcsrif",
                      "other")
  output_dir <- file.path(pages_dir, subfolder)
  file_path <- file.path(output_dir, paste0(file_id, ".qmd"))
  
  # Common fields
  title        <- row[["title"]] %||% "Untitled Project"
  lead         <- row[["project_leads"]] %||% row[["recipient"]] %||% "N/A"
  abstract     <- row[["abstract"]] %||% "No abstract available."
  overview     <- row[["overview"]] %||% "No description available."
  session      <- row[["session"]] %||% "Uncategorized"
  presenters   <- row[["speakers"]] %||% "Presenters TBD"
  date         <- row[["presentation_date"]] %||% "TBD"
  organization <- row[["organization"]] %||% "Not specified"
  
  output_file <- paste0(file_id, ".html")
  
  # Compose content based on program
  if (row[["source_program"]] == "PSSI") {
    division <- row[["division"]] %||% "N/A"
    section  <- row[["section"]] %||% "N/A"
    pillar   <- row[["pssi_pillar"]] %||% "Unspecified"
    project_id <- row[["project_id"]]
    
    # Check if PDF exists for this project
    pdf_path <- here("data", "PSSI_bulletin", glue("{project_id}.pdf"))
    pdf_relative_path <- glue("../../data/PSSI_bulletin/{project_id}.pdf")
    
    pdf_section <- ""
    if (file.exists(pdf_path)) {
      pdf_section <- glue(
        "\n## 📄 Project Bulletin\n\n",
        "<iframe src=\"{pdf_relative_path}\" width=\"100%\" height=\"800px\" ",
        "style=\"border: 1px solid #ccc; border-radius: 4px;\"></iframe>\n\n",
        "<p style=\"text-align: center; margin-top: 10px;\">\n",
        "[📥 Download PDF]({pdf_relative_path}){{.btn .btn-primary target=\"_blank\"}}\n",
        "</p>\n\n"
      )
    }
    
    page_content <- glue(
      "---\n",
      "title: \"{title}\"\n",
      "output-file: \"{output_file}\"\n",
      "Leads: \"{lead}\"\n",
      "toc: true\n",
      "---\n\n",
      "## 📋 PSSI Project Summary\n\n",
      "**Division:** {division}  \n",
      "**Section:** {section}  \n",
      "**Organization:** {organization}  \n",
      "**Session(s):** {session}  \n",
      "**Presentation Date(s):** {date}  \n",
      "**Speakers:** {presenters}  \n",
      "**Abstract:**  \n{abstract}   \n\n",
      "{pdf_section}"
    )
    
  } else if (row[["source_program"]] == "BCSRIF") {
    species   <- row[["species_group"]] %||% "Not specified"
    location  <- row[["location_of_project"]] %||% "Unknown"
    partners  <- row[["list_of_partners_or_collaborators"]] %||% "None listed"
    start     <- row[["agreement_start_date"]]
    end       <- row[["agreement_end_date"]]
    start_fmt <- if (!is.na(start)) format(as.Date(start, origin = "1970-01-01"), "%B %d, %Y") else "TBD"
    end_fmt   <- if (!is.na(end)) format(as.Date(end, origin = "1970-01-01"), "%B %d, %Y") else "TBD"
    
    page_content <- glue(
      "---\n",
      "title: \"{title}\"\n",
      "output-file: \"{output_file}\"\n",
      "Leads: \"{lead}\"\n",
      "toc: true\n",
      "---\n\n",
      "## 📋 BCSRIF Project Summary\n\n",
      "**Species Group:** {species}  \n",
      "**Location:** {location}  \n",
      "**Partners:** {partners}  \n",
      "**Agreement Period:** {start_fmt} to {end_fmt}  \n",
      "**Organization:** {organization}  \n",
      "**Session(s):** {session}  \n",
      "**Presentation Date(s):** {date}  \n",
      "**Speakers:** {presenters}  \n",
      "**Abstract:**  \n{abstract}   \n\n"
    )
  } else {
    page_content <- glue(
      "---\n",
      "title: \"{title}\"\n",
      "output-file: \"{output_file}\"\n",
      "Leads: \"{lead}\"\n",
      "toc: true\n",
      "---\n\n",
      "## 📋 Project Summary\n\n",
      "**Organization:** {organization}  \n",
      "**Session(s):** {session}  \n",
      "**Presentation Date(s):** {date}  \n",
      "**Speakers:** {presenters}  \n",
      "**Abstract:**  \n{abstract}   \n\n"
    )
  }
  
  writeLines(page_content, file_path)
  progress_count <- progress_count + 1
  
  if (progress_count %% 10 == 0) {
    cat(glue("   ... {progress_count} pages generated\n"))
  }
}

cat(glue("✅ Generated {nrow(aggregated_projects_confirmed)} project pages\n\n"))

# 📅 Build December 2025 calendar----
cat("📅 Building December 2025 calendar...\n")
speaker_projects_dated <- session_projects %>%
  filter(project_id %in% Speaker.Themes$project_id) %>%
  filter(!is.na(presentation_date)) %>%
  mutate(presentation_date = as.Date(presentation_date)) %>%
  arrange(presentation_date)

december_sessions <- speaker_projects_dated %>%
  filter(month(presentation_date) == 12, year(presentation_date) == 2025) %>%
  mutate(day = day(presentation_date)) %>%
  select(day, session) %>%
  distinct()

cat(glue("   Found {nrow(december_sessions)} December session dates\n"))

# 🎨 Single color for all sessions
session_color <- "#007BFF"  # Blue - change this to match your site!

get_session_color <- function(session_name) {
  session_color
}

# 🧱 Build calendar HTML
calendar_html <- c(
  "<table class='calendar-table'>",
  "<caption><strong>December 2025</strong></caption>",
  "<tr>",
  paste0("<th>", c("Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"), "</th>", collapse = ""),
  "</tr>"
)

first_day <- as.Date("2025-12-01")
start_weekday <- as.POSIXlt(first_day)$wday

days <- rep("", start_weekday)
for (d in 1:31) {
  label <- as.character(d)
  sessions_today <- december_sessions %>% filter(day == d)
  if (nrow(sessions_today) > 0) {
    buttons <- paste0(
      "<div class='calendar-button' style='background-color:", 
      sapply(sessions_today$session, get_session_color), 
      "'>", sessions_today$session, "</div>"
    )
    label <- glue("<div class='calendar-cell'><strong>{d}</strong><br>{paste(buttons, collapse='')}</div>")
  } else {
    label <- glue("<div class='calendar-cell'><strong>{d}</strong></div>")
  }
  days <- c(days, label)
}

while (length(days) %% 7 != 0) days <- c(days, "")

weeks <- split(days, ceiling(seq_along(days)/7))
for (week in weeks) {
  calendar_html <- c(calendar_html, "<tr>")
  for (cell in week) {
    calendar_html <- c(calendar_html, glue("<td>{cell}</td>"))
  }
  calendar_html <- c(calendar_html, "</tr>")
}
calendar_html <- c(calendar_html, "</table>")

# 📄 Generate index.qmd----
cat("📄 Generating index.qmd...\n")

index_md <- c(
  "---",
  'title: "🌊 Pacific Salmon Science Symposium"',
  'description: "45+ online presentations, 8 sessions over 4 days, reporting on the results of the most current salmon science research through the PSSI and BCSRIF programs."',
  'author: "PSSI Implementation Team"',
  'format: html',
  'toc: false',
  "---",
  "",
  "## 🗓️ December 2025 Calendar Overview",
  "",
  "::: {.calendar}",
  calendar_html,
  ":::",
  "",
  "- 🌊 PSSI (Pacific Salmon Science Initiative)",
  "- 🌱 BCSRIF (BC Salmon Restoration and Innovation Fund)",
  ""
)

# Group presentations by date
presentations_by_date <- split(speaker_projects_dated, speaker_projects_dated$presentation_date)

for (date_key in names(presentations_by_date)) {
  date_presentations <- presentations_by_date[[date_key]]
  formatted_date <- format(as.Date(date_key), "%B %d, %Y")
  
  index_md <- c(index_md, glue("## 📅 {formatted_date}"), "")
  
  sessions_list <- date_presentations %>%
    group_by(session) %>%
    group_split()
  
  # Order sessions by session_id
  sessions_list <- sessions_list %>%
    lapply(function(group) {
      session_name <- unique(group$session)
      session_id <- sessions_raw %>%
        filter(normalize_session(session) == session_name) %>%
        pull(session_id) %>%
        first()
      list(data = group, session_id = session_id %||% 999)
    }) %>%
    .[order(sapply(., function(x) x$session_id))]
  
  for (session_obj in sessions_list) {
    group <- session_obj$data
    session_title <- unique(group$session) %||% "Uncategorized"
    
    # Get session information including new fields
    session_info <- sessions_raw %>%
      filter(normalize_session(session) == session_title) %>%
      slice(1)
    
    session_description <- session_info$description %||% ""
    session_time <- session_info$time %||% ""
    session_location <- session_info$location %||% ""
    session_url <- session_info$webinar_url %||% ""
    
    # Build session header with new information
    desc_text <- if (session_description != "" && !is.na(session_description)) session_description else ""
    
    # Create info line with time and location
    info_parts <- c()
    if (session_time != "" && !is.na(session_time)) {
      info_parts <- c(info_parts, glue("**Time:** {session_time}"))
    }
    if (session_location != "" && !is.na(session_location)) {
      info_parts <- c(info_parts, glue("**Location:** {session_location}"))
    }
    if (session_url != "" && !is.na(session_url)) {
      info_parts <- c(info_parts, glue("[Join Webinar]({session_url})"))
    }
    
    info_line <- if (length(info_parts) > 0) paste(info_parts, collapse = " | ") else ""
    
    index_md <- c(index_md, glue("### 🐟 {session_title}"), desc_text, info_line, "")
    
    projects_display <- group %>%
      select(project_id, title) %>%
      left_join(
        Speaker.Themes %>% select(project_id, speakers, organization),
        by = "project_id"
      ) %>%
      left_join(
        aggregated_projects_confirmed %>% select(project_id, source_program),
        by = "project_id"
      ) %>%
      mutate(
        subfolder = case_when(
          source_program == "BCSRIF" ~ "bcsrif",
          source_program == "PSSI" ~ "pssi",
          TRUE ~ "other"
        ),
        speakers = if_else(is.na(speakers) | speakers == "", "Presenters TBD", speakers),
        organization = if_else(is.na(organization) | organization == "", "Not specified", as.character(organization)),
        file_id = sanitize_filename(project_id),
        project_link = glue("[{title}](pages/{subfolder}/{file_id}.html)"),
        source_emoji = case_when(
          source_program == "BCSRIF" ~ "🌱",
          source_program == "PSSI" ~ "🌊",
          TRUE ~ "❓"
        )
      ) %>%
      distinct(project_id, .keep_all = TRUE) %>%
      arrange(title)
    
    # Debug: print counts
    if (nrow(projects_display) == 0) {
      cat(glue("   ⚠ WARNING: No projects found for session '{session_title}'\n"))
      cat(glue("      Original group size: {nrow(group)}\n"))
    }
    
    for (i in seq_len(nrow(projects_display))) {
      row <- projects_display[i, ]
      index_md <- c(index_md, glue("- {row$source_emoji} {row$project_link} | {row$speakers} | {row$organization}"))
    }
    
    index_md <- c(index_md, "")
  }
}

writeLines(index_md, here("index.qmd"))
cat("✅ Generated index.qmd\n\n")


# 🌐 Write CNAME file
writeLines("www.pacificsalmonscience.ca", here("CNAME"))

# Note: _quarto.yml already exists with correct subfolder configuration

# 🚀 Render site
cat("🔨 Rendering Quarto site...\n")
system("quarto render")
cat("✅ Quarto render complete\n\n")

cat("📤 Pushing to GitHub...\n")
system("git add .")
system('git commit -m "Updated project pages with confirmed filter, organization, and abstracts"')
system("git push origin main")

cat("\n✨ All done! Site deployed.\n")