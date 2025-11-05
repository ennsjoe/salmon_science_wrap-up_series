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

# 🧼 Helper functions----
sanitize_filename <- function(x) gsub("[^a-zA-Z0-9_-]", "_", x)

normalize_session <- function(x) {
  x %>%
    tolower() %>%
    str_replace_all("[^a-z0-9]+", " ") %>%
    str_squish() %>%
    str_to_title()
}

`%||%` <- function(a, b) if (is.null(a) || is.na(a) || a == "") b else a

# 🗂️ Validate database path----
db_path <- here("science_projects.sqlite")
if (!file.exists(db_path)) {
  stop(glue("❌ Database file not found at: {db_path}"))
}

# 🔌 Connect to database----
con <- dbConnect(SQLite(), dbname = db_path)
if (!dbIsValid(con)) stop("❌ Database connection is not valid")

# 📌 Validate required tables----
required_tables <- c("Science.PSSI.Projects", "presentation_info", "session_info")
available_tables <- dbListTables(con)
missing_tables <- setdiff(required_tables, available_tables)
if (length(missing_tables) > 0) {
  dbDisconnect(con)
  stop(glue("❌ Missing required tables: {paste(missing_tables, collapse = ', ')}"))
}
cat(glue("   Found {length(available_tables)} tables in database\n"))

# 📥 Load and clean tables----
presentation_info <- dbReadTable(con, "presentation_info") %>%
  mutate(project_id = as.character(project_id)) %>%
  mutate(session = normalize_session(session))

if ("confirmed" %in% names(presentation_info)) {
  presentation_info <- presentation_info %>% filter(confirmed == "Yes")
}
cat(glue("   ✓ presentation_info: {nrow(presentation_info)} confirmed rows\n"))

projects <- dbReadTable(con, "Science.PSSI.Projects") %>%
  mutate(project_id = as.character(project_id))
cat(glue("   ✓ PSSI Projects: {nrow(projects)} rows\n"))

bcsrif_projects <- if ("BCSRIF.Project.List.September.2025" %in% available_tables) {
  dbReadTable(con, "BCSRIF.Project.List.September.2025") %>%
    clean_names() %>%
    rename(project_id = project_number) %>%
    mutate(project_id = as.character(project_id))
} else {
  cat("   ⚠️ BCSRIF table not found, using empty placeholder\n")
  data.frame(project_id = character())
}
cat(glue("   ✓ BCSRIF Projects: {nrow(bcsrif_projects)} rows\n"))

sessions <- dbReadTable(con, "session_info") %>%
  mutate(session = normalize_session(session),
         date = as.Date(date, origin = "1899-12-30"))

# 🔗 Join all tables----
cat("🔗 Joining tables...\n")
joined <- presentation_info %>%
  left_join(sessions, by = "session") %>%
  left_join(projects, by = "project_id") %>%
  left_join(bcsrif_projects, by = "project_id") %>%
  mutate(
    title = coalesce(pres_title, title, project_name, "Untitled Project"),
    overview = coalesce(project_overview_jde, description_short),
    presentation_date = date,
    file_id = sanitize_filename(project_id),
    source_program = case_when(
      source == "DFO" ~ "PSSI",
      source == "BCSRIF" ~ "BCSRIF",
      TRUE ~ "Unknown"
    )
  ) %>%
  filter(!is.na(project_id), project_id != "", project_id != "(blank)") %>%
  arrange(presentation_date) %>%
  distinct(project_id, session, .keep_all = TRUE)

cat(glue("   Joined rows: {nrow(joined)}\n"))

# 🧠 Aggregate metadata----
aggregated_projects <- joined %>%
  mutate(
    project_leads_clean = coalesce(project_leads, recipient, "N/A"),
    overview_combined = coalesce(project_overview_jde, description_short, overview, "No description available."),
    title = coalesce(pres_title, title, project_name, "Untitled Project"),
    presentation_date_formatted = if_else(
      !is.na(presentation_date),
      format(as.Date(presentation_date), "%B %d, %Y"),
      "TBD"
    ),
    organization = if_else(is.na(organization) | organization == "", "Not specified", as.character(organization)),
    abstract = coalesce(abstract, overview, "No abstract available")
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
    source_program,
    bio,
    collaborators,
    authors
  )

cat(glue("   Aggregation complete: {nrow(aggregated_projects)} rows\n"))
cat(glue("     PSSI: {sum(aggregated_projects$source_program == 'PSSI', na.rm = TRUE)}\n"))
cat(glue("     BCSRIF: {sum(aggregated_projects$source_program == 'BCSRIF', na.rm = TRUE)}\n\n"))


# Create sessions_raw (without normalized session names) for calendar building
sessions_raw <- dbReadTable(con, "session_info") %>%
  mutate(date = as.Date(date, origin = "1899-12-30"))

# Create session_projects for calendar with correct titles from joined data
session_projects <- joined %>%
  select(project_id, session, presentation_date, title, speakers, organization, 
         source, source_program) %>%
  {if ("start_time" %in% names(presentation_info)) 
    left_join(., presentation_info %>% select(project_id, start_time), by = "project_id") 
    else .} %>%
  filter(!is.na(session))

# 🔌 Disconnect from database
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

# 🖼️Â Extract banner image from database
cat("🖼️Â Extracting banner image from database...\n")

# Reconnect briefly to get banner
con_banner <- dbConnect(SQLite(), dbname = db_path)

# Check if banner_images table exists
banner_path_relative <- NULL

if ("banner_images" %in% dbListTables(con_banner)) {
  banner_data <- dbReadTable(con_banner, "banner_images")
  
  if (nrow(banner_data) > 0) {
    # Get the first banner (or you can filter for "Symposium Banner.png")
    banner_row <- banner_data %>%
      filter(grepl("Symposium Banner", file_name, ignore.case = TRUE)) %>%
      slice(1)
    
    # If no Symposium Banner found, use the first one
    if (nrow(banner_row) == 0) {
      banner_row <- banner_data[1, ]
    }
    
    file_name <- banner_row$file_name
    image_binary <- banner_row$image_data[[1]]
    file_type <- banner_row$file_type
    
    if (!is.null(image_binary) && length(image_binary) > 0) {
      # Create images directory if it doesn't exist
      images_dir <- here("images")
      if (!dir.exists(images_dir)) {
        dir.create(images_dir, recursive = TRUE)
      }
      
      # Write banner to file
      banner_path <- file.path(images_dir, file_name)
      
      tryCatch({
        writeBin(image_binary, banner_path)
        banner_path_relative <- glue("images/{file_name}")
        cat(glue("  ✓ Extracted banner: {file_name}\n"))
      }, error = function(e) {
        cat(glue("  ⚠️ Could not write banner (file may be locked): {e$message}\n"))
      })
    }
  } else {
    cat("⚠️  No banner images found in database\n")
  }
} else {
  cat("⚠️  banner_images table not found in database\n")
}

dbDisconnect(con_pdf)

# �‚ Create output directory----
cat("�‚ Creating output directories...\n")
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
confirmed_project_ids <- presentation_info %>% pull(project_id)
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
  bio          <- row[["bio"]] %||% ""
  collaborators <- row[["collaborators"]] %||% ""
  authors    <- row[["authors"]] %||% ""
  
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
        "\n## Project Bulletin\n\n",
        "<iframe src=\"{pdf_relative_path}\" width=\"100%\" height=\"800px\" ",
        "style=\"border: 1px solid #ccc; border-radius: 4px;\"></iframe>\n\n",
        "<p style=\"text-align: center; margin-top: 10px;\">\n",
        "[📅 Download PDF]({pdf_relative_path}){{.btn .btn-primary target=\"_blank\"}}\n",
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
      "## PSSI Project Summary\n\n",
      "**Division:** {division}  \n",
      "**Section:** {section}  \n",
      "**Organization:** {organization}  \n",
      "**Session(s):** {session}  \n",
      "**Presentation Date(s):** {date}  \n",
      "**Speakers:** {presenters}  \n",
      "**Abstract:**  \n{abstract}   \n\n",
      if (bio != "" && !is.na(bio)) paste0("**Bio:**  \n", bio, "  \n\n") else "",
      if (collaborators != "" && !is.na(collaborators)) paste0("**Collaborators:**  \n", collaborators, "  \n\n") else "",
      if (authors != "" && !is.na(authors)) paste0("**Authors:**  \n", authors, "  \n\n") else "",
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
      "## BCSRIF Project Summary\n\n",
      "**Species Group:** {species}  \n",
      "**Location:** {location}  \n",
      "**Partners:** {partners}  \n",
      "**Organization:** {organization}  \n",
      "**Session(s):** {session}  \n",
      "**Presentation Date(s):** {date}  \n",
      "**Speakers:** {presenters}  \n",
      "**Abstract:**  \n{abstract}   \n\n",
      if (bio != "" && !is.na(bio)) paste0("**Bio:**  \n", bio, "  \n\n") else "",
      if (collaborators != "" && !is.na(collaborators)) paste0("**Collaborators:**  \n", collaborators, "  \n\n") else "",
      if (authors != "" && !is.na(authors)) paste0("**Authors:**  \n", authors, "  \n\n") else ""
    )
  } else {
    page_content <- glue(
      "---\n",
      "title: \"{title}\"\n",
      "output-file: \"{output_file}\"\n",
      "Leads: \"{lead}\"\n",
      "toc: true\n",
      "---\n\n",
      "## Project Summary\n\n",
      "**Organization:** {organization}  \n",
      "**Session(s):** {session}  \n",
      "**Presentation Date(s):** {date}  \n",
      "**Speakers:** {presenters}  \n",
      "**Abstract:**  \n{abstract}   \n\n",
      if (bio != "" && !is.na(bio)) paste0("**Bio:**  \n", bio, "  \n\n") else "",
      if (collaborators != "" && !is.na(collaborators)) paste0("**Collaborators:**  \n", collaborators, "  \n\n") else "",
      if (authors != "" && !is.na(authors)) paste0("**Authors:**  \n", authors, "  \n\n") else ""
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
cat("🗓️ Building December 2025 calendar...\n")
speaker_projects_dated <- session_projects %>%
  filter(project_id %in% presentation_info$project_id) %>%
  filter(!is.na(presentation_date)) %>%
  mutate(presentation_date = as.Date(presentation_date)) %>%
  arrange(presentation_date)

december_sessions <- speaker_projects_dated %>%
  filter(month(presentation_date) == 12, year(presentation_date) == 2025) %>%
  mutate(day = day(presentation_date)) %>%
  select(day, session) %>%
  distinct() %>%
  left_join(
    sessions_raw %>% select(session, session_id) %>% mutate(session = normalize_session(session)),
    by = "session"
  ) %>%
  arrange(day, session_id)

cat(glue("   Found {nrow(december_sessions)} December session dates\n"))

# 🎨 Single color for all sessions----
session_color <- "#1f4456"

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
cat("📄 Generating index.qmd with banner and title block...\n")

index_md <- c(
  "---",
  'title-block-banner: false',
  'format:',
  '  html:',
  '    title-block-style: none',
  'toc: false',
  "---",
  ""
)

# Banner at top
if (!is.null(banner_path_relative)) {
  index_md <- c(index_md,
                "::: {.column-screen .banner-container}",
                glue("![Pacific Salmon Strategy Initiative]({banner_path_relative}){{.banner-image}}"),
                ":::",
                "")
}

# Title and description block
index_md <- c(index_md,
              "# Pacific Salmon Science Symposium",
              "",
              "Join us this December for a series of online sessions sharing knowledge and outcomes from PSSI and BCSRIF investments into salmon research and conservation. The symposium will feature over 40 presentations from biologists and researchers organized into eight themed sessions. Scroll down for further details and registration links for the sessions.",
              "",
              "*Brought to you by the PSSI Science Implementation Team and DFO Science Pacific Region*",
              "")

# Calendar section
index_md <- c(index_md,
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
  
  index_md <- c(index_md, 
                "",
                glue("<div style='background-color: #1f4456; color: white; padding: 12px 20px; border-radius: 8px; margin-top: 30px; margin-bottom: 20px;'>"),
                glue("## 📅 {formatted_date}"),
                "</div>",
                "")
  
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
    session_hosts <- session_info$hosts %||% ""
    
    # Build session header with new information
    desc_text <- if (session_description != "" && !is.na(session_description)) session_description else ""
    
    # Create info list with time, location, chair, and webinar button
    info_parts <- c()
    if (session_time != "" && !is.na(session_time)) {
      info_parts <- c(info_parts, glue("**Time:** {session_time}"))
    }
    if (session_location != "" && !is.na(session_location)) {
      info_parts <- c(info_parts, glue("**Location:** {session_location}"))
    }
    if (session_hosts != "" && !is.na(session_hosts)) {
      info_parts <- c(info_parts, glue("**Chair:** {session_hosts}"))
    }
    if (session_url != "" && !is.na(session_url)) {
      info_parts <- c(info_parts, glue("[Register for Webinar]({session_url}){{.btn .btn-primary}}"))
    }
    
    info_line <- if (length(info_parts) > 0) paste(info_parts, collapse = "  \n") else ""
    
    index_md <- c(index_md, glue("### {session_title}"), desc_text, "", info_line, "")
    
    projects_display <- group %>%
      select(project_id, title) %>%
      left_join(
        presentation_info %>% 
          {if ("start_time" %in% names(.)) select(., project_id, speakers, organization, start_time) 
            else select(., project_id, speakers, organization)},
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
        ),
        # Parse start_time for sorting (if column exists)
        time_sort = if ("start_time" %in% names(.)) {
          case_when(
            is.na(start_time) | start_time == "" ~ as.POSIXct("9999-12-31 23:59:59"),  # Put blanks at end
            TRUE ~ parse_date_time(start_time, orders = c("I:M p", "H:M"), quiet = TRUE)
          )
        } else {
          as.POSIXct(NA)
        }
      ) %>%
      distinct(project_id, .keep_all = TRUE) %>%
      arrange(time_sort, title)  # Sort by time first, then title
    
    # Debug: print counts
    if (nrow(projects_display) == 0) {
      cat(glue("   ⚠️ WARNING: No projects found for session '{session_title}'\n"))
      cat(glue("      Original group size: {nrow(group)}\n"))
    }
    
    for (i in seq_len(nrow(projects_display))) {
      row <- projects_display[i, ]
      
      # Add start_time to display if available
      if ("start_time" %in% names(row) && !is.na(row$start_time) && row$start_time != "") {
        index_md <- c(index_md, glue("- **{row$start_time}** - {row$source_emoji} {row$project_link} | {row$speakers} | {row$organization}"))
      } else {
        index_md <- c(index_md, glue("- {row$source_emoji} {row$project_link} | {row$speakers} | {row$organization}"))
      }
    }
    
    index_md <- c(index_md, "")
  }
}

writeLines(index_md, here("index.qmd"))
cat("✅ Generated index.qmd\n\n")


# 🌐Â Write CNAME file
writeLines("www.pacificsalmonscience.ca", here("CNAME"))

# Note: _quarto.yml already exists with correct subfolder configuration

# 🚀 Render site
cat("🔨 Rendering Quarto site...\n")
cat("   Waiting for file handles to release...\n")
Sys.sleep(2)  # Wait 2 seconds for any file handles to release

# Try to render, with error handling
render_result <- tryCatch({
  system("quarto render --no-clean", intern = FALSE, ignore.stderr = FALSE)
}, error = function(e) {
  cat("⚠️  Render encountered an issue, retrying...\n")
  Sys.sleep(3)
  system("quarto render --no-clean", intern = FALSE, ignore.stderr = FALSE)
})

cat("✅ Quarto render complete\n\n")

#cat("📤 Pushing to GitHub...\n")
#system("git add .")
#system('git commit -m "Updating schedulesI "')
#system("git push origin main")

#cat("\n✨ All done! Site deployed.\n")
