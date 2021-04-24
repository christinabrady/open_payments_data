library(RPostgreSQL)
library(dplyr)
library(NCmisc)
library(rvest)

options(timeout = 200)  ### CMS site is timing out with 60 second limit
# library(ckit) ## this can't be installed on the server due to version incompatibility

### am currently unable to install ckit on the server due to version issues
cleanColNames <- function(dat){
  tmp <- gsub("\\.+", "_", tolower(colnames(dat)))
  tmp <- gsub("\\s+", "_", tmp)
  colnames(dat) <- tmp
  dat
}

dt_format <- "%m/%d/%Y"

### the file names change to include the upload date.
### so it will be best to grab the file names directly from the website.

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv,
  dbname = "play",
  host = psqlCreds()$host,
  port = psqlCreds()$port,
  user = psqlCreds()$user,
  password = psqlCreds()$password)

links <- read_html("https://www.cms.gov/OpenPayments/Explore-the-Data/Dataset-Downloads") %>%
  html_nodes("a") %>%
  html_attr("href")

data_links <- grep(".PGYR.*\\.ZIP$", links, value = TRUE) ## this grabs the zip file download links from all of the links.
## the zipped file in the base link expands to 4 files:
# 1. OP_DTL_GNRL_PGYR...csv
# 2. OP_DTL_OWNRSHP_PGYR...csv
# 3. OP_DTL_RSRCH_PGYR...csv
# 4. OP_PGYR...README...txt

## The General file GNRL is enormous and needs to be split
## The others aren't that big.

getHeader <- function(fl){
  read.csv(fl, nrow = 1) %>%
  cleanColNames() %>%
  colnames()
}

processGeneralFile <- function(dbcon, fl){
  file.split(fl)
  cnames <- getHeader(fl)
  split_files <- list.files(pattern = "part")
  cat("There are", length(split_files), "files to work on", fill = TRUE)

  lapply(split_files, function(sfl){
    cat("Working on", basename(sfl), fill = TRUE)

    tmp <- read.csv(sfl, header = grepl("a+\\.csv", sfl), colClasses = "character") %>%
      setNames(cnames) %>%
      mutate(download_date = Sys.Date(),
        total_amount_of_payment_usdollars = as.numeric(total_amount_of_payment_usdollars),
        number_of_payments_included_in_total_amount = as.numeric(number_of_payments_included_in_total_amount),
        date_of_payment = as.Date(date_of_payment, dt_format),
        payment_publication_date = as.Date(payment_publication_date, dt_format)
      )
    dbWriteTable(conn = dbcon,
      name = c("sunshine", "general"),
      value = tmp,
      row.names = FALSE,
      append = dbExistsTable(dbcon, c("sunshine", "general"))
    )
    cat("Deleting file", fill = TRUE)
    unlink(sfl)
  })
  unlink(fl)
}

processResearchFiles <- function(dbcon, fl, tname){
  tmp <- read.csv(fl, colClasses = "character") %>%
    cleanColNames() %>%
    mutate(download_date = Sys.Date(),
      program_year = as.numeric(program_year),
      total_amount_of_payment_usdollars = as.numeric(total_amount_of_payment_usdollars),
      date_of_payment = as.Date(date_of_payment, dt_format),
      payment_publication_date = as.Date(payment_publication_date, dt_format)
    )
  dbWriteTable(conn = dbcon,
    name = c("sunshine", tname),
    value = tmp,
    row.names = FALSE,
    append = dbExistsTable(dbcon, c("sunshine", tname))
  )
  cat("Deleting file", fill = TRUE)
  unlink(fl)
}

processOwnershipFiles <- function(dbcon, fl, tname){
  tmp <- read.csv(fl, colClasses = "character") %>%
    cleanColNames() %>%
    mutate(download_date = Sys.Date(),
      program_year = as.numeric(program_year),
      total_amount_invested_usdollars = as.numeric(total_amount_invested_usdollars),
      payment_publication_date = as.Date(payment_publication_date, dt_format)
    )
  dbWriteTable(conn = dbcon,
    name = c("sunshine", tname),
    value = tmp,
    row.names = FALSE,
    append = dbExistsTable(dbcon, c("sunshine", tname))
  )
  cat("Deleting file", fill = TRUE)
  unlink(fl)
}


processLink <- function(lnk){
  ### download and unzip the zip file
  cat("Downloading ", lnk)
  download.file(lnk, destfile = basename(lnk))
  unzip(basename(lnk))
  unzipped_fls <- list.files(pattern = '\\.csv')

  ### process the general file, which needs to be split
  cat("Starting on General file")
  gen <- grep("GNRL", unzipped_fls, value = TRUE)
  processGeneralFile(con, gen)

  cat("Starting on ownership file")
  owner <- grep("OWNRSHP", unzipped_fls, value = TRUE)
  processOwnershipFiles(con, owner, "ownership")

  cat("Starting on research file")
  research <- grep("RSRCH", unzipped_fls, value = TRUE)
  processResearchFiles(con, research, "research")

  remaining_files <- list.files()
  lapply(remaining_files, unlink)
}

dir.create("_tmp")
setwd("_tmp")

# dbSendQuery(con, "CREATE SCHEMA sunshine")

lapply(data_links, processLink)

setwd("../")
unlink("_tmp", recursive = TRUE)
