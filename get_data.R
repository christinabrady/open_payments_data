library(RPostgreSQL)
library(dplyr)
library(NCmisc)
library(rvest)
library(ckit)

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

    tmp <- read.csv(sfl, header = FALSE) %>%
      setNames(cnames) %>%
      mutate(download_date = Sys.Date())
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

processOtherFiles <- function(dbcon, fl, tname){
  tmp <- read.csv(fl) %>%
    cleanColNames() %>%
    mutate(download_date = Sys.Date())
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
  processOtherFiles(con, owner, "ownership")

  cat("Starting on research file")
  research <- grep("RSRCH", unzipped_fls, value = TRUE)
  processOtherFiles(con, research, "research")

  remaining_files <- list.files()
  lapply(remaining_files, unlink)
}

dir.create("_tmp")
setwd("_tmp")

dbSendQuery(con, "CREATE SCHEMA sunshine")

lapply(data_links, processLink)

setwd("../")
unlink("_tmp", recursive = TRUE)
