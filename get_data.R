library(RODBC)
library(dplyr)
library(NCmisc)
library(ckit)

options(stringsAsFactors = F)

### as of August 31, 2019 the only difference in data links is the program year. This may change.
### example link http://download.cms.gov/openpayments/PGYR18_P062819.ZIP

## the zipped file in the base link expands to 4 files:
# 1. OP_DTL_GNRL_PGYR...csv
# 2. OP_DTL_OWNRSHP_PGYR...csv
# 3. OP_DTL_RSRCH_PGYR...csv
# 4. OP_PGYR...README...txt

pgyr <- 18
base_link <- "http://download.cms.gov/openpayments/PGYR%s_P062819.ZIP"
fname <- sprintf(base_link, pgyr)

fltypes <- c("GNRL", "OWNRSHP", "RSRCH")

dbexists(chan, tablename){
  tmp <- sqlQuery(chan, sprintf("SELECT * FROM %s LIMIT 1"), tablename)
  is.data.frame(tmp)
}

prepFiles <- function(fltype){
  file.split(grep(ft, fls, value = T))
  grep_pat <- sprintf("(?=.*%s)(?=.*part)", ft)
  grep(grep_pat, list.files(), value = T, perl = T)
}

startUpload <- function(fl, chan, tname){
  tmp <- read.csv(fl) %>%
    cleanColNames() %>%
    mutate(data_date = Sys.Date())
  sqlSave(chan,
    tmp,
    tname,
    append = dbexists(chan, tname),
    rownames = F,
    varTypes = c("data_date" = "date")
  )
  unlink(fl)
  return(colnames(tmp))
}


dbplay <- odbcConnect("play")
sqlQuery(dbplay, "CREATE SCHEMA sunshine")


mkdir("_tmp")
setwd("_tmp")

download.file(fname, destfile = basename(fname), method = "wget")
unzip(basename(fname))

fls <- list.files(pattern = "csv")

lapply(fltypes, function(ft){
  cat("working on ", ft, fill = T)
  newfls <- prepFiles(ft)

  cat(length(newfls), " files to process", fill = T)

  tname <- sprintf("sunshine.%s", tolower(ft))

  ### read the first file separately because it has headers
  cnames <- startUpload(newfls[1], dbplay, tname)
  lapply(newfls[2:length(newfls)], function(fl){
    read.csv(fl) %>%
      mutate(data_date = Sys.Date()) %>%
      setNames(cnames) %>%
      sqlSave(channel = dbplay,
        tablename = tname,
        append = dbexists(dbplay, tname),
        rownames = F,
        varTypes = c("data_date" = "date")
      )
    unlink(fl)
  })
})
