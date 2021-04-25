library(RPostgreSQL)
library(dplyr)
library(rmarkdown)
library(highcharter)
library(knitr)
library(kableExtra)

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv,
  dbname = "play",
  host = psqlCreds()$host,
  port = psqlCreds()$port,
  user = psqlCreds()$user,
  password = psqlCreds()$password)

year_tab <- dbGetQuery(con, "SELECT COUNT(*), program_year FROM sunshine.general GROUP BY program_year")
gen_test <- dbGetQuery(con, "SELECT * FROM sunshine.general LIMIT 1000")
