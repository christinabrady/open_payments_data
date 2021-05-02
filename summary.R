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

recipient_type <- dbGetQuery(con,
  "SELECT COUNT(*) AS freq, program_year, covered_recipient_type
  FROM sunshine.general
  GROUP BY program_year, covered_recipient_type")

physician_id_ct <- dbGetQuery(con,
  "SELECT COUNT(DISTINCT(physician_profile_id)) AS id_ct, program_year
  FROM sunshine.general
  GROUP BY program_year")

## do physician id's change?

pharma_ct <- dbGetQuery(con,
  "SELECT COUNT(*) AS pharma_ct, submitting_applicable_manufacturer_or_applicable_gpo_name, program_year
  FROM sunshine.general
  GROUP BY program_year, submitting_applicable_manufacturer_or_applicable_gpo_name")

## amount per year (total_amount_of_payment_usdollars)
## most common form of payment? (form_of_payment_or_transfer_of_value)
## most common nature of payment (nature_of_payment_or_transfer_of_value)
## dispute status (dispute_status_for_publication)
## who gets disputed more
## where do the physicians live/are licensed (physician_license_state_code1)
## what type of doctor (physician_primary_type)
## what speciaties do they practice (physician_specialty)
## what drugs are associated (indicate_drug_or_biological_or_device_or_medical_supply_1, name_of_drug_or_biological_or_device_or_medical_supply_1, associated_drug_or_biological_ndc_1)
## what are common cities/states/countries of travel (city_of_travel, state_of_travel, country_of_travel)
## what third parties are used (name_of_third_party_entity_receiving_payment_or_transfer_of_val)
## what pharmas use charties and what are the charities? (charity_indicator)

### sample
