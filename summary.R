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
vars <- colnames(gen_test)


mytheme <- hc_theme(
  chart = list(
    backgroundColor = "#DCDCDC",
    height = 600,
    width = 900
  ),
  colors = c("#05117d",
    "#7e007a",
    "#bf0068",
    "#ea244d",
    "#ff6a2e",
    "#ffa600"
  ),
  title = list(
    style = list(
      color = "black",
      fontSize = "35px",
      fontWeight = "bold",
      fontFamily = "Lato"
    )
  ),
  subtitle = list(
    style = list(
      color = "black",
      fontSize = "25px",
      fontFamily = "Lato"
    )
  ),
  legend = list(
    align = "left",
    verticalAlign = "top",
    itemStyle = list(
      fontFamily = "Lato",
      fontSize = "15px"
    )
  )
)

form_payment_yearly <- dbGetQuery(con,
  "SELECT COUNT(form_of_payment_or_transfer_of_value) AS freq, form_of_payment_or_transfer_of_value AS payment_form, program_year
  FROM sunshine.general
  WHERE program_year != ''
  GROUP BY payment_form, program_year")

form_payment_yearly %>%
  hchart("column",
    hcaes(x = program_year,
      y = freq,
      group = payment_form)
    ) %>%
  hc_xAxis(title = list(
      text = "Program Year"
      ),
      labels = list(
        style = list(
          fontSize = "15px"
        )
      )) %>%
  hc_yAxis(title = list(
      text = "Frequency"
      ),
      labels = list(
        style = list(
          fontSize = "15px"
        )
      )) %>%
  hc_title(text = "Forms of Payment") %>%
  hc_add_theme(mytheme) %>%
  hc_exporting(enabled = TRUE)

form_payment_yearly_dollars <- dbGetQuery(con,
  "SELECT SUM(total_amount_of_payment_usdollars) AS total_payment, form_of_payment_or_transfer_of_value AS payment_form, program_year
  FROM sunshine.general
  WHERE program_year != ''
  GROUP BY payment_form, program_year")

form_payment_yearly_dollars %>%
  hchart("column",
    hcaes(x = program_year,
      y = total_payment,
      group = payment_form)
    ) %>%
  hc_xAxis(title = list(
      text = "Program Year"
      ),
      labels = list(
        style = list(
          fontSize = "15px"
        )
      )) %>%
  hc_yAxis(title = list(
      text = "Total Amount"
      ),
      labels = list(
        style = list(
          fontSize = "15px"
        ),
        formatter = JS(
          "function(){
            return('$' + this.value/1000000000 + 'B')
          }"
        )
      )) %>%
  hc_title(text = "Total Spent in Each Form of Payment") %>%
  hc_add_theme(mytheme) %>%
  hc_exporting(enabled = TRUE)

recipient_type <- dbGetQuery(con,
  "SELECT COUNT(*) AS freq, program_year, covered_recipient_type
  FROM sunshine.general
  WHERE program_year != ''
  GROUP BY program_year, covered_recipient_type")

recipient_type %>%
  hchart("column",
    hcaes(x = program_year,
      y = freq,
      group = covered_recipient_type)
    ) %>%
  hc_xAxis(title = list(
      text = "Program Year"
      ),
      labels = list(
        style = list(
          fontSize = "15px"
        )
      )) %>%
  hc_yAxis(title = list(
      text = "Frequency"
      ),
      labels = list(
        style = list(
          fontSize = "15px"
        )
      )) %>%
  hc_title(text = "Number of Payments to Each Type of Recipient") %>%
  hc_add_theme(mytheme) %>%
  hc_exporting(enabled = TRUE)

dbGetQuery(con,
  "SELECT COUNT(DISTINCT(physician_profile_id))
  FROM sunshine.general")

dbGetQuery(con,
  "SELECT COUNT(DISTINCT(teaching_hospital_id))
  FROM sunshine.general")


physician_id_ct <- dbGetQuery(con,
  "SELECT COUNT(DISTINCT(physician_profile_id)) AS id_ct, program_year
  FROM sunshine.general
  GROUP BY program_year")

## do physician id's change?

pharma_ct <- dbGetQuery(con,
  "SELECT COUNT(*) AS pharma_ct, submitting_applicable_manufacturer_or_applicable_gpo_name AS pharma_co, program_year
  FROM sunshine.general
  GROUP BY program_year, pharma_co")

total_yearly_dollars <- dbGetQuery(con, "SELECT SUM(total_amount_of_payment_usdollars) as total_amount, program_year
  FROM sunshine.general
  GROUP BY program_year")

yearly_dollars_xpharm <- dbGetQuery(con,
  "SELECT SUM(total_amount_of_payment_usdollars) as total_amount, program_year, submitting_applicable_manufacturer_or_applicable_gpo_name AS pharma_co
  FROM sunshine.general
  GROUP BY program_year, pharma_co")

paid_for_yearly <- dbGetQuery(con,
  "SELECT COUNT(nature_of_payment_or_transfer_of_value) AS freq, nature_of_payment_or_transfer_of_value AS paid_for, program_year
  FROM sunshine.general
  GROUP BY paid_for, program_year")

paid_for_yearly_dollars <- dbGetQuery(con,
  "SELECT SUM(total_amount_of_payment_usdollars) AS total_paid, nature_of_payment_or_transfer_of_value AS paid_for, program_year
  FROM sunshine.general
  GROUP BY paid_for, program_year")

paid_for_yearly_dollars_xpharm <- dbGetQuery(con,
  "SELECT SUM(total_amount_of_payment_usdollars) AS total_paid, nature_of_payment_or_transfer_of_value AS paid_for, submitting_applicable_manufacturer_or_applicable_gpo_name AS pharma_co, program_year
  FROM sunshine.general
  GROUP BY paid_for, pharma_co, program_year")


form_payment_yearly %>%
  hchart("column",
    hcaes(x = program_year,
      y = freq,
      group = payment_form)
    ) %>%
  hc_xAxis(title = list(
      text = "Program Year"
  )) %>%
  hc_yAxis(title = list(
      text = "Frequency"
  )) %>%
  hc_title(text = "Forms of Payment") %>%
  hc_add_theme(mytheme) %>%
  hc_exporting(enabled = TRUE)



## most common form of payment? (form_of_payment_or_transfer_of_value)
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
