
# Inizialization script for the script databricks_insuranceSCRIPT.R
# All the scripts that should run before continue writing more lines

# Necessary libraries
library(ggplot2)
library(jsonlite)
library(data.table)
library(lubridate)

# Data load
policies <- read.csv("https://raw.githubusercontent.com/databricks-industry-solutions/dlt-insurance-claims/refs/heads/main/data/samples/mysql/policies.csv")

claims <- fromJSON("https://raw.githubusercontent.com/databricks-industry-solutions/dlt-insurance-claims/refs/heads/main/data/samples/mongodb/claims.json")

# Find duplicated policies
duplicated_policies <- names(head(sort(table(policies$POLICY_NO), decreasing = TRUE), 8)[-1])
duplicated_pol_claims <- sort(table(claims$policy_no), decreasing = T)
duplicated_pol_claims <- duplicated_pol_claims[which(duplicated_pol_claims > 1)]
duplicated_pol_claims <- names(duplicated_pol_claims)

# Calculate indexes to be droped
drop_index <- numeric()
for (i in duplicated_pol_claims) {
  drop_index <- c(drop_index, which(claims$policy_no == i))
}
##### NEW BLOCK
for (i in duplicated_policies) {
  drop_index <- c(drop_index, which(claims$policy_no == i))
}
##### END NEW BLOCK
# Merge
insurance <- merge(claims[-drop_index, ], policies, by.x = c("policy_no"), 
                   by.y = c("POLICY_NO"), all.x = T)

# Remove unexpected NA's
insurance <- insurance[-which(is.na(insurance$policy_no)), ]

insurance$claim_datetime <- as_datetime(insurance$claim_datetime)
insurance$claim_date <- as_date(insurance$claim_datetime)
insurance$claim_hour <- hour(insurance$claim_datetime)
insurance$claim_wday <- wday(insurance$claim_datetime, week_start = 1)

insurance$incident_date <- insurance$incident$date
insurance$incident_hour <- insurance$incident$hour
insurance$incident_type <- insurance$incident$type
insurance$incident_severity <- insurance$incident$severity

insurance$incident_date <- as_date(insurance$incident_date, format = "%d-%m-%Y")

dates_check_frame <- insurance[, c("claim_date", "incident_date")]
dates_check_frame$differenceDays <- dates_check_frame$claim_date - dates_check_frame$incident_date
insurance$claim_date[which(dates_check_frame$differenceDays < 0)] <- NA
insurance$incident_date[which(dates_check_frame$differenceDays < 0)] <- NA

# difference between claim date and incident date in days
insurance$diff_ci_days <- insurance$claim_date - insurance$incident_date

insurance$incident_wday <- wday(insurance$incident_date, week_start = 1)
insurance$incident_type <- factor(insurance$incident_type)

insurance$incident_severity <- factor(insurance$incident_severity, 
                                      levels = c("Trivial Damage", "Minor Damage", 
                                                 "Major Damage", "Total Loss"), 
                                      ordered = T)

#insurance$collision_type <- insurance$collision$type
insurance$vehicles_involved <- insurance$collision$number_of_vehicles_involved

#insurance$collision_type <- factor(insurance$collision_type)

insurance$collision_type <- insurance$collision$type
insurance$collision_type <- replace(insurance$collision_type, which(is.na(insurance$collision_type)), "unknown")
insurance$collision_type <- factor(insurance$collision_type)

insurance$driver_age <- insurance$driver$age
insurance$insured_rel <- insurance$driver$insured_relationship
insurance$license_issue_d <- insurance$driver$license_issue_date

# New NA's for the variable driver_age
insurance$driver_age[which(insurance$driver_age == 0)] <- NA
insurance$driver_age[which(insurance$driver_age < 15)] <- NA
insurance$driver_age[which(insurance$driver_age >= 100)] <- NA
insurance$insured_rel <- factor(insurance$insured_rel)

without_na <- which(!is.na(insurance$license_issue_d))
problems_index <- numeric()
for (i in 1:length(insurance$license_issue_d[without_na])) {
  if (length(unlist(strsplit(insurance$license_issue_d[without_na][i], "-"))) == 1) {
    problems_index <- c(problems_index, i)
  }
}

insurance$license_issue_d[without_na][problems_index] <- NA
insurance$license_issue_d <- as_date(insurance$license_issue_d, format = "%d-%m-%Y")
insurance$license_issue_d[which(year(insurance$license_issue_d) == 1900)] <- NA
insurance$license_issue_d[which(year(insurance$license_issue_d) == 9740)] <- NA
insurance$license_issue_d[which(year(insurance$license_issue_d) > 2020)] <- NA
# Adding new variable with the difference between incident date and the license issue date
insurance$diff_li_days <- insurance$incident_date - insurance$license_issue_d
insurance$diff_li_days[which(insurance$diff_li_days < 0)] <- NA

# Disaggregating the variable claim_amount
insurance$claim_amount_total <- insurance$claim_amount$total
insurance$claim_amount_injury <- insurance$claim_amount$injury
insurance$claim_amount_property <- insurance$claim_amount$property
insurance$claim_amount_vehicle <- insurance$claim_amount$vehicle
# Process tartget variable suspicious_activity
insurance$suspicious_activity <- replace(insurance$suspicious_activity, which(insurance$suspicious_activity == TRUE), 1)
insurance$suspicious_activity <- factor(insurance$suspicious_activity)

# Process of the variable months as customer (processed data renamed as months_cust2)
index_frame <- insurance[, c("driver_age", "months_as_customer")]
index_frame$years_as_customer <- index_frame$months_as_customer/12
index_frame$check <- numeric(dim(index_frame)[1])
# If the difference between the driver's age and the years_as_customer variable is bigger or 
# equal than 16 the check value takes the value 1 and 0 otherwise.
for (i in which(!is.na(index_frame$driver_age))) {
  if ((index_frame$driver_age[i] - index_frame$years_as_customer[i]) >= 16) {
    index_frame$check[i] <- 1
  }
}

insurance$months_cust2[which(index_frame$check == 0)] <- NA

# POLICY_TYPE to factor
insurance$POLICYTYPE <- as.factor(insurance$POLICYTYPE)

# POLICY_ISSUE_DATE to date (lubridate)
insurance$POL_ISSUE_DATE <- as_date(insurance$POL_ISSUE_DATE, format  = "%d-%m-%Y")
