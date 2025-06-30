######################################################################################################
# This script is intended for helping with the elaboration of the document databricks_insuranceRMKD. #
######################################################################################################

# TO DO: Explore the repeated policy numbers in the dataset claims

# Necessary libraries
library(ggplot2)
library(jsonlite)
library(data.table)
library(lubridate)

# Data load
policies <- read.csv("https://raw.githubusercontent.com/databricks-industry-solutions/dlt-insurance-claims/refs/heads/main/data/samples/mysql/policies.csv")

claims <- fromJSON("https://raw.githubusercontent.com/databricks-industry-solutions/dlt-insurance-claims/refs/heads/main/data/samples/mongodb/claims.json")

dim(policies)
dim(claims)

# Let's see if the datasets have some variable in common.
names(policies)
names(claims)

# Let's check if there are observations in common in the variables "POLICY_NO" and "policy_no".
length(unique(claims$policy_no))
length(unique(policies$POLICY_NO))

# More repeated values in the dataset claims?
head(sort(table(claims$policy_no), decreasing = T))

# Is the information in all the rows with the repeated policy numbers equal or different?
# Let's check first the case with more observations, the policy number: 102122085

claims[which(claims$policy_no == "102122085"), ]

# Is this policy number present in the other dataset?

which(policies$POLICY_NO == "102122085")

# Yes, only one appearance.

policies[which(policies$POLICY_NO == "102122085"), ]

# Let's inspect another repeated values

claims[which(claims$policy_no == "101511451"), ]

# Is this policy number present in the policies dataset?
policies[which(policies$POLICY_NO == "101511451"), ]

# Since it seems to be registered data in claims dataset with posterior dates to the 
# information obtained in the policies dataset, let's only use the observations which
# policy number appears only once in the claims dataset.

length(unique(policies$POLICY_NO))
length(policies$POLICY_NO) - length(unique(policies$POLICY_NO))

# In the policies dataset are also 21 repeated values, let's check if they take the same values
# for each variable.

head(sort(table(policies$POLICY_NO), decreasing = TRUE), 21)

# Checking 101610176/1

policies[which(policies$POLICY_NO == "101610176/1"), ]
# Discrepancies seen in columns BOROUGH, NEIGHBORHOOD, ZIP_CODE, PREMIUM and DEDUCTABLE.

policies[which(policies$POLICY_NO == "102000969/RI"), ]
# Discrepancies found in some columns.

policies[which(policies$POLICY_NO == "102065942"), ]
# Only discrepancies found in the columns NEIGHBORHOOD, ZIP_CODE and DEDUCTABLE.

policies[which(policies$POLICY_NO == "102098939"), ]
# Discrepancies in NEIGHBORHOOD, ZIP_CODE and DEDUCTABLE

policies[which(policies$POLICY_NO == "102107577"), ]
# Discrepancies found in ZIP_CODE

policies[which(policies$POLICY_NO == "102120560"), ]
# Discrepancies in DRV_DOB, ZIP_CODE, PRODUCT and PREMIUM.

policies[which(policies$POLICY_NO == "102134102"), ]
# Discrepancies found in BODY, DRV_DOB, ZIP_CODE and DEDUCTABLE.

# 15 observations has no policy number:
policies[which(policies$POLICY_NO == ""), ]

# Let's try to obtain this information with a single function.

duplicated_policies <- names(head(sort(table(policies$POLICY_NO), decreasing = TRUE), 8)[-1])

na_to_char <- function(input_vector) {
  # This function recieves a row of a data.frame object and if it contains NA values, it returns
  # the same vector with the NA's also as characters.
  # Input by the user is assumed to be correct.
  if (length(input_vector[which(is.na(input_vector))]) > 0) {
    input_vector[which(is.na(input_vector))] <- "NA"
  }
  return(input_vector)
}

checkDiscrepancies <- function(p_numbers) {
  # Input: p_numbers, a character vector with the policy numbers to check.
  # If both rows don't have the same values for each column, a data.frame object
  # with the content of the columns that doesn't match is printed in the console.
  for (i in p_numbers) {
    check <- policies[which(policies$POLICY_NO == i), ]
    check[1, ] <- na_to_char(check[1, ])
    check[2, ] <- na_to_char(check[2, ])
    discrepancies <- which(check[1, ] != check[2, ])
    if (length(discrepancies) > 0) {
      cat("Discrepancies for the policy number ", i, " between the two rows found at the variables:\n")
      print("---------------------------------------------------------------------------------------")
      auxiliar_frame <- data.frame(variable_name = colnames(check[discrepancies]), 
                                   row_one_values = unlist(check[1, discrepancies]), 
                                   row_two_values = unlist(check[2, discrepancies]))
      print(auxiliar_frame)
      print("---------------------------------------------------------------------------------------")
    } else {
      cat("No discrepancies were found for the policy with the number: ", i)
    }
  }
}

checkDiscrepancies(duplicated_policies)

# Are this duplicated policy numbers present in the dataset claims?
# Yes, the dataset claims contain observations for some of this repeated policies.
# Conclussion is to drop them before the merge, also with the duplicates present in the dataset
# claims.

# Let's remove now the repeated policies from both datasets.
# We don't want to conserve the unique ones, but remove all the repeated ones.
# So this code isn't value: 
# unique_pol_n <- unique(claims$policy_no)
# because it conserves one observation of for each repeated policy.

# There are policy numbers from the ducplicated ones in the dataset policies in this vector?

for (i in duplicated_policies) {
  print(which(unique_pol_n == i))
}

# Let's store first the policy numbers with repetitions

duplicated_pol_claims <- sort(table(claims$policy_no), decreasing = T)
length(duplicated_pol_claims[which(duplicated_pol_claims > 1)])
# So 4928 observations should be removed from this dataset.
duplicated_pol_claims <- duplicated_pol_claims[which(duplicated_pol_claims > 1)]
duplicated_pol_claims <- names(duplicated_pol_claims)

# We need to store the row indexes of the observations to drop from the dataset.
drop_index <- numeric()
for (i in duplicated_pol_claims) {
  drop_index <- c(drop_index, which(claims$policy_no == i))
}

# Also is necessary to remove the repeated policies observed in the dataset policies.

test <- numeric()
for (i in duplicated_policies) {
  test <- c(test, which(claims$policy_no == i))
}

print(test)
# Are this possitions already in the drop_index vector?
which(drop_index == test[1])
which(drop_index == test[2])
# Ans: no

# So let's add them
for (i in duplicated_policies) {
  drop_index <- c(drop_index, which(claims$policy_no == i))
}

# Can they be removed using only the minus sign?

dim(claims)
dim(claims[-drop_index, ])

# Before the merge, let's check how many policies of the "after drop" claims dataset are
# present in the policies dataset, id est, let's check how long will be the resulting dataset.

if (length(integer(0))) {
  print("Yes")
} else {
  print("No")
}

if (length(1888)) {
  print("yes")
} else {
  print("no")
}

future_length <- 0
for (i in claims[-drop_index, "policy_no"]) {
  check <- which(policies$POLICY_NO == i)
  if (length(check)) {
    future_length <- future_length + 1
  }
}

print(future_length)
print(dim(claims[-drop_index, ]))

# Length will be two observations less than the one of the claims dataset after 
# dropping out the duplicates.

# Let's go with the merge
insurance <- merge(claims[-drop_index, ], policies, by.x = c("policy_no"), 
                    by.y = c("POLICY_NO"), all.x = T)

# Let's check if we have the predicted length for the rows
dim(insurance)
# It looks that there are two extra rows, let's explore if this is caused because of observations
# with NA values.
which(is.na(insurance$policy_no))
# Yes, there are two observations with NA as their policy number. 
print(insurance[which(is.na(insurance$policy_no)), ])
# Since there are NA values in almost every column for this two rows, let's just delete them
# from the dataset.
insurance <- insurance[-which(is.na(insurance$policy_no)), ]

# Let's now explore each variable:

dim(insurance)[2]
names(insurance)

# 1 variable: policy_no

head(insurance$policy_no)
length(unique(insurance$policy_no))

# 2 variable: claim_no

head(insurance$claim_no)
length(unique(insurance$claim_no))

# 3 variable: claim_datetime

head(insurance$claim_datetime)
typeof(insurance$claim_datetime)

# This time we have a variable that contains dates in character format, so let's search 
# for a more appropriate data type for them.
# library(lubridate)
as_datetime(insurance$claim_datetime[1])
test <- as_datetime(insurance$claim_datetime[1])
day(test)
month(test)
year(test)
hour(test)
minutes(test)
seconds(test)

# First let's convert from character to a date-time object.
insurance$claim_datetime <- as_datetime(insurance$claim_datetime)
# And now let's store the date in one variable and the hour of the claim in another.
# Also let's store the week day associated to the claim so it could be of any help after.
insurance$claim_date <- as_date(insurance$claim_datetime)
insurance$claim_hour <- hour(insurance$claim_datetime)
insurance$claim_wday <- wday(insurance$claim_datetime, week_start = 1)
# We use the week_start option so Monday appear as the first day of week

range(insurance$claim_date)
length(unique(insurance$claim_date))
# The range of dates of the observations goes from february of 2015 to the last day of 2020.
# There are only 1633 different dates for the 40412 observations in the dataset.
# Let's observe if all the years are represented equally.
barplot(table(year(insurance$claim_date)))

# Let's create an auxiliar data.frame for using it with the ggplot function

plot_frame <- data.frame(year = sort(unique(year(insurance$claim_date))))
plot_frame$frequencies <- unname(table(year(insurance$claim_date)))

ggplot(data = plot_frame, aes(x = year, y = frequencies)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Observations for each year")

# As can be seen there are observations from 2015 to 2020. The years with more registered 
# cases are 2016 and 2017 which accumulate the 35% and the 26% of the observations respectivelly.

sort(unique(insurance$claim_hour))
length(unique(insurance$claim_hour))

# There aren't observations for every hour, specifically the hours 24, 2 and 3 are lacking without
# any observation. --Maybe the cause could be the scarcity of traffic in the streets at these hours.--
# Note: They're claim hours, not accident hours.

sort(table(insurance$claim_hour))

plot_frame <- data.frame(hour = names(table(insurance$claim_hour)))
plot_frame$rel_freqs <- as.vector(round(100*table(insurance$claim_hour)/length(insurance$claim_hour), 3))

ggplot(data = plot_frame, aes(x = hour, y = rel_freqs)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Relative frequencies for each hour")

# Let's try to repeat the plot with the hours sorted

table(insurance$claim_hour)
plot_frame <- data.frame(hour = names(table(insurance$claim_hour)))
plot_frame$rel_freqs <- as.vector(round(100*table(insurance$claim_hour)/length(insurance$claim_hour), 3))
plot_frame$hour <- as.numeric(plot_frame$hour)

ggplot(data = plot_frame, aes(x = hour, y = rel_freqs)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Relative frequencies for each hour")

# There observations concentrate in the interval from the 8 a.m. to the 7 p.m.
# It looks that this coincides with the usual commercial hours. So maybe the majority 
# of people waits to put the claim in these hours and also the majority of incidets occur
# durying day time.

table(insurance$claim_wday)
round(100*table(insurance$claim_wday)/length(insurance$claim_wday), 3)

# There are observations for each week day.

plot_frame <- data.frame(day = 1:7)
plot_frame$rel_freqs <- as.vector(round(100*table(insurance$claim_wday)/length(insurance$claim_wday), 3))


ggplot(data = plot_frame, aes(x = day, y = rel_freqs)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Relative frequencies for each week day")

# Surprisingly the day with the most claims registered is the Sunday with the 18.660% of them.
# The days with lesser claims are Friday and Saturday.

# 4 variable: incident

head(insurance$incident)

# We can see that this variable contains four variables, so let's add each one as a new column
# to the dataset insurance.

head(insurance$incident$date)

insurance$incident_date <- insurance$incident$date
insurance$incident_hour <- insurance$incident$hour
insurance$incident_type <- insurance$incident$type
insurance$incident_severity <- insurance$incident$severity

# 4.1 variable: incident_date

# Since this variable contains dates, let's change its format.
insurance$incident_date <- as_date(insurance$incident_date, format = "%d-%m-%Y")
range(insurance$incident_date)
# We can see that the range of the incidents don't coincide with the one of the claims.

plot_frame <- data.frame(year = names(table(year(insurance$incident_date))), 
                         frequencies = as.vector(unname(table(year(insurance$incident_date)))))

ggplot(data = plot_frame, aes(x = year, y = frequencies)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Incidents registered for each year")

# 2016 is again the year with more observations. But this time we have registered incidents
# as older as 2010, while older claims were of 2015. Let's explore the years of the claims
# obtained for the incidents older than 2015.
#
# Note: also obtain how many claims were put the same year of the incident. 
#       There are claims put before the incident date?

# For the data to be correct is reasonable that incident claims should be older in time or 
# as much to register the same date of the claim. So let's check this assumed property.



