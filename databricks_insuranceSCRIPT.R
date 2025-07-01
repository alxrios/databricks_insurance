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
# as much to be registered on the same date of the claim. So let's check this assumed property.

subset_incidents <- insurance[which(year(insurance$incident_date) < 2015), c("claim_date", "incident_date")]
dim(subset_incidents)
head(subset_incidents)

# Let's plot the claim's years for this subset.

plot_frame <- data.frame(year = as.numeric(names(table(year(subset_incidents$claim_date)))))
plot_frame$frequencies <- as.vector(table(year(subset_incidents$claim_date)))

ggplot(data = plot_frame, aes(x = year, y = frequencies)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Claim year for incidents registered before 2015")

# The majority of claims are registered in 2015. Some rare cases are registered as far as 2020.

# Let's check now if all the claims were put after or the same day of the incident, since to have
# incidents registered after the claim date will not be reasonable and will indicate some kind
# problem with the data.

dates_check_frame <- insurance[, c("claim_date", "incident_date")]
dates_check_frame$differenceDays <- dates_check_frame$claim_date - dates_check_frame$incident_date
range(dates_check_frame$differenceDays)
length(which(dates_check_frame$differenceDays < 0))
# 5456 incident dates are older than the claims
hist(dates_check_frame$differenceDays)

ggplot(dates_check_frame, aes(x = as.numeric(differenceDays))) +
  geom_histogram(color="darkblue", fill="lightblue") + xlab("days") +
  ggtitle("Days of difference between claim and incident dates")

# Histogram for the observations with a less than zero difference.
ggplot(dates_check_frame[which(dates_check_frame$differenceDays < 0), ], aes(x = as.numeric(differenceDays))) +
  geom_histogram(color="darkblue", fill="lightblue", bins = 15) + xlab("days") +
  ggtitle("Days of difference between claim and incident dates")

summary(dates_check_frame$differenceDays[which(dates_check_frame$differenceDays < 0)])

# It's not possible to have claims put before the incident occurred, so let's reclassify this 
# dates as NA values.

insurance$claim_date[which(dates_check_frame$differenceDays < 0)] <- NA
insurance$incident_date[which(dates_check_frame$differenceDays < 0)] <- NA

# Let's check again the differences to ensure any negative difference remain.
dates_check_frame <- insurance[, c("claim_date", "incident_date")]
dates_check_frame$differenceDays <- dates_check_frame$claim_date - dates_check_frame$incident_date
range(dates_check_frame$differenceDays, na.rm = T)
# Ok, now there aren't negative differences.

# Now let's add a new variable to the dataset insurance to store the differences between the claim
# date and the incident date, now without the observations that resulted in neggative quantities
# of days.

# difference between claim date and incident date in days
insurance$diff_ci_days <- insurance$claim_date - insurance$incident_date
range(insurance$diff_ci_days, na.rm = T)

# Histogram for the differences between the claim date and the incident date.
ggplot(insurance[which(!is.na(insurance$diff_ci_days)), ], aes(x = as.numeric(diff_ci_days))) +
  geom_histogram(color="darkblue", fill="lightblue") + xlab("days") +
  ggtitle("Days of difference between claim and incident dates")

summary(insurance$diff_ci_days)
head(sort(table(insurance$diff_ci_days), decreasing = T), 10)

# Barplot of the incident years

plot_frame <- data.frame(year = names(table(year(insurance$incident_date))))
plot_frame$frequencies <- as.vector(table(year(insurance$incident_date)))

ggplot(data = plot_frame, aes(x = year, y = frequencies)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Incident years")


# As we did with the claim dates, let's store the day of week in which the incident was produced.
# Maybe some days are choose more than others to fake incidents.
insurance$incident_wday <- wday(insurance$incident_date, week_start = 1)

plot_frame <- data.frame(day = names(table(insurance$incident_wday)))
plot_frame$rel_freq <- as.vector(100*round(table(insurance$incident_wday)/length(insurance$incident_wday), 3))

ggplot(data = plot_frame, aes(x = day, y = rel_freq)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Relative frequencies for each week day.")

# As in the case of the claims, Friday and Saturday appear as the days with less incidents, 
# and sundays as the day with more.

# 4.2 incident_hour

length(which(is.na(insurance$incident_hour)))
table(insurance$incident_hour)

plot_frame <- data.frame(hour = 0:23)
plot_frame$frequencies <- unname(table(insurance$incident_hour))
plot_frame$rel_freq <- as.vector(round(100*table(insurance$incident_hour)/length(insurance$incident_hour), 2))

ggplot(data = plot_frame, aes(x = hour, y = rel_freq)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Relative frequencies for incident hour.")


sort(round(100*table(insurance$incident_hour)/length(insurance$incident_hour), 2), decreasing = T)

# Checking the weekdays for the incidents happended at 12:am, 3 a.m. and 23 p.m.
subset_incidents <- numeric()
for (i in c(3, 0, 23)) {
  subset_incidents <- c(subset_incidents, which(insurance$incident_hour == i))
}

# Let's check the subset it's the corrrect
unique(insurance$incident_hour[subset_incidents])

plot_frame <- data.frame(day = names(table(insurance$incident_wday[subset_incidents])))
plot_frame$frequencies <- as.vector(table(insurance$incident_wday[subset_incidents]))

ggplot(data = plot_frame, aes(x = day, y = frequencies)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Day of week frequenciess for incidents occurred at 3 a.m.,\n 12 a.m. or 23 p.m.")

# The distribution of days looks the same like the one for the whole set of hours.

# 4.3 incident_type

length(which(is.na(insurance$incident_type)))
# No NA's
unique(insurance$incident_type)

# Let's convert the variable into a factor.
insurance$incident_type <- factor(insurance$incident_type)

plot_frame <- data.frame(type = names(table(insurance$incident_type)))
plot_frame$frequencies <- as.vector(table(insurance$incident_type))

bar_labels <- as.vector(as.character(round(100*table(insurance$incident_type)/length(insurance$incident_type), 2)))
bar_labels <- paste(bar_labels, "%", sep = "")

ggplot(data = plot_frame, aes(x = type, y = frequencies)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Incident type frequencies.") + geom_text(aes(label = test_labels), vjust = 1.3)



ggplot(data = plot_frame, aes(x = type, y = frequencies), label = scales::percent(bar_labels)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Incident type frequencies.")

plot_frame$bar_labels <- as.vector(as.character(round(100*table(insurance$incident_type)/length(insurance$incident_type), 2)))

ggplot(data = plot_frame, aes(x = type, y = frequencies)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Incident type frequencies.") +
  geom_text(aes(label = scales::percent(bar_labels, scale = 1)), vjust = 1.3)







