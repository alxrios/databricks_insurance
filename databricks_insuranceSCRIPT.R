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

plot_frame$bar_labels <- as.vector(round(100*table(insurance$incident_type)/length(insurance$incident_type), 2))

ggplot(data = plot_frame, aes(x = type, y = frequencies)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Incident type frequencies.") +
  geom_text(aes(label = scales::percent(bar_labels, scale = 1)), vjust = 1.4)

# Are vehicle theft incident types more prevalent during night hours? Let's take a look.

frequencies <- table(insurance$incident_hour[which(insurance$incident_type == "Vehicle Theft")])
plot_frame <- data.frame(hour = names(frequencies))
plot_frame$frequencies <- as.vector(frequencies)

ggplot(data = plot_frame, aes(x = hour, y = frequencies)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Incidents hours for the vehicle thefts.")

sort(round(100*frequencies/sum(frequencies), 3), decreasing = T)

frequencies <- sort(round(100*frequencies/sum(frequencies), 3), decreasing = T)

plot_frame <- data.frame(hour = as.numeric(names(frequencies)))
plot_frame$frequencies <- as.vector(frequencies)

ggplot(data = plot_frame, aes(x = hour, y = frequencies)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Incidents hours for the vehicle thefts.")


# It seems that more thefts are registered at 12 a.m., 3 a.m., 23 p.m. and 17 p.m.

# 4.3 incident_severity

length(which(is.na(insurance$incident_severity)))
# No NA's.

unique(insurance$incident_severity)
# Only four different values. This variable it's an ordered categorical, so let's change its type
# to an ordered factor.

test <- sample(insurance$incident_severity, 100)
test <- factor(test, levels = c("Trivial Damage", "Minor Damage", "Major Damage", "Total Loss"), 
               ordered = T)

insurance$incident_severity <- factor(insurance$incident_severity, 
                                      levels = c("Trivial Damage", "Minor Damage", 
                                                 "Major Damage", "Total Loss"), 
                                      ordered = T)


frequencies <- round(100*table(insurance$incident_severity)/dim(insurance)[1], 2)
plot_frame <- data.frame(severity = names(frequencies))
plot_frame$frequencies <- as.vector(frequencies)

ggplot(data = plot_frame, aes(x = severity, y = frequencies)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Severity of the incident frequencies.") +
  geom_text(aes(label = scales::percent(frequencies, scale = 1)), vjust = 1.4)


# 5 collision

head(insurance$collision)
insurance$collision_type <- insurance$collision$type
insurance$vehicles_involved <- insurance$collision$number_of_vehicles_involved

# 5.1 collision_type

length(which(is.na(insurance$collision_type)))
unique(insurance$collision_type)
insurance$collision_type <- factor(insurance$collision_type)

frequencies <- round(100*table(insurance$collision_type)/dim(insurance)[1], 2)
plot_frame <- data.frame(collision_type = names(frequencies))
plot_frame$frequencies <- as.vector(frequencies)

ggplot(data = plot_frame, aes(x = collision_type, y = frequencies)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Frequencies of the types of collision.") +
  geom_text(aes(label = scales::percent(frequencies, scale = 1)), vjust = 1.4)

# We could treat NAs as unknowns?

# Let's inspect the distribution of incident types for the observations with collision type
# classified as NA.

subset_incidents <- insurance$incident_type[which(is.na(insurance$collision_type))]
frequencies <- round(100*table(subset_incidents)/length(subset_incidents), 2)
plot_frame <- data.frame(incident_type = names(frequencies))
plot_frame$frequencies <- as.vector(frequencies)

ggplot(data = plot_frame, aes(x = incident_type, y = frequencies)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Incident type frequencies for the observations with collision type missing.") +
  geom_text(aes(label = scales::percent(frequencies, scale = 1)), vjust = 1.4)

# Looks pretty similar to the distribution of the rest of the observations.
# In this case it could make sense to think that the missing observations are not just missing, 
# but also cases were the collision it's just unkown, like in a case of vandalism were the car 
# was parked in the street or a heavy impact in which the car is so damaged that it's impossible to
# know the side of the collision or because it was heavy impacted in several ways. So this time, 
# let's try to put all the missing observations into a new category that will be named as 'unknown'.

test <- insurance$collision$type
unique(test)
head(addNA(test))
unique(replace(test, which(is.na(test)), 'unknown'))
test2 <- addNA(test)
unique(test2)
length(which(is.na(test2)))

# Let's add again the content into a new column.
insurance$collision_type <- insurance$collision$type
insurance$collision_type <- replace(insurance$collision_type, which(is.na(insurance$collision_type)), "unknown")
insurance$collision_type <- factor(insurance$collision_type)
head(insurance$collision_type)

frequencies <- round(100*table(insurance$collision_type)/dim(insurance)[1], 2)
plot_frame <- data.frame(collision_type = names(frequencies))
plot_frame$frequencies <- as.vector(frequencies)

ggplot(data = plot_frame, aes(x = collision_type, y = frequencies)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Frequencies of the types of collision.") +
  geom_text(aes(label = scales::percent(frequencies, scale = 1)), vjust = 1.4) 

# 5.2 vehicles_involved

head(insurance$vehicles_involved)
length(which(is.na(insurance$vehicles_involved)))
unique(insurance$vehicles_involved)
round(100*table(insurance$vehicles_involved)/length(insurance$vehicles_involved), 2)
scales::percent(as.vector(table(insurance$vehicles_involved)/length(insurance$vehicles_involved)))
vehicles_table <- paste(round(100*table(insurance$vehicles_involved)/length(insurance$vehicles_involved), 2), "%", sep = "")

# Let's now observe the values the variable incident_type takes for each number of vehicles
# involved.

frequencies1 <- table(insurance$incident_type[which(insurance$vehicles_involved == 1)])
frequencies2 <- table(insurance$incident_type[which(insurance$vehicles_involved == 4)])
frequencies1 <- round(100*frequencies1/sum(frequencies1))
frequencies2 <- round(100*frequencies2/sum(frequencies2))
plot_frame <- data.frame(incident_type = c(names(frequencies1), names(frequencies2)))
plot_frame$frequencies <- c(as.vector(frequencies1), as.vector(frequencies2))
plot_frame$n_vehicles <- c(rep(1, 4), rep(4, 4))

ggplot(data = plot_frame, aes(x = incident_type, y = frequencies)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Frequencies of incident types by number of vehicles involved.") +
  facet_wrap(~n_vehicles) +
  geom_text(aes(label = scales::percent(frequencies, scale = 1)), hjust = 0.75) +
  coord_flip()


# 6 driver

head(insurance$driver)

# Again we have a variable that contains others, so let's add a column for each one of them.
insurance$driver_age <- insurance$driver$age
insurance$insured_rel <- insurance$driver$insured_relationship
insurance$license_issue_d <- insurance$driver$license_issue_date

# 6.1 driver_age

length(which(is.na(insurance$driver_age)))
# 950 missing values in the variable.

ggplot(insurance, aes(x = driver_age)) +
  geom_histogram(color="darkblue", fill="lightblue") + xlab("age") +
  ggtitle("Distribution of driver's age")

summary(insurance$driver_age)

# How many negatives does the variable has?

length(which(insurance$driver_age < 0))
insurance$driver_age[which(insurance$driver_age < 0)]

insurance$driver_age[which(insurance$driver_age < 0)] <- NA

length(which(insurance$driver_age == 0))

insurance$driver_age[which(insurance$driver_age == 0)] <- NA

# How many ages are less than 16?
length(which(insurance$driver_age  < 16))
insurance$driver_age[which(insurance$driver_age < 16)]
insurance$driver_age[which(insurance$driver_age == 16)]
length(which(insurance$driver_age  >= 16 & insurance$driver_age <= 18))
table(insurance$driver_age[which(insurance$driver_age  >= 16 & insurance$driver_age <= 18)])

# Values less than 15 as missing
insurance$driver_age[which(insurance$driver_age < 15)] <- NA


# Let's inspect the distribution of the variable again.

ggplot(insurance, aes(x = driver_age)) + geom_boxplot(color = "cornflowerblue") + coord_flip() +
  labs(title = "Boxplot of variable driver_age", x = "age") + 
  theme(plot.title = element_text(hjust = 0.5, face = "bold"))

summary(insurance$driver_age)

# How many observations have 100 years or more?
length(which(insurance$driver_age >= 100))

summary(insurance$driver_age[which(insurance$driver_age >= 100)])

# Values bigger or egual than 100 as missing
insurance$driver_age[which(insurance$driver_age >= 100)] <- NA

summary(insurance$driver_age)

# Distribution of upper outliers

summary(insurance$driver_age)
# 41 is the 3rd quartile
u_whisker <- 41 + sum(summary(insurance$driver_age)[c(5, 2)]%*%matrix(c(1, 0, 0, -1), nrow = 2))*1.5
outliers <- insurance$driver_age[which(insurance$driver_age > u_whisker)]
summary(outliers)

ggplot(data.frame(outliers), aes(x = outliers)) +
  geom_histogram(color="darkblue", fill="lightblue") + xlab("age") +
  ggtitle("Distribution of outlier observations for the variable driver age")


quantile(insurance$driver_age, probs = c(0.999), na.rm = T)
quantile(insurance$driver_age, probs = c(0.25, 0.5, 0.75, 0.99, 0.999), na.rm = T)

# Distribution of age change for each incident severity category?

# There were NA values for incident severity?
length(which(is.na(insurance$incident_severity)))
# Ans: no
# How many categories the variable had?
unique(insurance$incident_severity)

# Let's start with a loop over the categories with the function summary
for (i in unique(insurance$incident_severity)) {
  cat("Category: ", i, "\n")
  print(summary(insurance$driver_age[which(insurance$incident_severity == i)]))
}

# Now let's plot the histograms for each severity category

ggplot(insurance, aes(x = driver_age)) +
  geom_histogram(color="darkblue", fill="lightblue") + xlab("age") +
  ggtitle("Distribution of driver's age for each kind of incident severity") + 
  facet_wrap(~incident_severity)

# 6.2 insured_rel

length(which(is.na(insurance$insured_rel)))
# The variable has no missing observations.
unique(insurance$insured_rel)
# The variable is categorical without order, so let's change its type to factor
insurance$insured_rel <- factor(insurance$insured_rel)

# Let's plot its frequencies

frequencies <- round(100*table(insurance$insured_rel)/dim(insurance)[1], 2)
frequencies 

plot_frame <- data.frame(relative = names(frequencies))
plot_frame$frequencies <- as.vector(frequencies)

ggplot(data = plot_frame, aes(x = relative, y = frequencies)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Frequencies of the variable insured_rel.") +
  geom_text(aes(label = scales::percent(frequencies, scale = 1)), vjust = 1.2)


# 6.3 license_issue_d


head(insurance$license_issue_d)
length(which(is.na(insurance$license_issue_d)))

as_date(insurance$license_issue_d[5], format = "%d-%m-%Y")
day(as_date(insurance$license_issue_d[5], format = "%d-%m-%Y"))
month(as_date(insurance$license_issue_d[5], format = "%d-%m-%Y"))
year(as_date(insurance$license_issue_d[5], format = "%d-%m-%Y"))

head(as_date(insurance$license_issue_d, format = "%d-%m-%Y"))

insurance$license_issue_d <- as_date(insurance$license_issue_d, format = "%d-%m-%Y")

# 12 failed to parse, let's return them again to character and try to know why this is happening

insurance$license_issue_d <- insurance$driver$license_issue_date
check <- insurance$license_issue_d
typeof(check[1])
class(check[1])

check <- sort(check)
head(check)
length(unique(check))
# 6217 different dates for 40414 observations
head(sort(table(check), decreasing = T))
# 19899 observations take the value "01-01-1900" so let's directly assume that they are missing
# observations and change them to NA.
check[which(check == "01-01-1900")] <- NA
head(sort(table(check), decreasing = T))
tail(check)
# We can see that at least two errors are caused by different formatting in the dates, 
# which appear with slashes as separators instead of hyphens, also the years looks badly recorded.
# So this two observations need to be reclassified as missing.
#
# After finishing the next step, continue checking if there are more observations with slashes.
#
# Now let's split in three columns the days, months and years of the dates, to see if all of them
# are in the expected range of values.
# lic_days <- numeric()
# lic_months <- numeric()
# lic_years <- numeric()
# for (i in which(!is.na(check))) {
#   aux_split <- unlist(strsplit(check[i], "-"))
#   lic_days <- c(lic_days, aux_split[1])
#   lic_months <- c(lic_months, aux_split[2])
#   lic_years <- c(lic_years, aux_split[3])
# }

# This loop isn't going to work while having observations with different separators than -
# so let's first check how many observations are with other kinds of separators.
#
# Let's remove the NA's for the testing since they are of no interest
check <- check[which(!is.na(check))]
strsplit(check[1], "-")
strsplit(check[20026], "-")
length(strsplit(check[1], "-"))
class(strsplit(check[1], "-"))
typeof(strsplit(check[1], "-"))
length(strsplit(check[20026], "-"))
class(strsplit(check[20026], "-"))

length(unlist(strsplit(check[1], "-")))
length(unlist(strsplit(check[20026], "-")))

# Let's check all the elements that after splitting them and unlist them only have length 1.
without_na <- which(!is.na(insurance$license_issue_d))
problems_index <- numeric()
for (i in 1:length(insurance$license_issue_d[without_na])) {
  if (length(unlist(strsplit(insurance$license_issue_d[without_na][i], "-"))) == 1) {
    problems_index <- c(problems_index, i)
  }
}

length(problems_index)
check[problems_index]
insurance$license_issue_d[without_na][problems_index]

# Replacing the problematic dates with NA's
insurance$license_issue_d[without_na][problems_index] <- NA
insurance$license_issue_d <- as_date(insurance$license_issue_d, format = "%d-%m-%Y")

typeof(insurance$license_issue_d)
class(insurance$license_issue_d)

# Now let's inspect the dates
range(insurance$license_issue_d, na.rm = T)
which(insurance$license_issue_d == "9740-01-31")
insurance$license_issue_d[15853]
head(sort(table(insurance$license_issue_d), decreasing = T))
range(year(insurance$license_issue_d), na.rm = T)
head(sort(year(insurance$license_issue_d), decreasing = T))

length(which(year(insurance$license_issue_d) == 1900))

insurance$license_issue_d[which(year(insurance$license_issue_d) == 1900)] <- NA
insurance$license_issue_d[which(year(insurance$license_issue_d) == 9740)] <- NA

# Let's recalculate the range
range(insurance$license_issue_d, na.rm = T)

# From which year was the older incident registered?
max(insurance$incident_date, na.rm = T)

# There should not be license issue dates older than 2020
length(which(year(insurance$license_issue_d) > 2020))

# Let's tabulate them
sort(insurance$license_issue_d[which(year(insurance$license_issue_d) > 2020)], decreasing = T)

# Range of claim dates:
range(insurance$claim_date, na.rm = T)

# Let's put all the license issue dates posterior to 2020 to NA.
insurance$license_issue_d[which(year(insurance$license_issue_d) > 2020)] <- NA
range(insurance$license_issue_d, na.rm = T)

# Now that the variable seems to be cleaner, let's continue analyzing it a little more
#
# First let's observe the distribution of the years of the variable
length(unique(year(insurance$license_issue_d)))
# 57 different years in the variable.
frequencies <- sort(table(year(insurance$license_issue_d)))
plot_frame <- data.frame(year = names(frequencies))
plot_frame$frequency <- as.vector(frequencies)

ggplot(data = plot_frame, aes(x = year, y = frequency)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Observations for each year") + coord_flip()

length(which(is.na(insurance$license_issue_d)))

# Now let's try to plot the graph again but with the years sorted
order(plot_frame$year)
plot_frame[order(plot_frame$year), ]

ggplot(data = plot_frame[order(plot_frame$year), ], aes(x = year, y = frequency)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("License issued by year") + coord_flip()


test <- plot_frame[order(plot_frame$year, decreasing = F), ]
test$year <- as.numeric(test$year)
ggplot(data = test, aes(x = year, y = frequency)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("License issued by year") + coord_flip()


plot_frame$year <- as.numeric(plot_frame$year)

# Let's try with the relative ones
frequencies <- sort(round(100*table(year(insurance$license_issue_d))/40416, 2))
plot_frame <- data.frame(year = names(frequencies))
plot_frame$frequency <- as.vector(frequencies)
plot_frame$year <- as.numeric(plot_frame$year)

ggplot(data = plot_frame[order(plot_frame$year), ], aes(x = year, y = frequency)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("License issued by year") + coord_flip()

# Are all the incident dates older than the license issue date?
check_frame <- insurance[, c("incident_date", "license_issue_d")]
dim(check_frame)
check_frame$diff_dates <- check_frame$incident_date - check_frame$license_issue_d
range(check_frame$diff_dates, na.rm = T)
# There are negative differences, so there are incidents that were registered before the 
# date of the license issuing.

ggplot(check_frame, aes(x = as.numeric(diff_dates))) +
  geom_histogram(color="darkblue", fill="lightblue") + xlab("days") +
  ggtitle("Days of difference between incident and license issue dates")

length(which(check_frame$diff_dates < 0))

summary(check_frame$diff_dates[which(check_frame$diff_dates < 0)])

dim(check_frame)

# Let's put all the observations with negative differences to NA.

# First let's add a new column in the dataset insurance
insurance$diff_li_days <- insurance$incident_date - insurance$license_issue_d
range(insurance$diff_li_days, na.rm = T)
insurance$diff_li_days[which(insurance$diff_li_days < 0)] <- NA
range(insurance$diff_li_days, na.rm = T)

ggplot(insurance, aes(x = as.numeric(diff_li_days))) +
  geom_histogram(color="darkblue", fill="lightblue") + xlab("days") +
  ggtitle("Days of difference between incident and license issue dates")

summary(insurance$diff_li_days)


# 7 claim_amount

head(insurance$claim_amount)
# Again we have a variable that contains others, so let's add them to the original dataset
insurance$claim_amount_total <- insurance$claim_amount$total
insurance$claim_amount_injury <- insurance$claim_amount$injury
insurance$claim_amount_property <- insurance$claim_amount$property
insurance$claim_amount_vehicle <- insurance$claim_amount$vehicle

# 7.1 claim_amount_total
range(insurance$claim_amount_total)
summary(insurance$claim_amount_total)
length(which(is.na(insurance$claim_amount_total)))

ggplot(insurance, aes(x = claim_amount_total)) +
  geom_histogram(color="darkblue", fill="lightblue") + xlab("days") +
  ggtitle("Histogram of the variable claim amount total.")

# Bimodal distribution

# Let's check if the median for the left is near 10000 and if for the right is about 60000.

summary(insurance$claim_amount_total[which(insurance$claim_amount_total < 15000)])

ggplot(insurance[which(insurance$claim_amount_total < 15000), ], aes(x = claim_amount_total)) +
  geom_histogram(color="darkblue", fill="lightblue") + xlab("days") +
  ggtitle("Histogram of the variable claim amount total.")

# Let's try with 10000

summary(insurance$claim_amount_total[which(insurance$claim_amount_total < 10000)])

ggplot(insurance[which(insurance$claim_amount_total < 10000), ], aes(x = claim_amount_total)) +
  geom_histogram(color="darkblue", fill="lightblue") + xlab("days") +
  ggtitle("Histogram of the variable claim amount total.")

# Now let's check if the median for the right side is arround 60000
summary(insurance$claim_amount_total[which(insurance$claim_amount_total > 10000)])
ggplot(insurance[which(insurance$claim_amount_total > 10000), ], aes(x = claim_amount_total)) +
  geom_histogram(color="darkblue", fill="lightblue") + xlab("days") +
  ggtitle("Histogram of the variable claim amount total.")


# 7.2 claim_amount_injury

ggplot(insurance, aes(x = claim_amount_injury)) +
  geom_histogram(color="darkblue", fill="lightblue") + xlab("days") +
  ggtitle("Histogram of the variable claim amount injury.")

summary(insurance$claim_amount_injury)

# 7.3 claim_amount_property

summary(insurance$claim_amount_property)

ggplot(insurance, aes(x = claim_amount_property)) +
  geom_histogram(color="darkblue", fill="lightblue") + xlab("days") +
  ggtitle("Histogram of the variable claim amount property.")

# 7.4 claim_amount_vehicle


summary(insurance$claim_amount_vehicle)

ggplot(insurance, aes(x = claim_amount_vehicle)) +
  geom_histogram(color="darkblue", fill="lightblue") + xlab("days") +
  ggtitle("Histogram of the variable claim amount vehicle.")

# Now let's check if the sum of the three variables is equal to the quantity registered in
# claim amount total.

check_total <- logical(dim(insurance)[1])

for (i in 1:dim(insurance)[1]) {
  if (insurance$claim_amount_total[i] == sum(insurance[i, 46:48])) {
    check_total[i] <- TRUE
  } else {
    check_total[i] <- FALSE
  }
}

sum(check_total) == length(insurance$claim_amount_total)

# 8 number_of_witnesses

head(insurance$number_of_witnesses)
range(insurance$number_of_witnesses)
summary(insurance$number_of_witnesses)
unique(insurance$number_of_witnesses)
length(which(is.na(insurance$number_of_witnesses)))

plot_frame <- data.frame(witnesses = sort(unique(insurance$number_of_witnesses)))
plot_frame$frequencies <- as.vector(unname(table(insurance$number_of_witnesses)))

ggplot(data = plot_frame, aes(x = witnesses, y = frequencies)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Frequencies of witnesses")

# Repeat the plot with the relative frequencies.

plot_frame <- data.frame(witnesses = sort(unique(insurance$number_of_witnesses)))
plot_frame$frequencies <- round(100*as.vector(unname(table(insurance$number_of_witnesses)))/length(insurance$number_of_witnesses), 2)

ggplot(data = plot_frame, aes(x = witnesses, y = frequencies)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Relative requencies of witnesses") +
  geom_text(aes(label = scales::percent(frequencies, scale = 1)), vjust = 1.2)

# Let's explore the incident types for the observations with 0 witnesses.

table(insurance$incident_type[which(insurance$number_of_witnesses == 0)])
round(100*table(insurance$incident_type[which(insurance$number_of_witnesses == 0)])/dim(insurance)[1], 2)

# Let's explore the incidents for the rest of the number of witnesses registered
round(100*table(insurance$incident_type[which(insurance$number_of_witnesses == 1)])/dim(insurance)[1], 2)
round(100*table(insurance$incident_type[which(insurance$number_of_witnesses == 2)])/dim(insurance)[1], 2)
round(100*table(insurance$incident_type[which(insurance$number_of_witnesses == 3)])/dim(insurance)[1], 2)

# 9 suspicious_activity

head(insurance$suspicious_activity)
length(which(is.na(insurance$suspicious_activity)))

# Has no NA's.

plot_frame <- data.frame(suspicious = sort(unique(insurance$suspicious_activity)))
plot_frame$frequencies <- round(100*as.vector(unname(table(insurance$suspicious_activity)))/length(insurance$suspicious_activity), 2)

ggplot(data = plot_frame, aes(x = suspicious, y = frequencies)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Relative requencies of suspicious activity") +
  geom_text(aes(label = scales::percent(frequencies, scale = 1)), vjust = 1.2)

# Let's test replacing TRUE and FALSE values with the function replace.

replace(c(TRUE, FALSE), which(c(TRUE, FALSE) == TRUE), 1)
test <- insurance$suspicious_activity
test <- replace(test, which(test == TRUE), 1)
unique(test)
table(test)
table(test)/length(test)

# It works, so let's apply it on the dataset

insurance$suspicious_activity <- replace(insurance$suspicious_activity, which(insurance$suspicious_activity == TRUE), 1)
table(insurance$suspicious_activity)
table(insurance$suspicious_activity)/dim(insurance)[1]

# Convert it to factor

insurance$suspicious_activity <- factor(insurance$suspicious_activity)
head(insurance$suspicious_activity)

# 10 months_as_customer

head(insurance$months_as_customer)
length(which(is.na(insurance$months_as_customer)))
# No NA's.
range(insurance$months_as_customer)
summary(insurance$months_as_customer)
# This variable should be less than the driver's age

ggplot(insurance, aes(x = months_as_customer)) +
  geom_histogram(color="darkblue", fill="lightblue") + xlab("days") +
  ggtitle("Histogram of the variable months as customer.")

ggplot(insurance, aes(x = months_as_customer)) + geom_boxplot(color = "cornflowerblue") + coord_flip() +
  labs(title = "Boxplot of variable months_as_customer", x = "months") + 
  theme(plot.title = element_text(hjust = 0.5, face = "bold"))

# Let's check if drivers ages are take higher values than the variable months_as_customer for
# all the observations.
head(insurance$driver_age)
summary(insurance$driver_age)

check_frame <- insurance[which(!is.na(insurance$driver_age)), c("driver_age", "months_as_customer")]
summary(check_frame)
summary(insurance[, c("driver_age", "months_as_customer")])
# Ok, we don't selected any NA values.

# Now let's convert months into years.
check_frame$years_as_customer <- check_frame$months_as_customer/12
# First let's check if driver_age < years_as_customer for all the observations.
check_frame$check_less <- rep(0L, dim(check_frame)[1])
# Now let's do the loop
for (i in 1:length(check_frame$driver_age)) {
  if (check_frame$years_as_customer[i] < check_frame$driver_age[i]) {
    check_frame$check_less[i] <- 1L
  }
}

sum(check_frame$check_less)
dim(check_frame)[1] - sum(check_frame$check_less)
# 29394 observations are ok
# 2990 are not ok
round(100*2990/29394, 2)
# This is a 10% of the observations (after removing the NA's in driver age)
head(check_frame[which(check_frame$check_less == 0), ], 20)

# This months should be classified as missing, since their information is not consistent with
# the one in driver's age.
#
# Now let's check again, but this time let's demand the difference between the driver_age and
# the years as customer be at least bigger or equal to 16.
#
# Let's select a new frame among the observations that accounted for the previous requirement
check_frame2 <- check_frame[which(check_frame$check_less == 1), ]
check_frame2$check16 <- rep(0L, dim(check_frame2)[1])

for (i in 1:dim(check_frame2)[1]) {
  if ((check_frame2$driver_age[i] - check_frame2$years_as_customer[i]) >= 16) {
    check_frame2$check16[i] <- 1L
  }
}

sum(check_frame2$check16)
length(check_frame2$check16)
# Only 18005 of 29394 observations comply the condition.
round(100*18005/29394, 2)
# This is the 61.25% of the observations that we selected and the
round(100*18005/40416, 2)
# 44.5 % over the total number of observations.

# So finally let's put all this observations as missing.
# For not deleting the original column, let's create another one.
insurance$months_cust2 <- insurance$months_as_customer
# Auxiliar dataframe for obtaining the indexes of the observations that are going to be changed
# to NA. (check_frame is not valid because it not included the observartions that were
# previously classified as NA).
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

table(index_frame$check)
18005/22411
18005/40416

insurance$months_cust2[which(index_frame$check == 0)] <- NA
summary(insurance$months_cust2)
22411/40416

summary(insurance$months_as_customer)
summary(insurance$months_cust2)

ggplot(insurance, aes(x = months_cust2)) +
  geom_histogram(color="darkblue", fill="lightblue") + xlab("days") +
  ggtitle("Histogram of the variable months_cust2.")

# Let's try to plot both variables histograms in a panel plot.
plot_frame <- data.frame(months = c(insurance$months_as_customer, insurance$months_cust2))
plot_frame$var <- c(rep(times = 40412, "months_as_customer"), rep(times = 40412, "months_cust2"))

ggplot(plot_frame, aes(x = months)) +
  geom_histogram(color="darkblue", fill="lightblue") + xlab("days") +
  ggtitle("Distribution of months_as_customer and months_cust2.")  +
  facet_wrap(~var)

# 11 CUST_ID

head(insurance$CUST_ID)
length(unique(insurance$CUST_ID))
head(sort(table(insurance$CUST_ID), decreasing = T))
# It's hard to know with certanity what this variable contains, if they were customer id's, it
# would result in the variable containing a customer with 16708 observations, which seems 
# a pretty high number if not for a large car renting company for example. Without more 
# information let's just forget about this variable for the rest of the project.

# 12 POLICYTYPE

head(insurance$POLICYTYPE)
length(which(is.na(insurance$POLICYTYPE)))
unique(insurance$POLICYTYPE)
# Only two types of policies, third party and comprehensive.
table(insurance$POLICYTYPE)
# Very equally distributed, strange when comprehensive should have a higher cost than third
# party.

freqs <- round(100*table(insurance$POLICYTYPE)/length(insurance$POLICYTYPE), 2)
plot_frame <- data.frame(type = names(freqs))
plot_frame$frequency <- as.vector(freqs)

bar_labels <- as.vector(as.character(freqs))
bar_labels <- paste(bar_labels, "%", sep = "")

ggplot(data = plot_frame, aes(x = type, y = frequency)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Policy type frequencies") + geom_text(aes(label = bar_labels), vjust = 1.3)

# This variable should be changed to factor.

insurance$POLICYTYPE <- as.factor(insurance$POLICYTYPE)

# Does change the driver age distribution for each policy type?
summary(insurance$driver_age[which(insurance$POLICYTYPE == "TP")])
summary(insurance$driver_age[which(insurance$POLICYTYPE == "COMP")])
# The basic statistics almost don't change.

# 13 POL_ISSUE_DATE

head(insurance$POL_ISSUE_DATE)
length(which(is.na(insurance$POL_ISSUE_DATE)))
# This variable should be changed to date
test <- as_date(insurance$POL_ISSUE_DATE[1], format = "%d-%m-%Y")
year(test)
month(test)
day(test)

insurance$POL_ISSUE_DATE <- as_date(insurance$POL_ISSUE_DATE, format = "%d-%-%m-%Y")
range(insurance$POL_ISSUE_DATE, na.rm = T)
range(year(insurance$POL_ISSUE_DATE), na.rm = T)
# The conversion has failed, revise.
# Let's check that all the observations are in the same format
check_frame <- data.frame(original = insurance$POL_ISSUE_DATE)

# First let's check that all the observations contain two hyphens
sum("-" == unlist(strsplit(check_frame$original[1], "")))

check_frame$check_hyphens <- numeric(length(check_frame$original))
for (i in 1:length(check_frame$original)) {
  if (sum("-" == unlist(strsplit(check_frame$original[i], ""))) == 2){
    check_frame$check_hyphens[i] <- 1
  }
}

sum(check_frame$check_hyphens)
# All are separated by hyphens
# Now let's store the year, month and day of the date separately
check_frame$years <- numeric(length(check_frame$original))
check_frame$months <- numeric(length(check_frame$original))
check_frame$days <- numeric(length(check_frame$original))

for (i in 1:length(check_frame$original)) {
  splitted <- unlist(strsplit(check_frame$original[i], "-"))
  check_frame$day[i] <- splitted[1]
  check_frame$month[i] <- splitted[2]
  check_frame$year[i] <- splitted[3]
}

range(check_frame$check_hyphens)
range(check_frame$day)
range(check_frame$month)
range(check_frame$year)
# All seem ok

as_date(insurance$POL_ISSUE_DATE[1:500], format = "%d-%m-%Y")
test <- as_date(insurance$POL_ISSUE_DATE, format = "%d-%m-%Y")

# The error was in the previous code

insurance$POL_ISSUE_DATE <- as_date(insurance$POL_ISSUE_DATE, format  = "%d-%m-%Y")
range(insurance$POL_ISSUE_DATE)
table(year(insurance$POL_ISSUE_DATE))

plot_frame <- data.frame(year = unique(year(insurance$POL_ISSUE_DATE)))
plot_frame$frequency <- as.vector(round(100*table(year(insurance$POL_ISSUE_DATE))/length(insurance$POL_ISSUE_DATE), 2))

bar_labels <- paste(as.character(plot_frame$frequency), "%", sep = "")

ggplot(data = plot_frame, aes(x = year, y = frequency)) +
  geom_bar(stat="identity", color = "blue", fill = "white") +
  ggtitle("Frequencies of the policy issue years") + geom_text(aes(label = bar_labels), vjust = 1.3)

# 14 POL_EFF_DATE

head(insurance$POL_EFF_DATE)
length(which(is.na(insurance$POL_EFF_DATE)))
# This variable should be changed to date
# POL_EFF_DATE should be older than POL_ISSUE_DATE

# 15 POL_EXPIRY_DATE

head(insurance$POL_EXPIRY_DATE)
length(which(is.na(insurance$POL_EXPIRY_DATE)))

# This variable should be changed to date.
# POL_EXPIRY_DATE should be older than POL_ISSUE_DATE and POL_EFF_DATE

# 16 BODY

head(insurance$BODY)
# "" should be reclassified as NA's
length(which(is.na(insurance$BODY)))
# No previous NA's
length(unique(insurance$BODY))
# 75 different categories which should be reduced after changing "" to NA.
# This variable should be changed to factor.

# 17 MAKE

head(insurance$MAKE)
length(which(is.na(insurance$MAKE)))
# No previous NA's
# This variable should be changed to factor.
length(unique(insurance$MAKE))
# 171 different categories before making any change


