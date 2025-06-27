######################################################################################################
# This script is intended for helping with the elaboration of the document databricks_insuranceRMKD. #
######################################################################################################

# TO DO: Explore the repeated policy numbers in the dataset claims

# Necessary libraries
library(ggplot2)
library(jsonlite)
library(data.table)

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























































