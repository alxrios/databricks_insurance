######################################################################################################
# This script is intended for helping with the elaboration of the document databricks_insuranceRMKD. #
######################################################################################################

# Necessary libraries
library(ggplot2)
library(jsonlite)

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

# Duplicate values should be removed?

# Let's start with only one case
which(claims$policy_no[1] == policies$POLICY_NO)

# Let's create a dataframe with one column with the policy numbers contained in the dataset claims
# and another column with the indexes in which they appear in the dataset policies

checkCoincide <- data.frame(pol_n = character(), policies_index = numeric())

getIndexc <- function(pol_num) {
  # Returns the index or indexes in policies which policy number coincides with the one
  # passed in the parameter pol_num.
  # pol_num must be a character single element.
  # Return is an vector integer of variable length.
  # Function parameter is assumed to be passed correctly by the user
  return(which(pol_num == policies$POLICY_NO))
}

# test
getIndexc(claims$policy_no[1])
getIndexc("333")

length(sapply(claims$policy_no[1:5], getIndexc))
sapply(claims$policy_no[1:5], getIndexc)
# How big will be the resulting dataframe?
length(sapply(claims$policy_no, getIndexc))
# Same length than the claims dataset so maybe repeated values are the ones in claims, 
# let's continue checking it.
indexes <- vapply(claims$policy_no, getIndexc, integer(1))
# Error, all results weren't of length one
#
# Let's try again with a loop
for (i in claims$policy_no) {
  indexes <- getIndexc(i)
  if (length(indexes > 0)) {
    for (k in indexes) {
      checkCoincide[dim(checkCoincide)[1] + 1,] <- c(i, k)
    }
  }
}

# Testing how to proceed inside the loop when getIndexc returns more than one observation
pol <- claims$policy_no[1] # example policy
indexes <- round(runif(5, 10000, 20000)) # example indexes
for (i in indexes) {
  checkCoincide[(dim(checkCoincide)[1] + 1),] <- c(pol, i)
}
# Maybe is better idea to make an auxiliar dataframe inside the loop, this way
# the indexes column remains being numeric

####################################################################
# Note, let's change to data.table to see if the process is faster #
####################################################################

length(unique(checkCoincide$pol_n))
length(unique(checkCoincide$policies_index))
50642 - 45342
50642 - 45344

# There are 2 less observations in the unique of the first column, so one policy would appear
# in two different rows in policies, so if we are going to use only the unique values of policy
# numbers for example this two should be eleminated also.

# Let's check now if all repeated policy_numbers in checkCoincide take the same index in the policies
# data.frame.

freqPlusOne <- sort(table(checkCoincide$pol_n), decreasing = T)
# Subsample of freqPlusOne, only policy numbers which frequency is bigger than 1
freqPlusOne2 <- data.frame(pol_n = names(freqPlusOne[which(freqPlusOne > 1)]), 
                           frequency = freqPlusOne[which(freqPlusOne > 1)])
# The previous one repeated the column with the names
freqPlusOne2 <- data.frame("frequencies" = freqPlusOne[which(freqPlusOne > 1)])
# Rename the columns
names(freqPlusOne2) <- c("pol_n", "frequencies")
# Now we have to check if for all this policy numbers, the indexes in checkCoincide (second column)
# are the same

# For that purpose let's create a new function and use vapply
checkIndex <- function(pol_num) {
  # Returns 1 if the length obtained when searching for the corresponding indexes to the
  # policy number passed as paremeter in pol_num is one and 0 otherwise.
  result <- 0 # zero by the default
  if (length(unique(checkCoincide$policies_index[which(checkCoincide$pol_n == pol_num)])) > 1) {
    result <- 1
  }
  return(result)
}

sum(vapply(freqPlusOne2$pol_n, checkIndex, numeric(1)))
# sum is 2, so yes there are 2 repeated indexes in policies (?), so one same policy at claims
# appears in two different row of policies. Let's identify them.
getIndex <- function(pol_num) {
  # Returns c(-1, -1) if the corresponding index to the policy number given as parameter to pol_num
  # are more than one in the dataset checkCoincide and return the indexes otherwise.
  result <- -1
  check <- unique(checkCoincide$policies_index[which(checkCoincide$pol_n == pol_num)])
  if (length(check) > 1 ) {
    result <- as.numeric(check)
  }
  return(result)
}

tst <- sapply(freqPlusOne2$pol_n, getIndex)
unlist(tst)[which(unlist(tst) != -1)]
# Ans: 164824 165589 187826 187827, let's observe their policy numbers
checkCoincide[which(checkCoincide$policies_index == "164824")]
checkCoincide[which(checkCoincide$policies_index == "165589")]
checkCoincide[which(checkCoincide$policies_index == "187826")]
checkCoincide[which(checkCoincide$policies_index == "187827")]

# Next let's check the values of the other variables for this observations in the dataset
# policies and claims. Are the same or different?

policies$POLICY_NO[164824]
policies$POLICY_NO[165589]
policies$CUST_ID[c(164824, 165589)]
policies$POLICYTYPE[c(164824, 165589)]

policies[c(164824, 165589),]
# For this case the changes are in the issue date, the zip_code, borough and neighborhood, 
# premium and deductable.
policies[c(187826, 187827),]
# This time the second observation seems to be the same but for some columns. The second register
# looks to have a badly codified date for the colum drv_dob. Also we can see changes in the
# zip_code  column, the product and the premium.

# Since there are badly codified columns in both cases and keeping this rows would add complexity
# for merging both datasets, let's just don't use them, so we must remove the corresponding 
# policy numbers in the claims dataset for this observations before making a merge.


