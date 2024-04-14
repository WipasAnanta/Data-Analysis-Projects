library(caret)
library(tidyverse)
library(dplyr)
library(readr)

# prepare data
churn_df <- read_csv("Data analytics/Datarockie/R programming course/churn.csv")
View(churn_df)

# change am to factor
churn_df$churn <- factor(churn_df$churn,
                         levels=c("No","Yes"),
                         labels=c(0,1)) 
str(churn_df$churn)
View(churn_df)

# split data
train_test_split <- function(data) {
  set.seed(42)
  n <- nrow(data)
  id <- sample(n, size=0.7*n)
  train_data <- data[id, ]
  test_data <- data[-id, ]
  return(list(train_data, test_data))
}

split_data <- train_test_split(churn_df)

# train model
glm_model <- train(churn ~ totaldaycalls + totalevecalls +
                     totalnightcalls + numbercustomerservicecalls,
                   data = split_data[[1]],
                   method = "glm") 

# score and evaluate
p <- predict(glm_model, newdata=split_data[[2]])

acc <- mean(p == split_data[[2]]$churn)