library(caret)
library(tidyverse)
library(dplyr)
library(readr)

# prepare data
churn_df <- read_csv("churn.csv")
View(churn_df)

# change churn to factor
class(churn_df$churn)
churn_df$churn <- factor(churn_df$churn)

## complete.cases -> check each row if it has missing value
mean(complete.cases(churn_df)) # if result = 1 means no missing value

# 1. split data
train_test_split <- function(data) {
  set.seed(42)
  n <- nrow(data)
  id <- sample(n, size=0.7*n)
  train_data <- data[id, ]
  test_data <- data[-id, ]
  return(list(train_data, test_data))
}

split_data <- train_test_split(churn_df)

## preProcess() before train data
train_df <- split_data[[1]]
test_df <- split_data[[2]]

## compute x_bar, x_sd
transformer <- preProcess(train_df,
                          method=c("center", "scale"))

train_df_z <- predict(transformer, train_df)
test_df_z <- predict(transformer, test_df)

## 2. train (knn: k-nearest neighbors)
set.seed(42)

## use for Recall, Precision, F1, AUC
ctrl <- trainControl(method = "cv", 
                     number = 5,
                     ## pr = precision + recall
                     summaryFunction = prSummary,
                     classProbs = TRUE)

knn_model <- train(
  churn ~ totaldaycalls + totalevecalls +
    totalnightcalls + numbercustomerservicecalls,
  data = split_data[[1]], 
  method = "knn",
  trControl = ctrl,
  tuneLength = 3
)

## 3. score
p <- predict(knn_model, newdata = test_df)

## 4. evaluate
mean(p == test_df$churn)

## confusion matrix
table(test_df$churn, p, dnn=c("actual", "preds"))

confusionMatrix(p, 
                factor(test_df$churn),
                positive="No",
                mode="prec_recall")