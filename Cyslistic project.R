library(tidyverse)
library(lubridate)
library(hms)
library(ggplot2)
library(scales)

## import the 12 files
df1 <- read.csv("Data/202301-divvy-tripdata.csv")
df2 <- read.csv("Data/202302-divvy-tripdata.csv")
df3 <- read.csv("Data/202303-divvy-tripdata.csv")
df4 <- read.csv("Data/202304-divvy-tripdata.csv")
df5 <- read.csv("Data/202305-divvy-tripdata.csv")
df6 <- read.csv("Data/202306-divvy-tripdata.csv")
df7 <- read.csv("Data/202307-divvy-tripdata.csv")
df8 <- read.csv("Data/202308-divvy-tripdata.csv")
df9 <- read.csv("Data/202309-divvy-tripdata.csv")
df10 <- read.csv("Data/202310-divvy-tripdata.csv")
df11 <- read.csv("Data/202311-divvy-tripdata.csv")
df12 <- read.csv("Data/202312-divvy-tripdata.csv")

## combine 12 files into one file
bike_rides <- rbind(df1,df2,df3,df4,df5,df6,df7,df8,df9,df10,df11,df12)
glimpse(bike_rides)

## convert column started_at and ended_at to date instead of character
bike_rides$started_at <- ymd_hms(bike_rides$started_at)
bike_rides$ended_at <- ymd_hms(bike_rides$ended_at)
glimpse(bike_rides)

## parse start and end time to new column
bike_rides$start_time <- as_hms(ymd_hms(bike_rides$started_at))
bike_rides$end_time <- as_hms(ymd_hms(bike_rides$ended_at))

## parse start and end date to new column
bike_rides$start_date <- as.Date(bike_rides$started_at)
bike_rides$end_date <- as.Date(bike_rides$ended_at)

## extract month from date
bike_rides$month <- format(bike_rides$start_date, "%m")

## finding trip duration
bike_rides$trip_duration <- bike_rides$end_time - bike_rides$start_time + 1

## create hour only column for later visualization
bike_rides$start_hour <- hour(bike_rides$start_time)
bike_rides$end_hour <- hour(bike_rides$end_time)

## convert date to day of week
Sys.setlocale("LC_TIME", "en_US")
bike_rides$day_of_week <- wday(bike_rides$start_date, label = TRUE, week_start = 1)

## change column name
names(bike_rides)[names(bike_rides) == 'member_casual'] <- 'membership_type'

## Visualization

## 1. Ride per week between member and casual riders
ggplot(bike_rides, 
       aes(day_of_week, fill = membership_type)) +
  geom_bar(position = "dodge") +
  scale_y_continuous(labels = comma) +
  labs(
    title = "Rides per Day of Week",
    x = "Day of Week",
    y = "Total Number of Rides"
  )

## 2. Rides per hour between member and casual riders
ggplot(bike_rides, 
       aes(start_hour, fill = membership_type)) +
  geom_bar(position = "dodge") +
  scale_y_continuous(labels = comma) +
  labs(
    title = "Count of Bike Rides per Hour",
    subtitle = "Jan - Dec 2023",
    x = "Start Hour of Ride",
    y = "Total Number of Rides"
  )

## 3. Rides per month between member and casual riders
ggplot(bike_rides, 
       aes(month, fill = membership_type)) +
  geom_bar(position = "dodge") +
  scale_y_continuous(labels = comma) +
  labs(
    title = "Rides per Month",
    subtitle = "Jan - Dec 2023",
    x = "Month",
    y = "Total Number of Rides"
  )

## 4. Which bike is popular
ggplot(bike_rides, 
       aes(rideable_type, fill = membership_type)) +
  geom_bar(position = "dodge") +
  scale_y_continuous(labels = comma) +
  labs(
    title = "Popular Bike Type",
    subtitle = "Jan - Dec 2023",
    x = "Type of Bikes",
    y = "Nummer of Rentals"
  )