## Ask five questions from nycflight dataset

## Q1. Most flight carrier during holiday season (Dec 2013)
flights %>%
  filter(month == 12, year == 2013) %>%
  count(carrier) %>%
  arrange(desc(n)) %>%
  head(5) %>%
  left_join(airlines)

## Q2. Top 5 destination during holiday season (Dec 2013)
flights %>%
  filter(month == 12, year == 2013) %>%
  count(dest) %>%
  arrange(desc(n)) %>%
  head(5)

## Q3. Top 3 on time carrier
low_delay <- flights %>%
  select(carrier, dep_delay, arr_delay) %>%
  group_by(carrier) %>%
  summarise(avg_dep_delay = mean(dep_delay, na.rm = TRUE),
            avg_arr_delay = mean(arr_delay, na.rm = TRUE),
            total_delay = avg_dep_delay + avg_arr_delay) %>%
  arrange(total_delay)

ontime_airlines <- low_delay %>%
  left_join(airlines, by = "carrier")

ontime_airlines %>%
  select(name, avg_dep_delay, avg_arr_delay, total_delay) %>%
  filter(total_delay >= 0) %>%
  head(3)

## Q4. Top 5 destinations flights often arrive late
flights %>%
  select(dest, arr_delay) %>%
  group_by(dest) %>%
  summarise(avg_arr_delay = mean(arr_delay, na.rm = TRUE)) %>%
  filter(avg_arr_delay >= 0) %>%
  arrange(desc(avg_arr_delay)) %>%
  head(5)

## Q5. Top 10 longest flight
flights %>%
  select(dest, air_time, distance) %>%
  group_by(dest) %>%
  summarise(avg_air_time = mean(air_time, na.rm = TRUE),
            avg_distance = mean(distance, na.rm = TRUE)) %>%
  arrange(desc(avg_air_time)) %>%
  head(10)