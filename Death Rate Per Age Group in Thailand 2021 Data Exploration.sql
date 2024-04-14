SELECT *
FROM `powerful-gizmo-418904.portfolio_deaths_age.deaths_age` 
LIMIT 1000;

SELECT *
FROM `powerful-gizmo-418904.portfolio_deaths_age.pop_total` 
LIMIT 1000;

-----------------------------------------------------
SELECT 
 SUM(deaths_age_90_99) as age_90_99,
 SUM(deaths_age_80_89) as age_80_89,
 SUM(deaths_age_70_79) as age_70_79,
 SUM(deaths_age_60_69) as age_60_69,
 SUM(deaths_age_50_59) as age_50_59,
 SUM(deaths_age_40_49) as age_40_49,
 SUM(deaths_age_30_39) as age_30_39,
 SUM(deaths_age_20_29) as age_20_29,
 SUM(deaths_age_10_19) as age_10_19,
 SUM(deaths_age_0_9) as age_0_9
FROM `powerful-gizmo-418904.portfolio_deaths_age.deaths_age` 
WHERE Entity = 'Thailand';

-----------------------------------------------------

WITH age_deaths as (
  SELECT
 SUM(deaths_age_90_99) as age_90_99,
 SUM(deaths_age_80_89) as age_80_89,
 SUM(deaths_age_70_79) as age_70_79,
 SUM(deaths_age_60_69) as age_60_69,
 SUM(deaths_age_50_59) as age_50_59,
 SUM(deaths_age_40_49) as age_40_49,
 SUM(deaths_age_30_39) as age_30_39,
 SUM(deaths_age_20_29) as age_20_29,
 SUM(deaths_age_10_19) as age_10_19,
 SUM(deaths_age_0_9) as age_0_9,
 SUM(total_deaths) as total
 FROM `powerful-gizmo-418904.portfolio_deaths_age.deaths_age` 
WHERE Entity = 'Thailand'
)
SELECT 
  (age_0_9/total)*100 as percent_age_0_9_deaths,
  (age_10_19/total)*100 as percent_age_10_19_deaths,
  (age_20_29/total)*100 as percent_age_20_29_deaths,
  (age_30_39/total)*100 as percent_age_30_39_deaths,
  (age_40_49/total)*100 as percent_age_40_49_deaths,
  (age_50_59/total)*100 as percent_age_50_59_deaths,
  (age_60_69/total)*100 as percent_age_60_69_deaths,
  (age_70_79/total)*100 as percent_age_70_79_deaths,
  (age_80_89/total)*100 as percent_age_80_89_deaths,
  (age_90_99/total)*100 as percent_age_90_99_deaths
   FROM age_deaths

-----------------------------------------------------


SELECT
  Year,
  Entity,
  deaths_age_0_9,
  SUM(deaths_age_0_9) OVER (PARTITION BY Entity ORDER BY Entity,Year) as eiei
 FROM `powerful-gizmo-418904.portfolio_deaths_age.deaths_age` 
WHERE Entity = 'Thailand'
ORDER BY 1 ASC


-----------------------------------------------------

SELECT 
  Year,
  MAX(total_deaths) as total_death_year
 FROM `powerful-gizmo-418904.portfolio_deaths_age.deaths_age` 
 GROUP BY 1
 ORDER BY 2 DESC

-----------------------------------------------------

WITH pop_deaths AS (
SELECT
  death.Year,
  death.Entity,
  death.total_deaths,
  pop.int64_field_66 as total_pop,
  From `powerful-gizmo-418904.portfolio_deaths_age.deaths_age` as death
  JOIN `powerful-gizmo-418904.portfolio_deaths_age.pop_total` as pop
  ON pop.string_field_1 = death.Code
WHERE death.Entity = 'Thailand'
AND death.Year = 2021
)
  SELECT *,
    (total_deaths/total_pop)*100 as percent_age_deaths,
    FROM pop_deaths
