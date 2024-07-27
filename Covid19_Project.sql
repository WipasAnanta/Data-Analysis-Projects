SELECT * 
FROM Covid_deaths
ORDER BY 3,4
LIMIT 1000

SELECT * 
FROM Covid_vaccinations
ORDER BY 3,4
LIMIT 1000

-- Select the data that we are going to be using
SELECT 
  location, 
  date, 
  total_cases, 
  new_cases, 
  total_deaths, 
  population
FROM Covid_deaths
ORDER BY 1,2

-- Looking at Total Cases vs Total Deaths in Thailand
SELECT 
  location, 
  date, 
  total_cases, 
  total_deaths,
  (total_deaths/total_cases)*100 as death_percentage
FROM Covid_deaths
WHERE location = 'Thailand'
ORDER BY 1,2

-- Looking at Total Cases vs Population
-- Show percentage of population that got covid in Thailand
SELECT 
  location, 
  date, 
  total_cases, 
  population,
  (total_cases/population)*100 as percent_population_infected
FROM Covid_deaths
WHERE Location = 'Thailand'
ORDER BY 1,2

-- Looking at countries with highest infection rate compare to population
SELECT 
  location,
  population, 
  MAX(total_cases) as highest_infection_count, 
  MAX((total_cases/population))*100 as percent_population_infected
FROM Covid_deaths
GROUP BY 1,2
ORDER BY 4 DESC

SELECT 
  location,
  population,
  date, 
  MAX(total_cases) as highest_infection_count, 
  MAX((total_cases/population))*100 as percent_population_infected
FROM Covid_deaths
WHERE continent IS NOT NULL
GROUP BY 1,2,3
ORDER BY 4 DESC

-- Showing countries with highest death count per population
SELECT 
  location, 
  MAX(total_deaths) as total_death_count
FROM Covid_deaths
WHERE continent IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC

-- Break down by continent
-- Showing continents with highest death count per population
SELECT 
  continent, 
  MAX(total_deaths) as total_death_count
FROM Covid_deaths
WHERE continent IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC

-- Global number
SELECT 
  SUM(new_cases) as total_cases, 
  SUM(cast(new_deaths as int)) as total_deaths, 
  SUM(cast(new_deaths as int))/SUM(New_Cases)*100 as death_percentage
From Covid_deaths
where continent is not null 
order by 1,2

SELECT *
FROM Covid_vaccinations
LIMIT 1000

SELECT *
FROM Covid_deaths as death
JOIN Covid_vaccinations as vac
  ON death.location = vac.location
  AND death.date = vac.date

-- Looking at total population vs vaccinations
-- Shows Percentage of Population that has recieved at least one Covid Vaccine
SELECT 
  death.continent,
  death.location,
  death.date,
  death.population,
  vac.new_vaccinations,
  SUM(vac.new_vaccinations) OVER (PARTITION BY death.location ORDER BY death.location, death.date) as rolling_people_vaccinated
FROM Covid_deaths as death
JOIN Covid_vaccinations as vac
  ON death.location = vac.location
  AND death.date = vac.date
WHERE death.continent IS NOT NULL
ORDER BY 2,3

-- Use CTE because we want to use column rolling_people_vaccinated devide by population to see how many people got vaccinated
WITH PopvsVac AS (
  SELECT 
  death.continent,
  death.location,
  death.date,
  death.population,
  vac.new_vaccinations,
  SUM(vac.new_vaccinations) OVER (PARTITION BY death.location ORDER BY death.location, death.date) as rolling_people_vaccinated
FROM Covid_deaths as death
JOIN Covid_vaccinations as vac
  ON death.location = vac.location
  AND death.date = vac.date
WHERE death.continent IS NOT NULL
ORDER BY 2,3
)
SELECT *,
  (rolling_people_vaccinated/population)*100 as percent_population_vaccinated
FROM PopvsVac
