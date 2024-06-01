-- Data Cleaning 
-- https://www.kaggle.com/datasets/swaptr/layoffs-2022

select *
from layoffs;

-- create a staging table. 
-- This is the one that will work in and clean the data. I need a table with the raw data in case something happens

create table layoffs_staging
like layoffs;

insert layoffs_staging
select *
from layoffs;

-- 1. Remove Dupicates
-- 2. Standardize the Data
-- 3. Null Value or blank values
-- 4. Remove Any Coulumns or Rows

-- 1. Remove Duplicates
# First let's check for duplicates

select *,
row_number() over(
partition by company,  industry, total_laid_off, percentage_laid_off, `date`) as row_num
from layoffs_staging; 	


with duplicate_cte as
(
select *,
row_number() over(
partition by company, location,
 industry, total_laid_off, percentage_laid_off, `date`, stage
 , country, funds_raised_millions) as row_num
from layoffs_staging
)
select *
from duplicate_cte
where row_num > 1;

-- let's just look at oda to confirm

select *
from layoffs_staging
where company = 'oda';

-- these are the ones we want to delete where the row number is > 1 or 2 or greater essentially

with duplicate_cte as
(
select *,
row_number() over(
partition by company, location,
 industry, total_laid_off, percentage_laid_off, `date`, stage
 , country, funds_raised_millions) as row_num
from layoffs_staging
)
delete 
from duplicate_cte
where row_num > 1;

-- one solution, which I think is a good one. Is to create a new column and add those row numbers in. Then delete where row numbers are over 2, then delete that column
-- so let's do it!!

CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

select *
from layoffs_staging2
where row_num > 1;

insert into layoffs_staging2
select *,
row_number() over(
partition by company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage
, country, funds_raised_millions) as row_num
from layoffs_staging;

-- now that we have this we can delete rows were row_num is greater than 2

delete 
from layoffs_staging2
where row_num > 1;

select *
from layoffs_staging2;


-- 2. standardizing data

select company, (trim(company))
from layoffs_staging2;

update layoffs_staging2
set company = trim(company);

select distinct industry
from layoffs_staging2;

update layoffs_staging2
set industry = 'crypto'
where induustry like 'crypto%';

-- everything looks good except apparently we have some "United States" 
-- and some "United States." with a period at the end. Let's standardize this.

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country = trim(trailing '.' from country)
where country like 'United States%';

-- we can use str to date to update this field

select `date`
from layoffs_staging2;

update layoffs_staging2
set`date` = str_to_date(`date`,'%m/%d/%Y');

Alter table layoffs_staging2
modify column `date` date;


-- 3. Look at Null Values

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

update layoffs_staging2
set industry = null
where industry = '';

select *
from layoffs_staging2
where industry is null
or industry = '';

select * 
from layoffs_staging2
where company like 'Bally%';

select t1.industry, t2.industry
from layoffs_staging2 t1
join layoffs_staging2 t2
	on t1.company = t2.company
    and t1.location = t2.location
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;

update layoffs_staging2 t1
join layoffs_staging2 t2
    on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

select *
from layoffs_staging2;

-- 4. remove any columns and rows we need to

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;
    
select *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;
    
    































