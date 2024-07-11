-- Data Cleaning

select *
from layoffs;

-- 0. Never work off of a raw dataset - make a staging dataset
-- 1. Remove Duplicates
-- 2. Standardise the data
-- 3. Null values or blank values
-- 4. Remove unneccassary columns



-- 0. 
create table layoffs_staging4
like layoffs;

select *
from layoffs_staging4;

insert layoffs_staging4
select *
from layoffs;


-- 1. 

select *
from layoffs;

select *
from layoffs_staging4;


select *,
row_number() over(
partition by company, industry, total_laid_off, percentage_laid_off, `date`) row_num
from layoffs_staging4;

with duplicate_cte as 
(
select *,
row_number() over(
partition by company, location, industry,
 total_laid_off, percentage_laid_off, `date`, 
 stage, country, funds_raised_millions) row_num
from layoffs_staging4
)
select *
from duplicate_cte
where row_num >1;


select *
from layoffs_staging4
where company = 'Casper';


with duplicate_cte as 
(
select *,
row_number() over(
partition by company, location, industry, total_laid_off,
 percentage_laid_off, `date`, stage, country, 
 funds_raised_millions) as row_num
from layoffs_staging4
)
delete
from duplicate_cte
where row_num >1;



CREATE TABLE `layoffs_staging6` (
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
from layoffs_staging4;

insert into layoffs_staging6
select *,
row_number() over(
partition by company, location, industry, total_laid_off,
 percentage_laid_off, `date`, stage, country, 
 funds_raised_millions) as row_num
from layoffs_staging4;

select *
from layoffs_staging6
where row_num > 1;

delete
from layoffs_staging6
where row_num > 1;

select *
from layoffs_staging6;


-- Standardizing Data (finding issues in your data and fixing them)

select company, trim(company)
from layoffs_staging6;

update layoffs_staging6
set company = trim(company);

select distinct industry
from layoffs_staging6
order by 1;

select *
from layoffs_staging6
where industry like 'Crypto%';

update layoffs_staging6
set industry = 'Crypto'
where industry like 'Crypto%';

select *
from layoffs_staging6;

select distinct country
from layoffs_staging6
where country like 'United States%';

select *
from layoffs_staging6
where country like 'United States%';

update layoffs_staging6
set country = 'United States'
where country like 'United States.';

select `date`,
str_to_date(`date`, '%m/%d/%Y')
from layoffs_staging6;

update layoffs_staging6
set `date` = str_to_date(`date`, '%m/%d/%Y');

alter table layoffs_staging6
modify column `date` date;

select *
from layoffs_staging6
where total_laid_off is null
and percentage_laid_off is null;



update layoffs_staging6
set industry = null
where industry = '';

select *
from layoffs_staging6
where industry is null
or industry ='';


select *
from layoffs_staging6
where company ='Airbnb';



select t1.industry, t2.industry 
from layoffs_staging6 t1
join layoffs_staging6 t2
	on t1.company = t2.company
where (t1.industry is null or t1.industry = '')
and t2.industry is not null;



update layoffs_staging6 t1
join layoffs_staging6 t2
	on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null
and t2.industry is not null;

select *
from layoffs_staging6
where company like 'Bally%';

-- deleting null rows for total laid off and percentage laid off

select *
from layoffs_staging6
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging6
where total_laid_off is null
and percentage_laid_off is null;

select *
from layoffs_staging6;

alter table layoffs_staging6
drop column row_num;