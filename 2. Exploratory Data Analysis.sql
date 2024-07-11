-- Exploratory Data Analysis


# exploratory data analysis project - analysing the
# layoffs dataset with no real goals in mind.
# finding trends and differences in the data and 
# uncovering insights


select *
from layoffs_staging6;

# looking at the highest number of employees laid off
# and the highest percentage of employees laid off
select max(total_laid_off), max(percentage_laid_off)
from layoffs_staging6;

# looking at all of the companies that went out of business
# and ordering them by the most employees lost to least
select *
from layoffs_staging6
where percentage_laid_off = 1
order by total_laid_off desc;

# analysing the layoffs between different companies
# and ordering by most laid off to least
select company, sum(total_laid_off)
from layoffs_staging6
group by company
order by 2 desc;

# time frame of the dataset for layoffs
select min(`date`), max(`date`)
from layoffs_staging6;

# comparing different industries layoff totals
select industry, sum(total_laid_off)
from layoffs_staging6
group by industry
order by 2 desc;

# same as above but comparing different countries
select country, sum(total_laid_off)
from layoffs_staging6
group by country
order by 2 desc;

# grouping by years to see differences between
# lay offs
select year(`date`), sum(total_laid_off)
from layoffs_staging6
group by year(`date`)
order by 1 desc;


select stage, sum(total_laid_off)
from layoffs_staging6
group by stage
order by 2 desc;

select company, avg(percentage_laid_off)
from layoffs_staging6
group by company
order by 1 desc;


# number of people being laid off per month
select substring(`date`,1,7) as `month`, sum(total_laid_off)
from layoffs_staging6
where substring(`date`,1,7) is not null
group by `month`
order by 1
;

# rolling total of layoffs ordered per month
with Rolling_Total as
(
select substring(`date`,1,7) as `month`, sum(total_laid_off) as total_off
from layoffs_staging6
where substring(`date`,1,7) is not null
group by `month`
order by 1
)
select `month`, total_off,
sum(total_off) over(order by `month`) as rolling_total
from Rolling_Total;



select company, sum(total_laid_off)
from layoffs_staging6
group by company
order by 2 desc;


select company, year(`date`), sum(total_laid_off)
from layoffs_staging6
group by company, year(`date`)
order by 3 desc;

with company_year (company, years, total_laid_off) as
(
select company, year(`date`), sum(total_laid_off)
from layoffs_staging6
group by company, year(`date`)
), company_year_rank as
(select *, dense_rank() over (partition by years order by total_laid_off desc) as ranking
from company_year
where years is not null
)
select *
from company_year_rank
where ranking <= 5
;








