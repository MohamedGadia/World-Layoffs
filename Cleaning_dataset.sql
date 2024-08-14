SELECT * FROM world_layoffs.layoffs;
----------------------------------------------------
-- Create a copy of the table to keep the original table as it is.

CREATE TABLE world_layoffs.layoffs_staging
LIKE world_layoffs.layoffs;

INSERT world_layoffs.layoffs_staging
SELECT * FROM world_layoffs.layoffs;

----------------------------------------------------
-- now when we are data cleaning we usually follow a few steps
-- 1. check for duplicates and remove any
-- 2. standardize data and fix errors
-- 3. Look at null values and see what 
-- 4. remove any columns and rows that are not necessary - few ways

####################################################
# 1. Remove Duplicates
####################################################
SELECT 
	* 
FROM 
	world_layoffs.layoffs_staging;
----------------------------------------------------
-- check for duplicates by calculate row num 
SELECT 
    company, 
    industry, 
    total_laid_off,
    `date`,
    ROW_NUMBER() OVER (
        PARTITION BY company, industry, total_laid_off, `date`
    ) AS row_num
FROM 
    world_layoffs.layoffs_staging;

----------------------------------------------------
-- select row num > 1
SELECT *
FROM (
	SELECT company, industry, total_laid_off,`date`,
		ROW_NUMBER() OVER (
			PARTITION BY company, industry, total_laid_off,`date`
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

----------------------------------------------------
-- let's just look at oda to confirm
SELECT 
	*
FROM
	world_layoffs.layoffs_staging
WHERE
	company ='Oda';
   
----------------------------------------------------
-- it looks like these are all legitimate entries and shouldn't be deleted. We need to really look at every single row to be accurate

SELECT *
FROM (
	SELECT *,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions
		) AS row_num
	FROM 
		world_layoffs.layoffs_staging
) duplicates
WHERE 
	row_num > 1;

----------------------------------------------------
-- these are the ones we want to delete where the row number is > 1 or 2 or greater essentially

-- add row num column
ALTER TABLE world_layoffs.layoffs_staging ADD row_num INT;

SELECT 
	*
FROM
	world_layoffs.layoffs_staging; 
----------------------------------------------------
-- create new table and insert data into

CREATE TABLE `world_layoffs`.`layoffs_staging2` (
`company` text,
`location`text,
`industry`text,
`total_laid_off` INT,
`percentage_laid_off` text,
`date` text,
`stage`text,
`country` text,
`funds_raised_millions` int,
row_num INT
);

INSERT INTO `world_layoffs`.`layoffs_staging2`
(`company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
`row_num`)
SELECT `company`,
`location`,
`industry`,
`total_laid_off`,
`percentage_laid_off`,
`date`,
`stage`,
`country`,
`funds_raised_millions`,
		ROW_NUMBER() OVER (
			PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions
			) AS row_num
	FROM 
		world_layoffs.layoffs_staging;
        
----------------------------------------------------
-- Delete duplicated 
SELECT 
	*
FROM
	world_layoffs.layoffs_staging2;
    
DELETE FROM
	world_layoffs.layoffs_staging2
WHERE
	row_num >1;
    
####################################################
# 2. standardize data and fix errors
####################################################
-- Remove spaces
UPDATE world_layoffs.layoffs_staging2
SET company = TRIM(company);

----------------------------------------------------
-- I noticed the Crypto has multiple different variations. We need to standardize that - let's say all to Crypto
SELECT DISTINCT industry
FROM
	world_layoffs.layoffs_staging2;
    
SELECT industry
FROM
	world_layoffs.layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- let's say all to Crypto    
UPDATE world_layoffs.layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

----------------------------------------------------
-- Discovre spelling error in united states rows
SELECT DISTINCT country 
FROM
	world_layoffs.layoffs_staging2;
-- Fix it
UPDATE world_layoffs.layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country);

----------------------------------------------------
-- Fix data type to date column
SELECT `date` 
FROM
	world_layoffs.layoffs_staging2;
    
UPDATE world_layoffs.layoffs_staging2
SET `date` = str_to_date(`date`, '%m/%d/%Y%');

----------------------------------------------------
-- notic the data type still text , so update didn't work , i must use alter 
ALTER TABLE world_layoffs.layoffs_staging2
MODIFY COLUMN `date` DATE;

####################################################
# 3. Look at null values
####################################################
-- replace blanck value to null
UPDATE world_layoffs.layoffs_staging2
SET industry = NULL, 
    total_laid_off = NULL,
    percentage_laid_off = NULL,
    funds_raised_millions = NULL
WHERE industry = '' 
  OR total_laid_off = ''
  OR percentage_laid_off=''
  OR funds_raised_millions ='';

----------------------------------------------------
-- look to null value
SELECT
	* 
FROM
	world_layoffs.layoffs_staging2
WHERE industry IS NULL;

SELECT
	* 
FROM world_layoffs.layoffs_staging2
WHERE company LIKE 'Airbnb';

----------------------------------------------------
-- Look for null values in the industry column when the company and loacation are the same  
SELECT t1.industry, t2.industry
FROM world_layoffs.layoffs_staging2 t1
JOIN world_layoffs.layoffs_staging2 t2
ON t1.company = t2.company
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

----------------------------------------------------
-- replace null values to not null in industry column
UPDATE world_layoffs.layoffs_staging2 t1
JOIN world_layoffs.layoffs_staging2 t2
ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

####################################################
# 4. remove any columns and rows that are not necessary
####################################################

SELECT *
FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

-- Delete Useless data we can't really use
DELETE FROM world_layoffs.layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT * 
FROM world_layoffs.layoffs_staging2;

ALTER TABLE world_layoffs.layoffs_staging2
DROP COLUMN row_num;

SELECT * 
FROM world_layoffs.layoffs_staging2
