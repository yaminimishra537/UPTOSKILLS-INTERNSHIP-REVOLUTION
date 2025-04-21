SELECT * FROM corporatedata.`corporate dataset`;
use corporatedata;



-- 1) loading data from the file


LOAD DATA LOCAL INFILE 'D:/UptoSkills (Internship)/Project/Corporate Dataset.csv'
INTO TABLE `corporate dataset`
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


SELECT count(*) FROM corporatedata.`corporate dataset`;

SELECT * FROM corporatedata.`corporate dataset`;

-- -----------------------------------------------------------------------------------------------------

-- 2) creating backup/stagging data

create table corporatedata_stagging like `corporate dataset`;

select * from corporatedata_stagging;

insert corporatedata_stagging select * from `corporate dataset`;

-- -----------------------------------------------------------------------------------------------------

-- 3) Removing duplicates 

select count(*) from corporatedata_stagging;
ALTER TABLE corporatedata_stagging ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY;

with duplicated AS (
	select *,
	ROW_NUMBER() over(PARTITION BY `Company Name`,`Source / Website Link`,`Contact Number`,
    `Contact Person / Name of HR`,`Email ID`,`Designation`,`Location`,`State`,`Internship(Paid) / Job`,
    `On- Site / Remote / Hybrid` 
    ORDER BY id) 
    AS row_num
    FROM corporatedata_stagging
)

-- This shows if the row is duplicate i.e row_num = 2 indicates its duplicate 
-- now deleting duplicates

DELETE FROM corporatedata_stagging
WHERE id IN (
  SELECT id
  FROM duplicated
  WHERE row_num > 1
);

select count(*) from corporatedata_stagging;

-- -----------------------------------------------------------------------------------------------------

-- 4) Removing null values

select * from corporatedata_stagging;

-- 'NA', 'N/A', 'nil', 'Not found', 'NL', '-', ''

-- This are the null values present in our data

SELECT *
FROM corporatedata_stagging
WHERE 
  `Company Name` IN ('NA', 'N/A', 'nil', 'Not found', 'NL', '-', '') OR
  `Source / Website Link` IN ('NA', 'N/A', 'nil', 'Not found', 'NL', '-', '') OR
  `Contact Number` IN ('NA', 'N/A', 'nil', 'Not found', 'NL', '-', '') OR
  `Contact Person / Name of HR` IN ('NA', 'N/A', 'nil', 'Not found', 'NL', '-', '') OR
  `Email ID` IN ('NA', 'N/A', 'nil', 'Not found', 'NL', '-', '') OR
  `Designation` IN ('NA', 'N/A', 'nil', 'Not found', 'NL', '-', '') OR
  `Location` IN ('NA', 'N/A', 'nil', 'Not found', 'NL', '-', '') OR
  `State` IN ('NA', 'N/A', 'nil', 'Not found', 'NL', '-', '') OR
  `Internship(Paid) / Job` IN ('NA', 'N/A', 'nil', 'Not found', 'NL', '-', '') OR
  `On- Site / Remote / Hybrid` IN ('NA', 'N/A', 'nil', 'Not found', 'NL', '-', '');


-- Now lets delete this data rows having this null values

DELETE FROM corporatedata_stagging
WHERE 
  `Company Name` IN ('NA', 'N/A', 'nil', 'Not found', 'NL', '-', '') OR
  `Source / Website Link` IN ('NA', 'N/A', 'nil', 'Not found', 'NL', '-', '') OR
  `Contact Number` IN ('NA', 'N/A', 'nil', 'Not found', 'NL', '-', '') OR
  `Contact Person / Name of HR` IN ('NA', 'N/A', 'nil', 'Not found', 'NL', '-', '') OR
  `Email ID` IN ('NA', 'N/A', 'nil', 'Not found', 'NL', '-', '') OR
  `Designation` IN ('NA', 'N/A', 'nil', 'Not found', 'NL', '-', '') OR
  `Location` IN ('NA', 'N/A', 'nil', 'Not found', 'NL', '-', '') OR
  `State` IN ('NA', 'N/A', 'nil', 'Not found', 'NL', '-', '') OR
  `Internship(Paid) / Job` IN ('NA', 'N/A', 'nil', 'Not found', 'NL', '-', '') OR
  `On- Site / Remote / Hybrid` IN ('NA', 'N/A', 'nil', 'Not found', 'NL', '-', '');

select count(*) from corporatedata_stagging;

-- -----------------------------------------------------------------------------------------------------

-- 5) Standardizing data

select * from corporatedata_stagging;
-- -----------------------------------------------------------------------------------------------------
-- 5.1. (state column): correcting spellings and combining different similar groups into one

select distinct(State) from corporatedata_stagging;

-- Karanataka, karnataka -> Karnataka
-- Uttarpradesh, UTTAR PRADESH, uttar pradesh, Uttar pradesh -> Uttar Pradesh
-- Rajesthan -> Rajasthan
-- Chattisgarh, CHHATISGARH -> Chhattisgarh
-- Andra Pradesh, andra pradesh -> Andhra Pradesh
-- Telanagana, telangana -> Telangana
-- madya pradesh, MADHYA PRADESH -> Madhya Pradesh
-- HIMACHAL PRADESH -> Himachal Pradesh
-- Panjab -> Punjab
-- kerela -> Kerala
-- Orissa -> Odisha
-- Chennai-tamil nadu, Tamil nadu -> Tamil nadu
-- Maharastra, MAHARASHTRA, maharashtra -> Maharashtra
-- west Bengal, west bengal -> West Bengal
-- assam -> Assam
-- BIHAR -> Bihar
-- delhi, NEW DELHI -> Delhi
-- JHARKHAND -> Jharkhand
-- kerala -> Kerala



-- This are the changes i am performing 

UPDATE corporatedata_stagging
SET `state` = TRIM(`state`);

UPDATE corporatedata_stagging
SET state = CASE
    WHEN state IN ('Karanataka', 'karnataka') THEN 'Karnataka'
    WHEN state IN ('Uttarpradesh', 'UTTAR PRADESH', 'uttar pradesh', 'Uttar pradesh') THEN 'Uttar Pradesh'
    WHEN state IN ('Rajesthan', 'rajasthan') THEN 'Rajasthan'
    WHEN state IN ('Chattisgarh', 'CHHATISGARH') THEN 'Chhattisgarh'
    WHEN state IN ('Andra Pradesh', 'andra pradesh') THEN 'Andhra Pradesh'
    WHEN state IN ('Telanagana', 'telangana') THEN 'Telangana'
    WHEN state IN ('madya pradesh', 'MADHYA PRADESH') THEN 'Madhya Pradesh'
    WHEN state = 'HIMACHAL PRADESH' THEN 'Himachal Pradesh'
    WHEN state = 'Panjab' THEN 'Punjab'
    WHEN state = 'kerela' THEN 'Kerala'
    WHEN state = 'Orissa' THEN 'Odisha'
    WHEN state IN ('Chennai-tamil nadu', 'Tamil nadu') THEN 'Tamil Nadu'
    WHEN state IN ('Maharastra', 'maharashtra', 'MAHARASHTRA') THEN 'Maharashtra'
    WHEN state IN ('west Bengal','west bengal') THEN 'West Bengal'
    WHEN state = 'assam' THEN 'Assam'
    WHEN state = 'BIHAR' THEN 'Bihar'
    WHEN state IN ('delhi', 'NEW DELHI') THEN 'Delhi'
    WHEN state = 'JHARKHAND' THEN 'Jharkhand'
    WHEN state = 'kerala' THEN 'Kerala'
    ELSE state -- if it doesn't match, keep it as it is
END;

CREATE TABLE corporatedata_stagging_backup AS SELECT * FROM corporatedata_stagging;

-- -----------------------------------------------------------------------------------------------------

-- 5.2. ("Internship(Paid) / Job" column): combining different similar groups into one

select distinct(`Internship(Paid) / Job`) from corporatedata_stagging;


-- Intership, Intern, INTERNSHIP, STIPEND, UNPAID --> Internship
-- internship(paid), INTERNSHIP (PAID), INTERNSHIP (PAID) / JOB, internship[paid], Internship{paid], INTERNSHIP (FULL TIME), Paid --> Internship (Paid)
-- JOB (Paid), Full time Job, JOB(FULL TIME), Part time/Fulltime Job, JOB --> Job
-- Full -time, Full-time, Fulltime, FULL TIME, Full time --> Job (Full Time)
-- part time, JOB (PART-TIME) --> Job (Part Time)


-- This are the parameters i am fixing

UPDATE corporatedata_stagging
SET `Internship(Paid) / Job` = TRIM(`Internship(Paid) / Job`);

UPDATE corporatedata_stagging
SET `Internship(Paid) / Job` = CASE
    WHEN `Internship(Paid) / Job` IN ('Intership', 'Intern', 'INTERNSHIP', 'STIPEND', 'UNPAID') THEN 'Internship'
    WHEN `Internship(Paid) / Job` IN ('internship(paid)', 'INTERNSHIP (PAID)', 'INTERNSHIP (PAID) / JOB', 'internship[paid]', 'Internship{paid]', 'INTERNSHIP (FULL TIME)', 'Paid') THEN 'Internship (Paid)'
    WHEN `Internship(Paid) / Job` IN ('JOB (Paid)', 'Full time Job', 'JOB(FULL TIME)', 'Part time/Fulltime Job', 'JOB') THEN 'Job'
    WHEN `Internship(Paid) / Job` IN ('Full -time', 'Full-time', 'Fulltime', 'FULL TIME', 'Full time') THEN 'Job (Full Time)'
    WHEN `Internship(Paid) / Job` IN ('part time', 'JOB (PART-TIME)') THEN 'Job (Part Time)'
    ELSE `Internship(Paid) / Job`
END;

-- We can see that there is retailer, product development, etc so we convert it into 'Job'

UPDATE corporatedata_stagging
SET `Internship(Paid) / Job` = CASE
    WHEN `Internship(Paid) / Job` IN 
    ('Retailer', 'marketing&communication', 'operations management', 'business development',
    'retail&sales', 'product development', 'retail management', 'Retail', 'Contract', 'contract') 
    THEN 'Job'
    ELSE `Internship(Paid) / Job`
END;
-- -----------------------------------------------------------------------------------------------------


-- 5.3. ("On- Site / Remote / Hybrid" column): combining different similar groups into one

select distinct(`On- Site / Remote / Hybrid`) from corporatedata_stagging;



-- ONSITE, ON-SITE, om-site, on site, On- Site, offsite, On--Site, OFFLINE, In Person --> On-Site
-- Full-time/Part-time --> Full-time / Part-time
-- Work from home, remote --> Remote
-- hybrid/on-sitedigital transformation --> Hybrid/on-site
-- Remote/Onsite, paid --> Remote/On-Site
-- HYBRID --> Hybrid


-- This are the transformations to be performed

UPDATE corporatedata_stagging
SET `On- Site / Remote / Hybrid` = TRIM(`On- Site / Remote / Hybrid`);

UPDATE corporatedata_stagging
SET `On- Site / Remote / Hybrid` = CASE
    WHEN `On- Site / Remote / Hybrid` IN ('ONSITE', 'ON-SITE', 'om-site', 'on site', 'On- Site', 'offsite', 'On--Site', 'OFFLINE', 'In Person') THEN 'On-Site'
    WHEN `On- Site / Remote / Hybrid` = 'Full-time/Part-time' THEN 'Full-time / Part-time'
    WHEN `On- Site / Remote / Hybrid` IN ('Work from home', 'remote') THEN 'Remote'
    WHEN `On- Site / Remote / Hybrid` = 'hybrid/on-sitedigital transformation' THEN 'Hybrid/On-Site'
    WHEN `On- Site / Remote / Hybrid` IN ('Remote/Onsite', 'paid') THEN 'Remote/On-Site'
    WHEN `On- Site / Remote / Hybrid` = 'HYBRID' THEN 'Hybrid'
    ELSE `On- Site / Remote / Hybrid`
END;


CREATE TABLE corporatedata_stagging_backup2 AS SELECT * FROM corporatedata_stagging;

-- -----------------------------------------------------------------------------------------------------

-- 6. Removing extra whitespace from starting and ending
-- example- ' internship' to 'internship'

UPDATE corporatedata_stagging
SET `Company Name` = TRIM(`Company Name`),
`Source / Website Link` = TRIM(`Source / Website Link`),
`Contact Number` = TRIM(`Contact Number`),
`Contact Person / Name of HR` = TRIM(`Contact Person / Name of HR`),
`Email ID` = TRIM(`Email ID`),
`Designation` = TRIM(`Designation`);

CREATE TABLE corporatedata_stagging_backup4 AS SELECT * FROM corporatedata_stagging;

-- -----------------------------------------------------------------------------------------------------

SELECT *
FROM corporatedata_stagging
INTO OUTFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/Final data.csv'
CHARACTER SET utf8
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';