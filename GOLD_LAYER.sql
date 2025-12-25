----------------------------------------------------------------
-- GOLD LAYER
-- TEAM QUERY_QUEST
----------------------------------------------------------------

--define the role, database, and warehouse
use role role_team_queryquest;
use database db_team_queryquest;
use warehouse animal_task_wh;


--create gold layer
create schema if not exists gold;
use schema gold;



--USE CASE 1 - IMR Outcomes Over Time
--Tracks yearly patterns in upheld vs overturned decisions, helping identify shifts in decision fairness or review consistency.
CREATE OR REPLACE TABLE GOLD.IMR_YEAR_DETERMINATION AS
SELECT
    d.Report_Year,
    r.Determination,
    COUNT(*) AS Num_Reviews,
    AVG(f.Sentiment) AS Avg_Sentiment
FROM SILVER.FACT_IMR f
JOIN SILVER.DIM_DATE d ON f.Date_SK = d.Date_SK
JOIN SILVER.DIM_REVIEW r ON f.Review_SK = r.Review_SK
GROUP BY d.Report_Year, r.Determination
ORDER BY d.Report_Year, r.Determination
;

SELECT * FROM GOLD.IMR_YEAR_DETERMINATION LIMIT 10;
SELECT COUNT(*) FROM GOLD.IMR_YEAR_DETERMINATION;  --32



--USE CASE 2 — Clinical Patterns (Diagnosis × Treatment)
--Highlights diagnosis–treatment areas with higher overturn counts, revealing potential misalignment between policy criteria and clinical needs.
CREATE OR REPLACE TABLE GOLD.IMR_DIAG_TREAT_OUTCOME AS
SELECT
    ddate.Report_Year,
    dd.Diagnosis_Category,
    dt.Treatment_Category,
    r.Determination,
    COUNT(*) AS Num_Reviews,
    AVG(f.Sentiment) AS Avg_Sentiment
FROM SILVER.FACT_IMR f
JOIN SILVER.DIM_DATE ddate ON f.Date_SK = ddate.Date_SK
JOIN SILVER.DIM_DIAGNOSIS dd ON f.Diagnosis_SK = dd.Diagnosis_SK
JOIN SILVER.DIM_TREATMENT dt ON f.Treatment_SK = dt.Treatment_SK
JOIN SILVER.DIM_REVIEW r ON f.Review_SK = r.Review_SK
GROUP BY ddate.Report_Year, dd.Diagnosis_Category, dt.Treatment_Category, r.Determination
ORDER BY ddate.Report_Year, dd.Diagnosis_Category, dt.Treatment_Category, r.Determination
;

SELECT * FROM GOLD.IMR_DIAG_TREAT_OUTCOME LIMIT 10;
SELECT COUNT(*) FROM GOLD.IMR_DIAG_TREAT_OUTCOME;   --4219



--USE CASE 3 — Demographic Insights (Age × Gender)
--Examines outcome differences across age ranges and genders to detect potential disparities or patterns in IMR decisions.
CREATE OR REPLACE TABLE GOLD.IMR_DEMOGRAPHICS_OUTCOME AS
SELECT
    ddate.Report_Year,
    p.Age_Range,
    p.Patient_Gender,
    r.Determination,
    COUNT(*) AS Num_Reviews,
    AVG(f.Sentiment) AS Avg_Sentiment
FROM SILVER.FACT_IMR f
JOIN SILVER.DIM_DATE ddate ON f.Date_SK = ddate.Date_SK
JOIN SILVER.DIM_PATIENT p ON f.Patient_SK = p.Patient_SK
JOIN SILVER.DIM_REVIEW r ON f.Review_SK = r.Review_SK
GROUP BY ddate.Report_Year, p.Age_Range, p.Patient_Gender, r.Determination
ORDER BY ddate.Report_Year, p.Age_Range, p.Patient_Gender, r.Determination;


SELECT * FROM GOLD.IMR_DEMOGRAPHICS_OUTCOME LIMIT 10;
SELECT COUNT(*) FROM GOLD.IMR_DEMOGRAPHICS_OUTCOME; --419





