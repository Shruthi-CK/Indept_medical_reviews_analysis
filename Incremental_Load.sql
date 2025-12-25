USE SCHEMA BRONZE;

-- =========================================================
-- INGEST INCREMENTAL DATA
-- =========================================================

-- Load only the new file into the existing Bronze table.
-- We explicitly map columns and set AI columns to NULL initially.
COPY INTO IMR_RAW
FROM (
    SELECT
        $1 AS Reference_ID,
        $2 AS Report_Year,
        $3 AS Diagnosis_Category,
        $4 AS Diagnosis_Sub_Category,
        $5 AS Treatment_Category,
        $6 AS Treatment_Sub_Category,
        $7 AS Determination,
        $8 AS Review_Type,
        $9 AS Age_Range,
        $10 AS Patient_Gender,
        $11 AS Findings,
        NULL AS Sentiment, -- Placeholder for AI processing
        NULL AS Summary    -- Placeholder for AI processing
    FROM @PROJECT_STAGE/Incremental_Load.csv
    )
FILE_FORMAT = my_csv_format;

-- Verify new data
Select * from IMR_RAW where reference_Id like '%SAM%';

-- Only run Cortex functions on rows where Sentiment is NULL.
-- This prevents re-processing the entire historical dataset.
UPDATE IMR_RAW
SET 
    SENTIMENT = SNOWFLAKE.CORTEX.SENTIMENT(Findings),
    SUMMARY = SNOWFLAKE.CORTEX.SUMMARIZE(Findings)
WHERE SENTIMENT IS NULL;

-- =========================================================
-- UPDATE SILVER DIMENSIONS 
-- =========================================================

USE SCHEMA SILVER;

-- Strategy for all Dimensions:
-- 1. Identify distinct values in Bronze.
-- 2. Check if they already exist in Silver (NOT IN clause).
-- 3. If new, generate a new Surrogate Key (NEXTVAL) and insert.

-- Update DIM_DIAGNOSIS
INSERT INTO DIM_DIAGNOSIS (Diagnosis_SK, Diagnosis_Category, Diagnosis_Sub_Category)
SELECT 
    seq_diagnosis_sk.NEXTVAL,
    New_Diag.Diagnosis_Category,
    New_Diag.Diagnosis_Sub_Category
FROM (
    SELECT DISTINCT 
        COALESCE(TRIM(Diagnosis_Category), 'Unspecified') AS Diagnosis_Category, 
        COALESCE(TRIM(Diagnosis_Sub_Category), 'Unspecified') AS Diagnosis_Sub_Category
    FROM BRONZE.IMR_RAW
    WHERE (Diagnosis_Category, Diagnosis_Sub_Category) NOT IN (
        SELECT Diagnosis_Category, Diagnosis_Sub_Category FROM DIM_DIAGNOSIS
    )
) New_Diag;

-- Verification: Show the newly added Dimension keys
SELECT * FROM SILVER.DIM_DIAGNOSIS 
ORDER BY Diagnosis_SK DESC 
LIMIT 10;

-- Update DIM_TREATMENT
INSERT INTO DIM_TREATMENT (Treatment_SK, Treatment_Category, Treatment_Sub_Category)
SELECT 
    seq_treatment_sk.NEXTVAL,
    New_Treat.Treatment_Category,
    New_Treat.Treatment_Sub_Category
FROM (
    SELECT DISTINCT 
        COALESCE(TRIM(Treatment_Category), 'Unspecified') AS Treatment_Category, 
        COALESCE(TRIM(Treatment_Sub_Category), 'Unspecified') AS Treatment_Sub_Category
    FROM BRONZE.IMR_RAW
    WHERE (Treatment_Category, Treatment_Sub_Category) NOT IN (
        SELECT Treatment_Category, Treatment_Sub_Category FROM DIM_TREATMENT
    )
) New_Treat;

SELECT * FROM SILVER.DIM_TREATMENT
ORDER BY Treatment_SK DESC 
LIMIT 10;

-- Update DIM_PATIENT
INSERT INTO DIM_PATIENT (Patient_SK, Age_Range, Patient_Gender)
SELECT 
    seq_patient_sk.NEXTVAL,
    New_Pat.Age_Range,
    New_Pat.Patient_Gender
FROM (
    SELECT DISTINCT 
        COALESCE(TRIM(Age_Range), 'Unspecified') AS Age_Range, 
        COALESCE(TRIM(Patient_Gender), 'Unspecified') AS Patient_Gender
    FROM BRONZE.IMR_RAW
    WHERE (Age_Range, Patient_Gender) NOT IN (
        SELECT Age_Range, Patient_Gender FROM DIM_PATIENT
    )
) New_Pat;

SELECT * FROM SILVER.DIM_PATIENT
ORDER BY Patient_SK DESC 
LIMIT 1;

-- Update DIM_REVIEW
INSERT INTO DIM_REVIEW (Review_SK, Review_Type, Determination)
SELECT 
    seq_review_sk.NEXTVAL,
    New_Rev.Review_Type,
    New_Rev.Determination
FROM (
    SELECT DISTINCT 
        COALESCE(TRIM(Review_Type), 'Unspecified') AS Review_Type, 
        COALESCE(TRIM(Determination), 'Unspecified') AS Determination
    FROM BRONZE.IMR_RAW
    WHERE (Review_Type, Determination) NOT IN (
        SELECT Review_Type, Determination FROM DIM_REVIEW
    )
) New_Rev;

SELECT * FROM SILVER.DIM_REVIEW
ORDER BY Review_SK DESC 
LIMIT 4;

-- Update DIM_DATE (Handling Binning Logic)
INSERT INTO DIM_DATE (Date_SK, Report_Year, Decade, Four_Year_Bin, Four_Year_Bin_Start, Four_Year_Bin_End)
SELECT 
    seq_date_sk.NEXTVAL,
    New_Date.Report_Year,
    FLOOR(New_Date.Report_Year / 10) * 10,
    CONCAT(FLOOR((New_Date.Report_Year - 2001) / 4) * 4 + 2001, '-', FLOOR((New_Date.Report_Year - 2001) / 4) * 4 + 2004),
    FLOOR((New_Date.Report_Year - 2001) / 4) * 4 + 2001,
    FLOOR((New_Date.Report_Year - 2001) / 4) * 4 + 2004
FROM (
    SELECT DISTINCT CAST(Report_Year AS INT) AS Report_Year
    FROM BRONZE.IMR_RAW
    WHERE Report_Year IS NOT NULL 
    AND CAST(Report_Year AS INT) NOT IN (SELECT Report_Year FROM DIM_DATE)
) New_Date;

SELECT * FROM SILVER.DIM_DATE
ORDER BY Date_SK DESC 
LIMIT 6;

-- =========================================================
-- UPDATE FACT TABLE (INCREMENTAL)
-- =========================================================
-- Insert only rows where the Reference_ID does not yet exist in the Fact table.
-- We join back to Dimensions to retrieve the correct SKs (old or new).

INSERT INTO FACT_IMR (
    Reference_ID, Diagnosis_SK, Treatment_SK, Patient_SK, Review_SK, Date_SK, 
    Findings, Sentiment, Summary
)
SELECT 
    b.Reference_ID, dd.Diagnosis_SK, dt.Treatment_SK, dp.Patient_SK, dr.Review_SK, ddate.Date_SK, 
    b.Findings, b.Sentiment, b.Summary
FROM BRONZE.IMR_RAW b
-- Join to standard dimensions to get SKs
INNER JOIN DIM_DIAGNOSIS dd 
    ON COALESCE(TRIM(b.Diagnosis_Category), 'Unspecified') = dd.Diagnosis_Category 
    AND COALESCE(TRIM(b.Diagnosis_Sub_Category), 'Unspecified') = dd.Diagnosis_Sub_Category
INNER JOIN DIM_TREATMENT dt 
    ON COALESCE(TRIM(b.Treatment_Category), 'Unspecified') = dt.Treatment_Category 
    AND COALESCE(TRIM(b.Treatment_Sub_Category), 'Unspecified') = dt.Treatment_Sub_Category
INNER JOIN DIM_PATIENT dp 
    ON COALESCE(TRIM(b.Age_Range), 'Unspecified') = dp.Age_Range 
    AND COALESCE(TRIM(b.Patient_Gender), 'Unspecified') = dp.Patient_Gender
INNER JOIN DIM_REVIEW dr 
    ON COALESCE(TRIM(b.Review_Type), 'Unspecified') = dr.Review_Type 
    AND COALESCE(TRIM(b.Determination), 'Unspecified') = dr.Determination
INNER JOIN DIM_DATE ddate 
    ON CAST(b.Report_Year AS INT) = ddate.Report_Year
-- Only insert rows that don't already exist in the Fact table
WHERE b.Reference_ID NOT IN (SELECT Reference_ID FROM FACT_IMR);

SELECT * FROM SILVER.FACT_IMR
where Reference_ID like '%SAM%';

-- Verification: Check totals after load
SELECT 'After Load' AS Status, 'FACT_IMR' AS Table_Name, COUNT(*) AS Row_Count FROM SILVER.FACT_IMR;

-- =========================================================
-- REFRESH GOLD LAYER 
-- =========================================================
-- Gold tables are aggregates, so we perform a full refresh to reflect new data.

USE SCHEMA GOLD;

-- Check gold layer status (before load)
SELECT 
    'Before Load' AS Status,
    'IMR_YEAR_DETERMINATION' AS Table_Name, 
    COUNT(*) AS Aggregated_Rows,
    SUM(Num_Reviews) AS Total_Reviews_Processed
FROM GOLD.IMR_YEAR_DETERMINATION
UNION ALL
SELECT 
    'Before Load',
    'IMR_DIAG_TREAT_OUTCOME', 
    COUNT(*) AS Aggregated_Rows,
    SUM(Num_Reviews) AS Total_Reviews_Processed
FROM GOLD.IMR_DIAG_TREAT_OUTCOME
UNION ALL
SELECT 
    'Before Load',
    'IMR_DEMOGRAPHICS_OUTCOME', 
    COUNT(*) AS Aggregated_Rows,
    SUM(Num_Reviews) AS Total_Reviews_Processed
FROM GOLD.IMR_DEMOGRAPHICS_OUTCOME;

-- Rebuild Gold Tables
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
ORDER BY d.Report_Year, r.Determination;


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
ORDER BY ddate.Report_Year, dd.Diagnosis_Category, dt.Treatment_Category, r.Determination;


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

-- Final Checks: Verify gold layer status (after load)
SELECT 
    'IMR_YEAR_DETERMINATION' AS Table_Name, 
    SUM(Num_Reviews) AS Total_Reviews_Tracked 
FROM GOLD.IMR_YEAR_DETERMINATION
UNION ALL
SELECT 
    'IMR_DIAG_TREAT_OUTCOME', 
    SUM(Num_Reviews) 
FROM GOLD.IMR_DIAG_TREAT_OUTCOME
UNION ALL
SELECT 
    'IMR_DEMOGRAPHICS_OUTCOME', 
    SUM(Num_Reviews) 
FROM GOLD.IMR_DEMOGRAPHICS_OUTCOME;



