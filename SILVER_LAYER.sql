USE ROLE ROLE_TEAM_QUERYQUEST;
USE DATABASE DB_TEAM_QUERYQUEST;
USE WAREHOUSE Animal_Task_WH;


-- Create Silver layer schema
CREATE SCHEMA IF NOT EXISTS SILVER;

USE SCHEMA SILVER;

-- Create Sequences for the Dimension tables 
CREATE OR REPLACE SEQUENCE seq_date_sk START WITH 1 INCREMENT BY 1;
CREATE OR REPLACE SEQUENCE seq_diagnosis_sk START WITH 100 INCREMENT BY 5;
CREATE OR REPLACE SEQUENCE seq_treatment_sk START WITH 200 INCREMENT BY 7;
CREATE OR REPLACE SEQUENCE seq_review_sk START WITH 300 INCREMENT BY 2;
CREATE OR REPLACE SEQUENCE seq_patient_sk START WITH 400 INCREMENT BY 1;


-- ============================================
-- DIMENSION TABLES
-- ============================================

-- 1. DIM_DIAGNOSIS
CREATE OR REPLACE TABLE DIM_DIAGNOSIS (
    Diagnosis_SK INT PRIMARY KEY,
    Diagnosis_Category VARCHAR(255) NOT NULL, 
    Diagnosis_Sub_Category VARCHAR(255) NOT NULL, 
    CONSTRAINT UK_DIAGNOSIS UNIQUE (Diagnosis_Category, Diagnosis_Sub_Category)
);

-- 2. DIM_TREATMENT
CREATE OR REPLACE TABLE DIM_TREATMENT (
    Treatment_SK INT PRIMARY KEY,
    Treatment_Category VARCHAR(255) NOT NULL, 
    Treatment_Sub_Category VARCHAR(255) NOT NULL, 
    CONSTRAINT UK_TREATMENT UNIQUE (Treatment_Category, Treatment_Sub_Category)
);

-- 3. DIM_PATIENT
CREATE OR REPLACE TABLE DIM_PATIENT (
    Patient_SK INT PRIMARY KEY,
    Age_Range VARCHAR(50) NOT NULL, 
    Patient_Gender VARCHAR(50) NOT NULL, 
    CONSTRAINT UK_PATIENT UNIQUE (Age_Range, Patient_Gender)
);

-- 4. DIM_REVIEW
CREATE OR REPLACE TABLE DIM_REVIEW (
    Review_SK INT PRIMARY KEY,
    Review_Type VARCHAR(255) NOT NULL, 
    Determination VARCHAR(255) NOT NULL, 
    CONSTRAINT UK_REVIEW UNIQUE (Review_Type, Determination)
);

-- 5. DIM_DATE
CREATE OR REPLACE TABLE DIM_DATE (
    Date_SK INT PRIMARY KEY,
    Report_Year INT NOT NULL,
    Decade INT NOT NULL,
    Four_Year_Bin VARCHAR(20) NOT NULL,
    Four_Year_Bin_Start INT NOT NULL,
    Four_Year_Bin_End INT NOT NULL,
    CONSTRAINT UK_DATE UNIQUE (Report_Year)
);


-- ============================================
-- FACT TABLE
-- ============================================

CREATE OR REPLACE TABLE FACT_IMR (
    Reference_ID VARCHAR(50) PRIMARY KEY,
    Diagnosis_SK INT,
    Treatment_SK INT,
    Patient_SK INT,
    Review_SK INT,
    Date_SK INT,
    Findings TEXT,
    Sentiment FLOAT,
    Summary TEXT,
    -- Foreign Keys
    CONSTRAINT FK_DIAGNOSIS FOREIGN KEY (Diagnosis_SK) REFERENCES DIM_DIAGNOSIS(Diagnosis_SK),
    CONSTRAINT FK_TREATMENT FOREIGN KEY (Treatment_SK) REFERENCES DIM_TREATMENT(Treatment_SK),
    CONSTRAINT FK_PATIENT FOREIGN KEY (Patient_SK) REFERENCES DIM_PATIENT(Patient_SK),
    CONSTRAINT FK_REVIEW FOREIGN KEY (Review_SK) REFERENCES DIM_REVIEW(Review_SK),
    CONSTRAINT FK_DATE FOREIGN KEY (Date_SK) REFERENCES DIM_DATE(Date_SK)
);

-- ============================================
-- POPULATE DIMENSION TABLES
-- ============================================

-- Populate DIM_DIAGNOSIS
INSERT INTO DIM_DIAGNOSIS (Diagnosis_SK, Diagnosis_Category, Diagnosis_Sub_Category)
SELECT 
    seq_diagnosis_sk.NEXTVAL AS Diagnosis_SK,
    COALESCE(t.Diagnosis_Category, 'Unspecified') AS Diagnosis_Category, 
    COALESCE(t.Diagnosis_Sub_Category, 'Unspecified') AS Diagnosis_Sub_Category
FROM (
    SELECT DISTINCT TRIM(Diagnosis_Category) AS Diagnosis_Category, TRIM(Diagnosis_Sub_Category) AS Diagnosis_Sub_Category
    FROM BRONZE.IMR_RAW
) t
ORDER BY Diagnosis_Category, Diagnosis_Sub_Category;


-- Populate DIM_TREATMENT 
INSERT INTO DIM_TREATMENT (Treatment_SK, Treatment_Category, Treatment_Sub_Category)
SELECT 
    seq_treatment_sk.NEXTVAL AS Treatment_SK,
    COALESCE(t.Treatment_Category, 'Unspecified') AS Treatment_Category, 
    COALESCE(t.Treatment_Sub_Category, 'Unspecified') AS Treatment_Sub_Category
FROM (
    SELECT DISTINCT TRIM(Treatment_Category) AS Treatment_Category, TRIM(Treatment_Sub_Category) AS Treatment_Sub_Category
    FROM BRONZE.IMR_RAW
) t
ORDER BY Treatment_Category, Treatment_Sub_Category;


-- Populate DIM_PATIENT 
INSERT INTO DIM_PATIENT (Patient_SK, Age_Range, Patient_Gender)
SELECT 
    seq_patient_sk.NEXTVAL AS Patient_SK,
    COALESCE(t.Age_Range, 'Unspecified') AS Age_Range,        
    COALESCE(t.Patient_Gender, 'Unspecified') AS Patient_Gender 
FROM (
    SELECT DISTINCT TRIM(Age_Range) AS Age_Range, TRIM(Patient_Gender) AS Patient_Gender
    FROM BRONZE.IMR_RAW
) t
ORDER BY Age_Range, Patient_Gender;


-- Populate DIM_REVIEW 
INSERT INTO DIM_REVIEW (Review_SK, Review_Type, Determination)
SELECT 
    seq_review_sk.NEXTVAL AS Review_SK,
    COALESCE(t.Review_Type, 'Unspecified') AS Review_Type,         
    COALESCE(t.Determination, 'Unspecified') AS Determination     
FROM (
    SELECT DISTINCT TRIM(Review_Type) AS Review_Type, TRIM(Determination) AS Determination
    FROM BRONZE.IMR_RAW
) t
ORDER BY Review_Type, Determination;

-- Populate DIM_DATE 
INSERT INTO DIM_DATE (
    Date_SK, 
    Report_Year, 
    Decade, Four_Year_Bin, Four_Year_Bin_Start, Four_Year_Bin_End
)
SELECT 
    seq_date_sk.NEXTVAL AS Date_SK,
    Report_Year, Decade, Four_Year_Bin, Four_Year_Bin_Start, Four_Year_Bin_End
FROM (
    SELECT DISTINCT 
        CAST(Report_Year AS INT) AS Report_Year,
        FLOOR(CAST(Report_Year AS INT) / 10) * 10 AS Decade,
        CONCAT(FLOOR((CAST(Report_Year AS INT) - 2001) / 4) * 4 + 2001, '-', FLOOR((CAST(Report_Year AS INT) - 2001) / 4) * 4 + 2004) AS Four_Year_Bin,
        FLOOR((CAST(Report_Year AS INT) - 2001) / 4) * 4 + 2001 AS Four_Year_Bin_Start,
        FLOOR((CAST(Report_Year AS INT) - 2001) / 4) * 4 + 2004 AS Four_Year_Bin_End
    FROM BRONZE.IMR_RAW
    WHERE Report_Year IS NOT NULL 
    ORDER BY Report_Year
);


-- ============================================
-- POPULATE FACT TABLE
-- ============================================

INSERT INTO FACT_IMR (
    Reference_ID, Diagnosis_SK, Treatment_SK, Patient_SK, Review_SK, Date_SK, 
    Findings, Sentiment, Summary
)
SELECT 
    b.Reference_ID, dd.Diagnosis_SK, dt.Treatment_SK, dp.Patient_SK, dr.Review_SK, ddate.Date_SK, 
    b.Findings, b.Sentiment, b.Summary
FROM BRONZE.IMR_RAW b

-- DIM_DIAGNOSIS:
INNER JOIN DIM_DIAGNOSIS dd 
    ON COALESCE(TRIM(b.Diagnosis_Category), 'Unspecified') = dd.Diagnosis_Category 
    AND COALESCE(TRIM(b.Diagnosis_Sub_Category), 'Unspecified') = dd.Diagnosis_Sub_Category

-- DIM_TREATMENT: 
INNER JOIN DIM_TREATMENT dt 
    ON COALESCE(TRIM(b.Treatment_Category), 'Unspecified') = dt.Treatment_Category 
    AND COALESCE(TRIM(b.Treatment_Sub_Category), 'Unspecified') = dt.Treatment_Sub_Category

-- DIM_PATIENT:
INNER JOIN DIM_PATIENT dp 
    ON COALESCE(TRIM(b.Age_Range), 'Unspecified') = dp.Age_Range 
    AND COALESCE(TRIM(b.Patient_Gender), 'Unspecified') = dp.Patient_Gender

-- DIM_REVIEW: 
INNER JOIN DIM_REVIEW dr 
    ON COALESCE(TRIM(b.Review_Type), 'Unspecified') = dr.Review_Type 
    AND COALESCE(TRIM(b.Determination), 'Unspecified') = dr.Determination

-- DIM_DATE:
INNER JOIN DIM_DATE ddate 
    ON CAST(b.Report_Year AS INT) = ddate.Report_Year;


--Verification
SELECT * FROM DIM_DATE;
SELECT * FROM DIM_DIAGNOSIS;
SELECT * FROM DIM_PATIENT;
SELECT * FROM DIM_REVIEW;
SELECT * FROM DIM_TREATMENT;
    
SELECT * FROM FACT_IMR;

SELECT * FROM BRONZE.IMR_RAW;


-- ============================================
-- DATA QUALITY CHECKS
-- ============================================

-- 1. Verify Reference_ID uniqueness in Bronze layer
SELECT 
    'Reference_ID Uniqueness Check' AS Check_Name,
    COUNT(*) AS Total_Records,
    COUNT(DISTINCT Reference_ID) AS Unique_Reference_IDs,
    COUNT(*) - COUNT(DISTINCT Reference_ID) AS Duplicates
FROM BRONZE.IMR_RAW;

-- 2. Check row counts across layers
SELECT 'BRONZE.IMR_RAW' AS Table_Name, COUNT(*) AS Row_Count FROM BRONZE.IMR_RAW
UNION ALL
SELECT 'SILVER.FACT_IMR', COUNT(*) FROM SILVER.FACT_IMR
UNION ALL
SELECT 'SILVER.DIM_DIAGNOSIS', COUNT(*) FROM SILVER.DIM_DIAGNOSIS
UNION ALL
SELECT 'SILVER.DIM_TREATMENT', COUNT(*) FROM SILVER.DIM_TREATMENT
UNION ALL
SELECT 'SILVER.DIM_PATIENT', COUNT(*) FROM SILVER.DIM_PATIENT
UNION ALL
SELECT 'SILVER.DIM_REVIEW', COUNT(*) FROM SILVER.DIM_REVIEW
UNION ALL
SELECT 'SILVER.DIM_DATE', COUNT(*) FROM SILVER.DIM_DATE;

-- 3. Check for NULL foreign keys
SELECT 
    'NULL Diagnosis_SK' AS Issue, COUNT(*) AS Count
FROM SILVER.FACT_IMR WHERE Diagnosis_SK IS NULL
UNION ALL
SELECT 'NULL Treatment_SK', COUNT(*) FROM SILVER.FACT_IMR WHERE Treatment_SK IS NULL
UNION ALL
SELECT 'NULL Patient_SK', COUNT(*) FROM SILVER.FACT_IMR WHERE Patient_SK IS NULL
UNION ALL
SELECT 'NULL Review_SK', COUNT(*) FROM SILVER.FACT_IMR WHERE Review_SK IS NULL
UNION ALL
SELECT 'NULL Date_SK', COUNT(*) FROM SILVER.FACT_IMR WHERE Date_SK IS NULL;
