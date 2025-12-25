USE ROLE ROLE_TEAM_QUERYQUEST;
USE DATABASE DB_TEAM_QUERYQUEST;
USE WAREHOUSE Animal_Task_WH;

-- Create the schema for raw data
CREATE SCHEMA IF NOT EXISTS BRONZE;

-- Create a stage to hold the CSV files before loading
CREATE STAGE project_stage;

USE SCHEMA BRONZE;

-- Define File Format: Handles CSV specificities like headers and null values
CREATE OR REPLACE FILE FORMAT my_csv_format
    TYPE = 'CSV'
    SKIP_HEADER = 1. -- Skip the column name row
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' -- Handle commas inside text fields
    NULL_IF = ('NULL', 'null', '') -- Standardize missing data
    EMPTY_FIELD_AS_NULL = TRUE;
    
-- Create the table and load the initial historical dataset in one step
CREATE OR REPLACE TABLE IMR_RAW AS
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
    $11 AS Findings
FROM @PROJECT_STAGE/Cal_Independent_Medical_Reviews.csv
(FILE_FORMAT => my_csv_format);

-- Add columns to store AI outputs
ALTER TABLE IMR_RAW ADD COLUMN SENTIMENT FLOAT;
ALTER TABLE IMR_RAW ADD COLUMN SUMMARY STRING;

-- Compute AI insights (Sentiment & Summary) for all rows
UPDATE IMR_RAW
SET 
    SENTIMENT = SNOWFLAKE.CORTEX.SENTIMENT(Findings),
    SUMMARY = SNOWFLAKE.CORTEX.SUMMARIZE(Findings);

-- Verification
SELECT Reference_ID, Findings, SENTIMENT, SUMMARY 
FROM IMR_RAW 
LIMIT 10;

-- Build an index on the text column to allow for natural language searching
CREATE OR REPLACE CORTEX SEARCH SERVICE IMR_SEARCH_SERVICE
ON Findings
ATTRIBUTES Diagnosis_Category, Report_Year
WAREHOUSE = Animal_Task_WH
TARGET_LAG = '1 minute' -- Data freshness target
AS (
    SELECT
        Findings,
        Diagnosis_Category,
        Report_Year
    FROM BRONZE.IMR_RAW
);

-- Test Query 1: Concept search ("heart failure")
WITH search_results AS (
    SELECT PARSE_JSON(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            'IMR_SEARCH_SERVICE',
            '{
               "query": "heart failure",
               "columns": [
                  "Findings",
                  "Diagnosis_Category",
                  "Report_Year"
               ],
               "limit": 10
            }'
        )
    ) AS json_data
)
SELECT
    r.value:"Diagnosis_Category"::STRING AS Diagnosis_Category,
    r.value:"Report_Year"::STRING AS Report_Year,
    r.value:"Findings"::STRING AS Findings,
    r.value:"@scores":"cosine_similarity"::FLOAT AS cosine_similarity,
    r.value:"@scores":"text_match"::FLOAT AS text_match
FROM search_results,
     LATERAL FLATTEN(input => json_data['results']) r;


-- Test Query 2: Search with strict filtering (Year 2016)
WITH search_results AS (
    SELECT PARSE_JSON(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            'IMR_SEARCH_SERVICE',
            '{
               "query": "surgery complications",
               "columns": [
                  "Findings",
                  "Diagnosis_Category",
                  "Report_Year"
               ],
               "filter": {"@eq": {"Report_Year": "2016"} },
               "limit": 10
            }'
        )
    ) AS json_data
)
SELECT
    r.value:"Diagnosis_Category"::STRING AS Diagnosis_Category,
    r.value:"Report_Year"::STRING AS Report_Year,
    r.value:"Findings"::STRING AS Findings,
    r.value:"@scores":"cosine_similarity"::FLOAT AS cosine_similarity,
    r.value:"@scores":"text_match"::FLOAT AS text_match
FROM search_results,
     LATERAL FLATTEN(input => json_data['results']) r;

-- Test Query 3: Complex Search (Keywords + Year Filter)
-- Demonstrates searching for records containing BOTH "neurology" and "surgery"
-- while strictly filtering for the Report Year 2018.

WITH search_results AS (
    SELECT PARSE_JSON(
        SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            'IMR_SEARCH_SERVICE',
            '{
               "query": "neurology AND surgery",
               "columns": [
                  "Findings",
                  "Diagnosis_Category",
                  "Report_Year"
               ],
               "filter": {
                   "@eq": { "Report_Year": "2018" }
               },
               "limit": 10
            }'
        )
    ) AS json_data
)
SELECT
    r.value:"Diagnosis_Category"::STRING AS Diagnosis_Category,
    r.value:"Report_Year"::INT AS Report_Year,
    r.value:"Findings"::STRING AS Findings,
    r.value:"@scores":"cosine_similarity"::FLOAT AS cosine_similarity,
    r.value:"@scores":"text_match"::FLOAT AS text_match
FROM search_results,
     LATERAL FLATTEN(input => json_data['results']) r
ORDER BY cosine_similarity DESC;
