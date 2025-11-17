/*
================================================================================
SCRIPT: 04_load_silver_crm_cust_info.sql
PURPOSE: Cleanses and transforms data from the Bronze crm_cust_info table
         and inserts it into the Silver crm_cust_info table.
STRATEGY: Full Refresh (TRUNCATE and INSERT).
          This makes the job idempotent (safe to re-run).
================================================================================
*/

-- Step 1: Switch to the Silver database
USE dw_silver;

-- Step 2: Truncate the target Silver table (Full Refresh)
TRUNCATE TABLE dw_silver.crm_cust_info;

-- Step 3: Insert cleansed data from Bronze into Silver
INSERT INTO dw_silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
    -- Metadata columns (meta_created_at, meta_updated_at)
    -- will be auto-populated by their DEFAULT definitions.
)
SELECT
    -- 1. Data Type Casting: Convert VARCHAR to INT
    cast(cst_id AS SIGNED) AS cst_id,
    
    -- 2. Handle Empty Strings & Whitespace: TRIM() and convert '' to NULL
    NULLIF(TRIM(cst_key), '') AS cst_key,
    NULLIF(TRIM(cst_firstname), '') AS cst_firstname,
    NULLIF(TRIM(cst_lastname), '') AS cst_lastname,

    -- 3. Abbreviation Transformation: Convert abbreviations to full names
    CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'UnKnown' -- Set unknowns, blanks, or other values to NULL
    END AS cst_marital_status,

    CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        ELSE 'UnKnown' -- Set unknowns, blanks, or other values to NULL
    END AS cst_gndr,
    
    -- 4. String to Date Conversion: Convert string 'YYYY-MM-DD' to DATE
    STR_TO_DATE(NULLIF(TRIM(cst_create_date), ''), '%Y-%m-%d') AS cst_create_date
    
FROM
    dw_bronze.crm_cust_info
WHERE
    TRIM(cst_id) != ''
        AND NULLIF(TRIM(cst_id), '') IS NOT NULL;




