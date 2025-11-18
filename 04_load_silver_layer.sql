/*
================================================================================
SCRIPT: 04_load_silver_layer.sql
PURPOSE: Cleanses and transforms data from Bronze CRM tables
         (crm_cust_info, crm_prd_info) and inserts them into their
         corresponding Silver tables.
STRATEGY: Full Refresh (TRUNCATE and INSERT).
         This makes the job idempotent (safe to re-run).
================================================================================
*/

-- Step 1: Switch to the Silver database
USE dw_silver;

-- ========================================================================
--                      LOAD: crm_cust_info
-- ========================================================================

-- Step 2: Truncate the target Silver table for idempotency
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
    -- 1. Data Type Casting: Convert VARCHAR to SIGNED (INT).
    CAST(NULLIF(TRIM(cst_id), '') AS SIGNED) AS cst_id,
    
    -- 2. Handle Empty Strings & Whitespace, then CAST to VARCHAR(20)
    CAST(NULLIF(TRIM(cst_key), '') AS CHAR(20)) AS cst_key,
    
    -- 3. Handle Empty Strings & Whitespace, then CAST to VARCHAR(100)
    CAST(NULLIF(TRIM(cst_firstname), '') AS CHAR(100)) AS cst_firstname,
    
    -- 4. Handle Empty Strings & Whitespace, then CAST to VARCHAR(100)
    CAST(NULLIF(TRIM(cst_lastname), '') AS CHAR(100)) AS cst_lastname,

    -- 5. Abbreviation Transformation, then CAST to VARCHAR(10)
    CAST(CASE UPPER(TRIM(cst_marital_status))
			WHEN 'S' THEN 'Single'
			WHEN 'M' THEN 'Married'
			ELSE 'UnKnown' -- Default for blanks, NULLs, or other values.
		END AS CHAR(10)) AS cst_marital_status,

    -- 6. Abbreviation Transformation, then CAST to VARCHAR(10)
    CAST(CASE UPPER(TRIM(cst_gndr))
			WHEN 'M' THEN 'Male'
			WHEN 'F' THEN 'Female'
			ELSE 'UnKnown' -- Default for blanks, NULLs, or other values.
		END AS CHAR(10)) AS cst_gndr,
    
    -- 7. String to Date Conversion: Safely convert string 'YYYY-MM-DD'
    -- to DATE. NULLIF() prevents STR_TO_DATE from failing on empty strings.
    STR_TO_DATE(NULLIF(TRIM(cst_create_date), ''), '%Y-%m-%d') AS cst_create_date
    
FROM
    dw_bronze.crm_cust_info
WHERE
    -- 8. Data Integrity Filter: Ensure primary business keys
    -- (cst_id, cst_key) are not NULL or empty.
    NULLIF(TRIM(cst_id), '') IS NOT NULL 
    AND NULLIF(TRIM(cst_key), '') IS NOT NULL;

-- ========================================================================
--                      LOAD: crm_prd_info
-- ========================================================================

-- Step 4: Truncate the target Silver table for idempotency
TRUNCATE TABLE dw_silver.crm_prd_info;

-- Step 5: Insert cleansed data from Bronze into Silver
INSERT INTO dw_silver.crm_prd_info (
    prd_id,
    prd_category,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
    -- Metadata columns (meta_created_at, meta_updated_at)
    -- will be auto-populated by their DEFAULT definitions.
)
SELECT
    -- 1. Data Type Casting: Convert VARCHAR to SIGNED (INT).
    CAST(NULLIF(TRIM(prd_id), '') AS SIGNED) AS prd_id,

    -- 2. Business Key Transformation (prd_category):
    -- Extract first 5 chars and replace separators for consistency.
    CAST(REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS CHAR(50)) AS prd_category,

    -- 3. Business Key Transformation (prd_key):
    -- Extract the key starting from the 7th character.
    CAST(SUBSTRING(prd_key, 7) AS CHAR(50)) AS prd_key,

    -- 4. Cleansing: Trim whitespace from product name.
    CAST(TRIM(prd_nm) AS CHAR(255)) AS prd_nm,

    -- 5. String to Numeric: Safely convert cost to DECIMAL.
    -- NULLIF handles empty strings, COALESCE sets NULLs to 0.
    CAST(COALESCE(NULLIF(TRIM(prd_cost), ''), 0) AS DECIMAL(19, 4)) AS prd_cost,

    -- 6. Abbreviation Transformation: Convert product line codes
    -- to readable names.
    CAST(CASE UPPER(TRIM(prd_line))
        WHEN 'M' THEN 'Mountain'
        WHEN 'S' THEN 'Other Sales'
        WHEN 'R' THEN 'Road'
        WHEN 'T' THEN 'Touring'
        ELSE 'N/A'
    END AS CHAR(15)) AS prd_line,

    -- 7. String to Date: Safely convert start date, handling empty strings.
    CAST(prd_start_dt AS DATE) AS prd_start_dt,

    -- 8. Advanced Date Logic (SCD Type 2):
    -- Corrects invalid end dates (where end < start).
    -- It replaces the invalid date with the next row's start date - 1 day.
    CAST(CASE
        -- Check if the original end date is EARLIER than the start date
        WHEN prd_end_dt < prd_start_dt THEN
            -- If yes, replace it with the next start date - 1 day
            DATE_SUB(
                LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt),
                INTERVAL 1 DAY
            )
        ELSE
            -- If end date is valid, keep the original end date
            prd_end_dt
    END AS DATE) AS prd_end_dt

FROM dw_bronze.crm_prd_info
WHERE
    -- 9. Data Integrity Filter: Ensure primary business keys
    -- (prd_id, prd_key) are not NULL or empty.
    NULLIF(TRIM(prd_id), '') IS NOT NULL
    AND NULLIF(TRIM(prd_key), '') IS NOT NULL;