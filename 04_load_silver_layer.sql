/*
================================================================================
SCRIPT: 04_load_silver_layer.sql
PURPOSE: Cleanses, transforms, and loads data from the Bronze layer into the 
         Silver layer for all CRM and ERP tables.
STRATEGY: Full Refresh (TRUNCATE and INSERT). 
         This ensures idempotency (safe to re-run).
TABLES LOADED:
    1. crm_cust_info
    2. crm_prd_info
    3. crm_sales_details
    4. erp_cust_az12
    5. erp_loc_a101
    6. erp_px_cat_g1v2
================================================================================
*/

-- Step 1: Switch to the Silver database
USE dw_silver;

-- ========================================================================
--                      1. LOAD: crm_cust_info
-- ========================================================================
TRUNCATE TABLE dw_silver.crm_cust_info;

INSERT INTO dw_silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT
    -- 1. Data Type Casting: Convert VARCHAR to SIGNED (INT).
    CAST(NULLIF(TRIM(cst_id), '') AS SIGNED) AS cst_id,
    
    -- 2. Handle Empty Strings & Whitespace, then CAST to CHAR(20)
    CAST(NULLIF(TRIM(cst_key), '') AS CHAR(20)) AS cst_key,
    
    -- 3. Handle Empty Strings & Whitespace, then CAST to CHAR(100)
    CAST(NULLIF(TRIM(cst_firstname), '') AS CHAR(100)) AS cst_firstname,
    
    -- 4. Handle Empty Strings & Whitespace, then CAST to CHAR(100)
    CAST(NULLIF(TRIM(cst_lastname), '') AS CHAR(100)) AS cst_lastname,

    -- 5. Abbreviation Transformation, then CAST to CHAR(10)
    CAST(CASE 
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'UnKnown' 
    END AS CHAR(10)) AS cst_marital_status,

    -- 6. Abbreviation Transformation, then CAST to CHAR(10)
    CAST(CASE 
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        ELSE 'UnKnown' 
    END AS CHAR(10)) AS cst_gndr,
    
    -- 7. String to Date Conversion: Safely convert string 'YYYY-MM-DD' to DATE.
    STR_TO_DATE(NULLIF(TRIM(cst_create_date), ''), '%Y-%m-%d') AS cst_create_date
FROM dw_bronze.crm_cust_info
WHERE NULLIF(TRIM(cst_id), '') IS NOT NULL 
  AND NULLIF(TRIM(cst_key), '') IS NOT NULL;

-- ========================================================================
--                      2. LOAD: crm_prd_info
-- ========================================================================
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
    
    
-- ========================================================================
--                      3. LOAD: crm_sales_details
-- ========================================================================
TRUNCATE TABLE dw_silver.crm_sales_details;

INSERT INTO dw_silver.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT
    -- 1. Cleanse and cast Primary Key
    CAST(NULLIF(TRIM(sls_ord_num), '') AS CHAR(20)) AS sls_ord_num,
    
    -- 2. Cleanse and cast Primary Key
    CAST(NULLIF(TRIM(sls_prd_key), '') AS CHAR(50)) AS sls_prd_key,

    -- 3. Cleanse and cast Foreign Key
    CAST(NULLIF(TRIM(sls_cust_id), '') AS SIGNED) AS sls_cust_id,

    -- 4. String to Date Conversion (YYYYMMDD format): Check length exactly 8.
    CASE 
        WHEN CHAR_LENGTH(TRIM(sls_order_dt)) = 8 THEN STR_TO_DATE(TRIM(sls_order_dt), '%Y%m%d') 
        ELSE NULL 
    END AS sls_order_dt,

    CASE 
        WHEN CHAR_LENGTH(TRIM(sls_ship_dt)) = 8 THEN STR_TO_DATE(TRIM(sls_ship_dt), '%Y%m%d') 
        ELSE NULL 
    END AS sls_ship_dt,

    CASE 
        WHEN CHAR_LENGTH(TRIM(sls_due_dt)) = 8 THEN STR_TO_DATE(TRIM(sls_due_dt), '%Y%m%d') 
        ELSE NULL 
    END AS sls_due_dt,

    -- 5. String to Numeric: Handle nulls/empty strings, default to 0.
    CAST(COALESCE(NULLIF(TRIM(sls_sales), ''), 0) AS DECIMAL(19, 4)) AS sls_sales,
    CAST(COALESCE(NULLIF(TRIM(sls_quantity), ''), 0) AS SIGNED) AS sls_quantity,
    CAST(COALESCE(NULLIF(TRIM(sls_price), ''), 0) AS DECIMAL(19, 4)) AS sls_price
FROM dw_bronze.crm_sales_details
WHERE NULLIF(TRIM(sls_ord_num), '') IS NOT NULL
  AND NULLIF(TRIM(sls_prd_key), '') IS NOT NULL
  AND NULLIF(TRIM(sls_cust_id), '') IS NOT NULL;


-- ========================================================================
--                      4. LOAD: erp_cust_az12
-- ========================================================================
TRUNCATE TABLE dw_silver.erp_cust_az12;

INSERT INTO dw_silver.erp_cust_az12 (CID, BDATE, GEN)
SELECT
    -- 1. CID Transformation: Remove "NAS" prefix if present.
    CAST(CASE 
        WHEN LEFT(TRIM(CID), 3) = 'NAS' THEN SUBSTRING(TRIM(CID), 4)
        ELSE TRIM(CID)
    END AS CHAR(20)) AS CID,

    -- 2. Date Conversion (Format: YYYY-MM-DD): Check length exactly 10.
    CASE 
        WHEN CHAR_LENGTH(TRIM(BDATE)) = 10 THEN STR_TO_DATE(TRIM(BDATE), '%Y-%m-%d') 
        ELSE NULL 
    END AS BDATE,

    -- 3. Gender Transformation: Trim and cast.
    CAST(NULLIF(TRIM(GEN), '') AS CHAR(10)) AS GEN
FROM dw_bronze.erp_cust_az12
WHERE NULLIF(TRIM(CID), '') IS NOT NULL;


-- ========================================================================
--                      5. LOAD: erp_loc_a101
-- ========================================================================
TRUNCATE TABLE dw_silver.erp_loc_a101;

INSERT INTO dw_silver.erp_loc_a101 (CID, CNTRY)
SELECT
    -- 1. CID Transformation: Remove dashes ('-').
    CAST(REPLACE(NULLIF(TRIM(CID), ''), '-', '') AS CHAR(20)) AS CID,

    -- 2. Country Standardization: Normalize 'US'/'USA' to 'United States'.
    CAST(CASE 
        WHEN TRIM(CNTRY) = 'US' THEN 'United States'
        WHEN TRIM(CNTRY) = 'USA' THEN 'United States'
        ELSE TRIM(CNTRY)
    END AS CHAR(50)) AS CNTRY
FROM dw_bronze.erp_loc_a101
WHERE NULLIF(TRIM(CID), '') IS NOT NULL;


-- ========================================================================
--                      6. LOAD: erp_px_cat_g1v2
-- ========================================================================
TRUNCATE TABLE dw_silver.erp_px_cat_g1v2;

INSERT INTO dw_silver.erp_px_cat_g1v2 (ID, CAT, SUBCAT, MAINTENANCE)
SELECT
    -- 1. Cleanse and cast ID.
    CAST(NULLIF(TRIM(ID), '') AS CHAR(20)) AS ID,

    -- 2. Cleanse and cast Category.
    CAST(NULLIF(TRIM(CAT), '') AS CHAR(50)) AS CAT,

    -- 3. Cleanse and cast Sub-Category.
    CAST(NULLIF(TRIM(SUBCAT), '') AS CHAR(50)) AS SUBCAT,

    -- 4. Maintenance Transformation: Convert 'Yes'/'No' to Boolean (1/0).
    CASE 
        WHEN TRIM(MAINTENANCE) = 'Yes' THEN 1
        WHEN TRIM(MAINTENANCE) = 'No' THEN 0
        ELSE NULL 
    END AS MAINTENANCE
FROM dw_bronze.erp_px_cat_g1v2
WHERE NULLIF(TRIM(ID), '') IS NOT NULL;