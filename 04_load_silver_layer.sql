/*
================================================================================
SCRIPT: 04_load_silver_layer.sql
PURPOSE: Loads typed and cleansed SQL Server silver tables from bronze.
STRATEGY: Idempotent UPDATE then INSERT, except product history full refresh.
================================================================================
*/

USE CustomerSales;
GO

SET XACT_ABORT ON;
BEGIN TRANSACTION;

-- crm_cust_info
DROP TABLE IF EXISTS #src_crm_cust_info;

SELECT
    TRY_CONVERT(INT, NULLIF(TRIM(cst_id), '')) AS cst_id,
    CONVERT(VARCHAR(20), NULLIF(TRIM(cst_key), '')) AS cst_key,
    CONVERT(VARCHAR(100), NULLIF(TRIM(cst_firstname), '')) AS cst_firstname,
    CONVERT(VARCHAR(100), NULLIF(TRIM(cst_lastname), '')) AS cst_lastname,
    CONVERT(VARCHAR(10), CASE
        WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
        WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
        ELSE 'UnKnown'
    END) AS cst_marital_status,
    CONVERT(VARCHAR(10), CASE
        WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
        WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
        ELSE 'UnKnown'
    END) AS cst_gndr,
    TRY_CONVERT(DATE, NULLIF(TRIM(cst_create_date), ''), 23) AS cst_create_date
INTO #src_crm_cust_info
FROM bronze.crm_cust_info
WHERE TRY_CONVERT(INT, NULLIF(TRIM(cst_id), '')) IS NOT NULL
  AND NULLIF(TRIM(cst_key), '') IS NOT NULL;

UPDATE tgt
SET
    cst_key = src.cst_key,
    cst_firstname = src.cst_firstname,
    cst_lastname = src.cst_lastname,
    cst_marital_status = src.cst_marital_status,
    cst_gndr = src.cst_gndr,
    cst_create_date = src.cst_create_date,
    meta_updated_at = SYSUTCDATETIME()
FROM silver.crm_cust_info AS tgt
INNER JOIN #src_crm_cust_info AS src
    ON tgt.cst_id = src.cst_id;

INSERT INTO silver.crm_cust_info (
    cst_id, cst_key, cst_firstname, cst_lastname,
    cst_marital_status, cst_gndr, cst_create_date
)
SELECT
    src.cst_id, src.cst_key, src.cst_firstname, src.cst_lastname,
    src.cst_marital_status, src.cst_gndr, src.cst_create_date
FROM #src_crm_cust_info AS src
WHERE NOT EXISTS (
    SELECT 1 FROM silver.crm_cust_info AS tgt WHERE tgt.cst_id = src.cst_id
);

-- crm_prd_info: full refresh for product history window logic
DELETE FROM silver.crm_prd_info;

WITH product_source AS (
    SELECT
        TRY_CONVERT(INT, NULLIF(TRIM(prd_id), '')) AS prd_id,
        CONVERT(VARCHAR(50), REPLACE(SUBSTRING(TRIM(prd_key), 1, 5), '-', '_')) AS prd_category,
        CONVERT(VARCHAR(50), SUBSTRING(TRIM(prd_key), 7, 255)) AS prd_key,
        CONVERT(VARCHAR(255), NULLIF(TRIM(prd_nm), '')) AS prd_nm,
        TRY_CONVERT(DECIMAL(19, 4), COALESCE(NULLIF(TRIM(prd_cost), ''), '0')) AS prd_cost,
        CONVERT(VARCHAR(15), CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'R' THEN 'Road'
            WHEN 'T' THEN 'Touring'
            ELSE 'N/A'
        END) AS prd_line,
        TRY_CONVERT(DATE, NULLIF(TRIM(prd_start_dt), ''), 23) AS prd_start_dt,
        TRY_CONVERT(DATE, NULLIF(TRIM(prd_end_dt), ''), 23) AS prd_end_dt,
        TRIM(prd_key) AS raw_prd_key
    FROM bronze.crm_prd_info
    WHERE TRY_CONVERT(INT, NULLIF(TRIM(prd_id), '')) IS NOT NULL
      AND NULLIF(TRIM(prd_key), '') IS NOT NULL
),
product_history AS (
    SELECT
        *,
        LEAD(prd_start_dt) OVER (PARTITION BY raw_prd_key ORDER BY prd_start_dt) AS next_start_dt
    FROM product_source
)
INSERT INTO silver.crm_prd_info (
    prd_id, prd_category, prd_key, prd_nm, prd_cost,
    prd_line, prd_start_dt, prd_end_dt
)
SELECT
    prd_id,
    prd_category,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    CASE
        WHEN prd_end_dt < prd_start_dt THEN DATEADD(DAY, -1, next_start_dt)
        ELSE prd_end_dt
    END AS prd_end_dt
FROM product_history;

-- crm_sales_details
DROP TABLE IF EXISTS #src_crm_sales_details;

SELECT
    CONVERT(VARCHAR(20), NULLIF(TRIM(sls_ord_num), '')) AS sls_ord_num,
    CONVERT(VARCHAR(50), NULLIF(TRIM(sls_prd_key), '')) AS sls_prd_key,
    TRY_CONVERT(INT, NULLIF(TRIM(sls_cust_id), '')) AS sls_cust_id,
    TRY_CONVERT(DATE, NULLIF(TRIM(sls_order_dt), ''), 112) AS sls_order_dt,
    TRY_CONVERT(DATE, NULLIF(TRIM(sls_ship_dt), ''), 112) AS sls_ship_dt,
    TRY_CONVERT(DATE, NULLIF(TRIM(sls_due_dt), ''), 112) AS sls_due_dt,
    TRY_CONVERT(DECIMAL(19, 4), COALESCE(NULLIF(TRIM(sls_sales), ''), '0')) AS sls_sales,
    TRY_CONVERT(INT, COALESCE(NULLIF(TRIM(sls_quantity), ''), '0')) AS sls_quantity,
    TRY_CONVERT(DECIMAL(19, 4), COALESCE(NULLIF(TRIM(sls_price), ''), '0')) AS sls_price
INTO #src_crm_sales_details
FROM bronze.crm_sales_details
WHERE NULLIF(TRIM(sls_ord_num), '') IS NOT NULL
  AND NULLIF(TRIM(sls_prd_key), '') IS NOT NULL
  AND TRY_CONVERT(INT, NULLIF(TRIM(sls_cust_id), '')) IS NOT NULL;

UPDATE tgt
SET
    sls_cust_id = src.sls_cust_id,
    sls_order_dt = src.sls_order_dt,
    sls_ship_dt = src.sls_ship_dt,
    sls_due_dt = src.sls_due_dt,
    sls_sales = src.sls_sales,
    sls_quantity = src.sls_quantity,
    sls_price = src.sls_price,
    meta_updated_at = SYSUTCDATETIME()
FROM silver.crm_sales_details AS tgt
INNER JOIN #src_crm_sales_details AS src
    ON tgt.sls_ord_num = src.sls_ord_num
   AND tgt.sls_prd_key = src.sls_prd_key;

INSERT INTO silver.crm_sales_details (
    sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt,
    sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
)
SELECT
    src.sls_ord_num, src.sls_prd_key, src.sls_cust_id, src.sls_order_dt,
    src.sls_ship_dt, src.sls_due_dt, src.sls_sales, src.sls_quantity, src.sls_price
FROM #src_crm_sales_details AS src
WHERE NOT EXISTS (
    SELECT 1
    FROM silver.crm_sales_details AS tgt
    WHERE tgt.sls_ord_num = src.sls_ord_num
      AND tgt.sls_prd_key = src.sls_prd_key
);

-- erp_cust_az12
DROP TABLE IF EXISTS #src_erp_cust_az12;

SELECT
    CONVERT(VARCHAR(20), CASE
        WHEN LEFT(TRIM(CID), 3) = 'NAS' THEN SUBSTRING(TRIM(CID), 4, 255)
        ELSE TRIM(CID)
    END) AS CID,
    TRY_CONVERT(DATE, NULLIF(TRIM(BDATE), ''), 23) AS BDATE,
    CONVERT(VARCHAR(10), NULLIF(TRIM(GEN), '')) AS GEN
INTO #src_erp_cust_az12
FROM bronze.erp_cust_az12
WHERE NULLIF(TRIM(CID), '') IS NOT NULL;

UPDATE tgt
SET BDATE = src.BDATE, GEN = src.GEN, meta_updated_at = SYSUTCDATETIME()
FROM silver.erp_cust_az12 AS tgt
INNER JOIN #src_erp_cust_az12 AS src ON tgt.CID = src.CID;

INSERT INTO silver.erp_cust_az12 (CID, BDATE, GEN)
SELECT src.CID, src.BDATE, src.GEN
FROM #src_erp_cust_az12 AS src
WHERE NOT EXISTS (SELECT 1 FROM silver.erp_cust_az12 AS tgt WHERE tgt.CID = src.CID);

-- erp_loc_a101
DROP TABLE IF EXISTS #src_erp_loc_a101;

SELECT
    CONVERT(VARCHAR(20), REPLACE(NULLIF(TRIM(CID), ''), '-', '')) AS CID,
    CONVERT(VARCHAR(50), CASE
        WHEN TRIM(CNTRY) IN ('US', 'USA') THEN 'United States'
        WHEN TRIM(CNTRY) = 'DE' THEN 'Denmark'
        ELSE TRIM(CNTRY)
    END) AS CNTRY
INTO #src_erp_loc_a101
FROM bronze.erp_loc_a101
WHERE NULLIF(TRIM(CID), '') IS NOT NULL;

UPDATE tgt
SET CNTRY = src.CNTRY, meta_updated_at = SYSUTCDATETIME()
FROM silver.erp_loc_a101 AS tgt
INNER JOIN #src_erp_loc_a101 AS src ON tgt.CID = src.CID;

INSERT INTO silver.erp_loc_a101 (CID, CNTRY)
SELECT src.CID, src.CNTRY
FROM #src_erp_loc_a101 AS src
WHERE NOT EXISTS (SELECT 1 FROM silver.erp_loc_a101 AS tgt WHERE tgt.CID = src.CID);

-- erp_px_cat_g1v2
DROP TABLE IF EXISTS #src_erp_px_cat_g1v2;

SELECT
    CONVERT(VARCHAR(20), NULLIF(TRIM(ID), '')) AS ID,
    CONVERT(VARCHAR(50), NULLIF(TRIM(CAT), '')) AS CAT,
    CONVERT(VARCHAR(50), NULLIF(TRIM(SUBCAT), '')) AS SUBCAT,
    CONVERT(BIT, CASE
        WHEN TRIM(MAINTENANCE) = 'Yes' THEN 1
        WHEN TRIM(MAINTENANCE) = 'No' THEN 0
        ELSE NULL
    END) AS MAINTENANCE
INTO #src_erp_px_cat_g1v2
FROM bronze.erp_px_cat_g1v2
WHERE NULLIF(TRIM(ID), '') IS NOT NULL;

UPDATE tgt
SET
    CAT = src.CAT,
    SUBCAT = src.SUBCAT,
    MAINTENANCE = src.MAINTENANCE,
    meta_updated_at = SYSUTCDATETIME()
FROM silver.erp_px_cat_g1v2 AS tgt
INNER JOIN #src_erp_px_cat_g1v2 AS src ON tgt.ID = src.ID;

INSERT INTO silver.erp_px_cat_g1v2 (ID, CAT, SUBCAT, MAINTENANCE)
SELECT src.ID, src.CAT, src.SUBCAT, src.MAINTENANCE
FROM #src_erp_px_cat_g1v2 AS src
WHERE NOT EXISTS (SELECT 1 FROM silver.erp_px_cat_g1v2 AS tgt WHERE tgt.ID = src.ID);

COMMIT TRANSACTION;
GO
