/*
================================================================================
SCRIPT: 04_load_silver_crm_cust_info.sql
PURPOSE: SQL Server single-table silver load for crm_cust_info.
NOTE: The full silver load is in 04_load_silver_layer.sql.
================================================================================
*/

USE DataWarehouse;
GO

SET XACT_ABORT ON;
BEGIN TRANSACTION;

DELETE FROM silver.crm_cust_info;

INSERT INTO silver.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
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
FROM bronze.crm_cust_info
WHERE TRY_CONVERT(INT, NULLIF(TRIM(cst_id), '')) IS NOT NULL
  AND NULLIF(TRIM(cst_key), '') IS NOT NULL;

COMMIT TRANSACTION;
GO
