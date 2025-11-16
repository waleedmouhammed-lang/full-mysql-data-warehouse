/*
================================================================================
SCRIPT: 02_load_bronze_data.sql
PURPOSE: Performs a FULL REFRESH of the Bronze layer tables from source CSVs.
         This is the primary daily/scheduled ETL script.
RUN AS:  ETL User
RUN ON:  Daily or on a scheduled trigger.
================================================================================
*/

USE dw_bronze;

/*
================================================================================
Section 1: Data Ingestion (Atomic Load)
Purpose: Load raw data from CSV files into the Bronze tables.
Strategy: We use `TRUNCATE TABLE` instead of `DROP/CREATE`.
          - It is much faster as it doesn't drop/recreate the object.
          - It preserves table permissions (GRANTs) that `DROP` would destroy.
          - The entire 6-file load is wrapped in a transaction for atomicity.
================================================================================
*/

-- Begin the all-or-nothing transaction block
START TRANSACTION;

-- NOTE: The 'local_infile' setting must be enabled on both the MySQL
--       server (one-time DBA task) and in the client connection.

-- --- !! UPDATE FILE PATHS BELOW BEFORE RUNNING !! ---

-- 1. Truncate and Load CRM Customer Info
TRUNCATE TABLE crm_cust_info;
LOAD DATA LOCAL INFILE '/Users/waleedmouhammed/Documents/GitHub/dw_dev/datasets/source_crm/cust_info.csv'
INTO TABLE crm_cust_info
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- 2. Truncate and Load CRM Product Info
TRUNCATE TABLE crm_prd_info;
LOAD DATA LOCAL INFILE '/Users/waleedmouhammed/Documents/GitHub/dw_dev/datasets/source_crm/prd_info.csv'
INTO TABLE crm_prd_info
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- 3. Truncate and Load CRM Sales Details
TRUNCATE TABLE crm_sales_details;
LOAD DATA LOCAL INFILE '/Users/waleedmouhammed/Documents/GitHub/dw_dev/datasets/source_crm/sales_details.csv'
INTO TABLE crm_sales_details
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- 4. Truncate and Load ERP Customer Demographics
TRUNCATE TABLE erp_cust_az12;
LOAD DATA LOCAL INFILE '/Users/waleedmouhammed/Documents/GitHub/dw_dev/datasets/source_erp/cust_az12.csv'
INTO TABLE erp_cust_az12
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- 5. Truncate and Load ERP Customer Location
TRUNCATE TABLE erp_loc_a101;
LOAD DATA LOCAL INFILE '/Users/waleedmouhammed/Documents/GitHub/dw_dev/datasets/source_erp/loc_a101.csv'
INTO TABLE erp_loc_a101
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- 6. Truncate and Load ERP Product Category
TRUNCATE TABLE erp_px_cat_g1v2;
LOAD DATA LOCAL INFILE '/Users/waleedmouhammed/Documents/GitHub/dw_dev/datasets/source_erp/px_cat_g1v2.csv'
INTO TABLE erp_px_cat_g1v2
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- If all 6 TRUNCATE/LOAD commands succeed, finalize the transaction.
COMMIT;

/*
================================================================================
Section 2: Post-Load Validation
Purpose: Run a fast row-count check on all loaded tables.
================================================================================
*/

SELECT 'crm_cust_info' AS table_name, COUNT(*) AS loaded_row_count FROM crm_cust_info
UNION ALL
SELECT 'crm_prd_info' AS table_name, COUNT(*) AS loaded_row_count FROM crm_prd_info
UNION ALL
SELECT 'crm_sales_details' AS table_name, COUNT(*) AS loaded_row_count FROM crm_sales_details
UNION ALL
SELECT 'erp_cust_az12' AS table_name, COUNT(*) AS loaded_row_count FROM erp_cust_az12
UNION ALL
SELECT 'erp_loc_a101' AS table_name, COUNT(*) AS loaded_row_count FROM erp_loc_a101
UNION ALL
SELECT 'erp_px_cat_g1v2' AS table_name, COUNT(*) AS loaded_row_count FROM erp_px_cat_g1v2;

-- End of script --