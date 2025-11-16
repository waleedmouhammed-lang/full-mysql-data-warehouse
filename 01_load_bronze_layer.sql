/*
================================================================================
SCRIPT: 01_load_bronze_layer.sql
PURPOSE: Performs a FULL REFRESH of the Bronze layer tables from source CSVs.
         This script is designed to be run on a daily or scheduled basis.
RUN AS:  Should be run by a dedicated, low-privilege ETL user (e.g., 'etl_user').
================================================================================
*/

-- Use the 'dw_bronze' database context.
-- This is a critical safety measure. If this command fails (e.g., DB
-- doesn't exist), the script will halt, preventing tables from being
-- dropped or created in an incorrect database (like 'mysql' or 'test').
USE dw_bronze;

/*
================================================================================
Section 1: Bronze Table Schema Definition (Full Refresh)
Purpose: Re-create the Bronze tables from scratch.
Strategy: The `DROP TABLE IF EXISTS` and `CREATE TABLE IF NOT EXISTS` pattern
          is intentional. It ensures we are always loading into a clean,
          empty table that matches the latest schema definition.
          
          All columns are `VARCHAR(255)` to ensure a resilient load.
          This prevents data type mismatch errors (e.g., `''` in a `DECIMAL`
          column) from failing the ingestion. All raw data is captured as
          text and will be cleaned/typed in the Silver layer.
================================================================================
*/

-- Table: CRM Customer Info
DROP TABLE IF EXISTS crm_cust_info;
CREATE TABLE IF NOT EXISTS crm_cust_info(
    cst_id VARCHAR(255),
    cst_key VARCHAR(255),
    cst_firstname VARCHAR(255),
    cst_lastname VARCHAR(255),
    cst_marital_status VARCHAR(255),
    cst_gndr VARCHAR(255),
    cst_create_date VARCHAR(255)
);

-- Table: CRM Product Info
DROP TABLE IF EXISTS crm_prd_info;
CREATE TABLE IF NOT EXISTS crm_prd_info(
    prd_id VARCHAR(255),
    prd_key VARCHAR(255),
    prd_nm VARCHAR(255),
    prd_cost VARCHAR(255),
    prd_line VARCHAR(255),
    prd_start_dt VARCHAR(255),
    prd_end_dt VARCHAR(255)
);

-- Table: CRM Sales Details
DROP TABLE IF EXISTS crm_sales_details;
CREATE TABLE IF NOT EXISTS crm_sales_details(
    sls_ord_num VARCHAR(255),
    sls_prd_key VARCHAR(255),
    sls_cust_id VARCHAR(255),
    sls_order_dt VARCHAR(255),
    sls_ship_dt VARCHAR(255),
    sls_due_dt VARCHAR(255),
    sls_sales VARCHAR(255),
    sls_quantity VARCHAR(255),
    sls_price VARCHAR(255)
);

-- Table: ERP Customer Demographics
DROP TABLE IF EXISTS erp_cust_az12;
CREATE TABLE IF NOT EXISTS erp_cust_az12(
    CID VARCHAR(255),
    BDATE VARCHAR(255),
    GEN VARCHAR(255)
);

-- Table: ERP Customer Location
DROP TABLE IF EXISTS erp_loc_a101;
CREATE TABLE IF NOT EXISTS erp_loc_a101(
    CID VARCHAR(255),
    CNTRY VARCHAR(255)
);

-- Table: ERP Product Category
DROP TABLE IF EXISTS erp_px_cat_g1v2;
CREATE TABLE IF NOT EXISTS erp_px_cat_g1v2(
    ID VARCHAR(255),
    CAT VARCHAR(255),
    SUBCAT VARCHAR(255),
    MAINTENANCE VARCHAR(255)
);

/*
================================================================================
Section 2: Raw Data Ingestion (Atomic Load)
Purpose: Load raw data from CSV files into the newly created Bronze tables.
Method:  Using `LOAD DATA LOCAL INFILE` for the fastest possible ingestion.
Atomicity: The entire 6-file load is wrapped in a single transaction.
           If any file fails to load (e.g., file not found, permissions
           error), the entire operation will be rolled back. This
           prevents a partially-loaded, inconsistent state.
================================================================================
*/

-- Begin the all-or-nothing transaction block
START TRANSACTION;

-- NOTE: The 'local_infile' setting must be enabled on both the MySQL
--       server (one-time DBA task) and in the client connection.

-- --- !! UPDATE FILE PATHS BELOW BEFORE RUNNING !! ---

-- Load CRM Customer Info
LOAD DATA LOCAL INFILE '/Users/waleedmouhammed/Documents/GitHub/dw_dev/datasets/source_crm/cust_info.csv'
INTO TABLE crm_cust_info
FIELDS TERMINATED BY ','           -- Specifies the column delimiter
OPTIONALLY ENCLOSED BY '"'         -- Handles values that are quoted
LINES TERMINATED BY '\r\n'         -- Specifies Windows-style line endings
IGNORE 1 LINES;                    -- Skips the header row in the CSV

-- Load CRM Product Info
LOAD DATA LOCAL INFILE '/Users/waleedmouhammed/Documents/GitHub/dw_dev/datasets/source_crm/prd_info.csv'
INTO TABLE crm_prd_info
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- Load CRM Sales Details
LOAD DATA LOCAL INFILE '/Users/waleedmouhammed/Documents/GitHub/dw_dev/datasets/source_crm/sales_details.csv'
INTO TABLE crm_sales_details
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- Load ERP Customer Demographics
LOAD DATA LOCAL INFILE '/Users/waleedmouhammed/Documents/GitHub/dw_dev/datasets/source_erp/cust_az12.csv'
INTO TABLE erp_cust_az12
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- Load ERP Customer Location
LOAD DATA LOCAL INFILE '/Users/waleedmouhammed/Documents/GitHub/dw_dev/datasets/source_erp/loc_a101.csv'
INTO TABLE erp_loc_a101
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- Load ERP Product Category
LOAD DATA LOCAL INFILE '/Users/waleedmouhammed/Documents/GitHub/dw_dev/datasets/source_erp/px_cat_g1v2.csv'
INTO TABLE erp_px_cat_g1v2
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- If all 6 LOAD DATA commands succeed without error, finalize the transaction
-- and make the data permanent.
COMMIT;

/*
================================================================================
Section 3: Post-Load Validation
Purpose: Run a fast row-count check on all loaded tables. This provides
         a simple, consolidated report to confirm the load was successful.
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