/*
================================================================================
Script: dw_creation_script.sql
Purpose: Initialize the medallion architecture data warehouse by creating three 
         database layers:
         - Bronze Layer: Raw data ingestion and storage
         - Silver Layer: Cleaned and validated data
         - Gold Layer: Business-ready aggregated data
         
Author: Waleed Mouhammed
Created: November 15, 2025
================================================================================
*/

-- Creating the bronze layer database
DROP DATABASE IF EXISTS dw_bronze;
CREATE DATABASE IF NOT EXISTS dw_bronze; 

-- Creating the silver layer database
DROP DATABASE IF EXISTS dw_silver;
CREATE DATABASE IF NOT EXISTS dw_silver;

-- Creating the gold layer database
DROP DATABASE IF EXISTS dw_gold;
CREATE DATABASE IF NOT EXISTS dw_gold;

-- Building the logic of dropping the table if exists before being created once again
-- is the standard behavior. It differs from sql dbms to another, for example in
-- SQL Server you need to build T-SQL logic to check the existence of the table first.
-- If it does exist, then drop it, otherwise create it directly.
DROP TABLE IF EXISTS dw_bronze.crm_cust_info;
-- Creating tables schema using VARCHAR(255) ensure proper raw data loading without any data loss
CREATE TABLE IF NOT EXISTS dw_bronze.crm_cust_info(
    cst_id VARCHAR(255),
    cst_key VARCHAR(255),
    cst_firstname VARCHAR(255),
    cst_lastname VARCHAR(255),
    cst_marital_status VARCHAR(255),
    cst_gndr VARCHAR(255),
    cst_create_date VARCHAR(255)
);

-- Create product info table
DROP TABLE IF EXISTS dw_bronze.crm_prd_info;
CREATE TABLE IF NOT EXISTS dw_bronze.crm_prd_info(
    prd_id VARCHAR(255),
    prd_key VARCHAR(255),
    prd_nm VARCHAR(255),
    prd_cost VARCHAR(255),
    prd_line VARCHAR(255),
    prd_start_dt VARCHAR(255),
    prd_end_dt VARCHAR(255)
);

-- Creating sales details table
DROP TABLE IF EXISTS dw_bronze.crm_sales_details;
CREATE TABLE IF NOT EXISTS dw_bronze.crm_sales_details(
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

-- Creating erp_cust_az12 table logic
DROP TABLE IF EXISTS dw_bronze.erp_cust_az12;
CREATE TABLE IF NOT EXISTS dw_bronze.erp_cust_az12(
    CID VARCHAR(255),
    BDATE VARCHAR(255),
    GEN VARCHAR(255)
);

-- Creating erp_loc_a101 table logic
DROP TABLE IF EXISTS dw_bronze.erp_loc_a101;
CREATE TABLE IF NOT EXISTS dw_bronze.erp_loc_a101(
    CID VARCHAR(255),
    CNTRY VARCHAR(255)
);

-- Creating erp_px_cat_g1v2 table logic
DROP TABLE IF EXISTS dw_bronze.erp_px_cat_g1v2;
CREATE TABLE IF NOT EXISTS dw_bronze.erp_px_cat_g1v2(
    ID VARCHAR(255),
    CAT VARCHAR(255),
    SUBCAT VARCHAR(255),
    MAINTENANCE VARCHAR(255)
);

/*
	- Start loading the raw data into the different tables in the database in bulk loading mode
    - SQL Server has the bulk loading mode but mysql has a similar bulk loading mode we can use
    - It's called data loading infile
*/

-- Setting the local infile loading to enable local file loading into the database
SET GLOBAL local_infile = 1;

-- Show the variable status to ensure that the local file loading is enabled
-- SHOW GLOBAL VARIABLES LIKE 'local_infile';

-- Identifying the secured place to store the csv files in to ease the loading process
-- SHOW VARIABLES LIKE 'secure_file_priv';

-- Starting the raw data loading process
TRUNCATE TABLE dw_bronze.crm_cust_info;

LOAD DATA LOCAL INFILE '/Users/waleedmouhammed/Documents/GitHub/dw_dev/datasets/source_crm/cust_info.csv'
INTO TABLE dw_bronze.crm_cust_info
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'  -- <--- The correct backslashes
IGNORE 1 LINES;

-- Load the second raw data set into the second table
TRUNCATE TABLE dw_bronze.crm_prd_info;

LOAD DATA LOCAL INFILE '/Users/waleedmouhammed/Documents/GitHub/dw_dev/datasets/source_crm/prd_info.csv'
INTO TABLE dw_bronze.crm_prd_info
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- Load the third raw data set into the third table
TRUNCATE TABLE dw_bronze.crm_sales_details;

LOAD DATA LOCAL INFILE '/Users/waleedmouhammed/Documents/GitHub/dw_dev/datasets/source_crm/sales_details.csv'
INTO TABLE dw_bronze.crm_sales_details
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- Truncating the table to ensure full refresh load
TRUNCATE TABLE dw_bronze.erp_cust_az12;

-- Using the LOCAL keyword to load data from local file system
LOAD DATA LOCAL INFILE '/Users/waleedmouhammed/Documents/GitHub/dw_dev/datasets/source_erp/cust_az12.csv'
-- Using the schema name as a prefix to ensure proper script execution
INTO TABLE dw_bronze.erp_cust_az12
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
-- Adding \r\n for Windows line endings
LINES TERMINATED BY '\r\n'
-- Ignoring one line as we have column headers in the first line
IGNORE 1 LINES;

TRUNCATE TABLE dw_bronze.erp_loc_a101;

LOAD DATA LOCAL INFILE '/Users/waleedmouhammed/Documents/GitHub/dw_dev/datasets/source_erp/loc_a101.csv'
INTO TABLE dw_bronze.erp_loc_a101
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

TRUNCATE TABLE dw_bronze.erp_px_cat_g1v2;

LOAD DATA LOCAL INFILE '/Users/waleedmouhammed/Documents/GitHub/dw_dev/datasets/source_erp/px_cat_g1v2.csv'
INTO TABLE dw_bronze.erp_px_cat_g1v2
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
IGNORE 1 LINES;

-- Validating the full data loading into the bronze layer - from quantity perspective!
select COUNT(*) from dw_bronze.crm_cust_info;
select COUNT(*) from dw_bronze.crm_prd_info;
select COUNT(*) from dw_bronze.crm_sales_details;
select COUNT(*) from dw_bronze.erp_cust_az12;
select COUNT(*) from dw_bronze.erp_loc_a101;
select COUNT(*) from dw_bronze.erp_px_cat_g1v2;
