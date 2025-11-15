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
CREATE TABLE IF NOT EXISTS dw_bronze.crm_cust_info(
    cst_id INT,
    cst_key VARCHAR(50),
    cst_firstname VARCHAR(50),
    cst_lastname VARCHAR(50),
    cst_marital_status VARCHAR(50),
    cst_gndr VARCHAR(50),
    cst_create_date DATETIME
);

-- Create product info table
DROP TABLE IF EXISTS dw_bronze.crm_prd_info;
CREATE TABLE IF NOT EXISTS dw_bronze.crm_prd_info(
    prd_id INT,
    prd_key VARCHAR(50),
    prd_nm VARCHAR(50),
    prd_cost DECIMAL,
    prd_line VARCHAR(10),
    prd_start_dt DATETIME,
    prd_end_dt DATETIME
);

-- Creating sales details table
DROP TABLE IF EXISTS dw_bronze.crm_sales_details;
CREATE TABLE IF NOT EXISTS dw_bronze.crm_sales_details(
    sls_ord_num VARCHAR(50),
    sls_prd_key VARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);

-- Creating erp_cust_az12 table logic
DROP TABLE IF EXISTS dw_bronze.erp_cust_az12;
CREATE TABLE IF NOT EXISTS dw_bronze.erp_cust_az12(
    CID VARCHAR(50),
    BDATE DATETIME,
    GEN VARCHAR(50)
);

-- Creating erp_loc_a101 table logic
DROP TABLE IF EXISTS dw_bronze.erp_loc_a101;
CREATE TABLE IF NOT EXISTS dw_bronze.erp_loc_a101(
    CID VARCHAR(50),
    CNTRY VARCHAR(50)
);

-- Creating erp_px_cat_g1v2 table logic
DROP TABLE IF EXISTS dw_bronze.erp_px_cat_g1v2;
CREATE TABLE IF NOT EXISTS dw_bronze.erp_px_cat_g1v2(
    ID VARCHAR(50),
    CAT VARCHAR(50),
    SUBCAT VARCHAR(50),
    MAINTENANCE VARCHAR(10)
);

/*
	- Start loading the raw data into the different tables in the database in bulk loading mode
    - SQL Server has the bulk loading mode but mysql has a similar bulk loading mode we can use
    - It's called data loading infile
*/

-- Setting the local infile loading
SET GLOBAL local_infile = 1;

-- SHOW GLOBAL VARIABLES LIKE 'local_infile';
-- SHOW VARIABLES LIKE 'secure_file_priv';

-- Load the first raw data set into the first table
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

select * from dw_bronze.crm_cust_info;
select * from dw_bronze.crm_prd_info;
select * from dw_bronze.crm_sales_details;

TRUNCATE TABLE dw_bronze.erp_cust_az12;

LOAD DATA LOCAL INFILE '/Users/waleedmouhammed/Documents/GitHub/dw_dev/datasets/source_erp/cust_az12.csv'
INTO TABLE dw_bronze.erp_cust_az12
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
LINES TERMINATED BY '\r\n'
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

select * from dw_bronze.erp_cust_az12;
select * from dw_bronze.erp_loc_a101;
select * from dw_bronze.erp_px_cat_g1v2;
