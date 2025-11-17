/*
================================================================================
SCRIPT: 03_create_silver_tables.sql
PURPOSE: Defines and creates all table structures in the Silver layer.
STRATEGY: This script follows a 1-to-1 mapping from the Bronze layer.
          Its only purpose is to cleanse and cast the data to its
          proper, strict data types.
          - Bronze tables (all VARCHAR) are recreated with correct types.
          - Cryptic names are NOT renamed (this happens in Gold).
          - Tables are NOT yet conformed/joined (this happens in Gold).
          - Metadata columns are added for auditing.
================================================================================
*/

-- Switch to the Silver database
USE dw_silver;

/*
================================================================================
Section 1: Table - crm_cust_info
Purpose: Cleansed/Casted version of dw_bronze.crm_cust_info
================================================================================
*/
DROP TABLE IF EXISTS crm_cust_info;
CREATE TABLE crm_cust_info (
    -- Cleansed & Casted Columns from Bronze
    cst_id INT PRIMARY KEY COMMENT 'Cast from VARCHAR. Primary business key.',
    cst_key VARCHAR(20) COMMENT 'Cleansed cst_key',
    cst_firstname VARCHAR(100) COMMENT 'Cleansed cst_firstname',
    cst_lastname VARCHAR(100) COMMENT 'Cleansed cst_lastname',
    
    -- UPDATED: Increased length from VARCHAR(2) to VARCHAR(10)
    cst_marital_status VARCHAR(10) COMMENT 'Cleansed cst_marital_status (e.g., Single, Married)',
    
    -- UPDATED: Increased length from VARCHAR(2) to VARCHAR(10)
    cst_gndr VARCHAR(10) COMMENT 'Cleansed cst_gndr (e.g., Male, Female)',
    
    cst_create_date DATE COMMENT 'Cast from cst_create_date string',
    
    -- Metadata / Auditing Columns
    meta_created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp when the row was first created',
    meta_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp when the row was last updated',
    
    -- Indexes
    INDEX idx_cst_key (cst_key)
);

/*
================================================================================
Section 2: Table - crm_prd_info
Purpose: Cleansed/Casted version of dw_bronze.crm_prd_info
================================================================================
*/
DROP TABLE IF EXISTS crm_prd_info;
CREATE TABLE crm_prd_info (
    -- Cleansed & Casted Columns from Bronze
    prd_id INT PRIMARY KEY COMMENT 'Cast from VARCHAR. Primary business key.',
    prd_key VARCHAR(50) NOT NULL COMMENT 'Cleansed prd_key, natural key',
    prd_nm VARCHAR(255) COMMENT 'Cleansed prd_nm',
    prd_cost DECIMAL(19, 4) NULL COMMENT 'Cast from prd_cost, allows NULLs for blanks',
    prd_line VARCHAR(10) COMMENT 'Cleansed prd_line',
    prd_start_dt DATE COMMENT 'Cast from prd_start_dt string',
    prd_end_dt DATE NULL COMMENT 'Cast from prd_end_dt, allows NULLs for active products',
    
    -- Metadata / Auditing Columns
    meta_created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp when the row was first created',
    meta_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp when the row was last updated',
    
    -- Indexes
    UNIQUE KEY uq_prd_key (prd_key)
);

/*
================================================================================
Section 3: Table - crm_sales_details
Purpose: Cleansed/Casted version of dw_bronze.crm_sales_details
================================================================================
*/
DROP TABLE IF EXISTS crm_sales_details;
CREATE TABLE crm_sales_details (
    -- Cleansed & Casted Columns from Bronze
    sls_ord_num VARCHAR(20) NOT NULL COMMENT 'Cleansed sls_ord_num',
    sls_prd_key VARCHAR(50) NOT NULL COMMENT 'Cleansed sls_prd_key',
    sls_cust_id INT NOT NULL COMMENT 'Cast from sls_cust_id',
    sls_order_dt DATE COMMENT 'Cast from YYYYMMDD string',
    sls_ship_dt DATE COMMENT 'Cast from YYYYMMDD string',
    sls_due_dt DATE COMMENT 'Cast from YYYYMMDD string',
    sls_sales DECIMAL(19, 4) COMMENT 'Cast from sls_sales',
    sls_quantity INT COMMENT 'Cast from sls_quantity',
    sls_price DECIMAL(19, 4) COMMENT 'Cast from sls_price',
    
    -- Metadata / Auditing Columns
    meta_created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp when the row was first created',
    meta_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp when the row was last updated',
    
    -- Primary key for sales line item
    PRIMARY KEY (sls_ord_num, sls_prd_key),
    
    -- Indexes for future joins
    INDEX idx_cust_id (sls_cust_id),
    INDEX idx_prd_key (sls_prd_key),
    INDEX idx_order_dt (sls_order_dt)
);

/*
================================================================================
Section 4: Table - erp_cust_az12
Purpose: Cleansed/Casted version of dw_bronze.erp_cust_az12
================================================================================
*/
DROP TABLE IF EXISTS erp_cust_az12;
CREATE TABLE erp_cust_az12 (
    -- Cleansed & Casted Columns from Bronze
    CID VARCHAR(20) PRIMARY KEY COMMENT 'Cleansed business key (CID)',
    BDATE DATE COMMENT 'Cast from BDATE string',
    GEN VARCHAR(10) COMMENT 'Cleansed GEN (e.g., Male, Female)',
    
    -- Metadata / Auditing Columns
    meta_created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp when the row was first created',
    meta_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp when the row was last updated'
);

/*
================================================================================
Section 5: Table - erp_loc_a101
Purpose: Cleansed/Casted version of dw_bronze.erp_loc_a101
================================================================================
*/
DROP TABLE IF EXISTS erp_loc_a101;
CREATE TABLE erp_loc_a101 (
    -- Cleansed & Casted Columns from Bronze
    CID VARCHAR(20) PRIMARY KEY COMMENT 'Cleansed business key (CID)',
    CNTRY VARCHAR(50) COMMENT 'Cleansed CNTRY',
    
    -- Metadata / Auditing Columns
    meta_created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp when the row was first created',
    meta_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp when the row was last updated'
);

/*
================================================================================
Section 6: Table - erp_px_cat_g1v2
Purpose: Cleansed/Casted version of dw_bronze.erp_px_cat_g1v2
================================================================================
*/
DROP TABLE IF EXISTS erp_px_cat_g1v2;
CREATE TABLE erp_px_cat_g1v2 (
    -- Cleansed & Casted Columns from Bronze
    ID VARCHAR(20) PRIMARY KEY COMMENT 'Cleansed category ID',
    CAT VARCHAR(50) COMMENT 'Cleansed CAT',
    SUBCAT VARCHAR(50) COMMENT 'Cleansed SUBCAT',
    MAINTENANCE BOOLEAN COMMENT 'Cast from Yes/No string',
    
    -- Metadata / Auditing Columns
    meta_created_at DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT 'Timestamp when the row was first created',
    meta_updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT 'Timestamp when the row was last updated'
);