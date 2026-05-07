/*
================================================================================
SCRIPT: 03_create_silver_tables.sql
PURPOSE: Creates typed SQL Server silver tables.
================================================================================
*/

USE CustomerSales;
GO

DROP TABLE IF EXISTS silver.crm_sales_details;
DROP TABLE IF EXISTS silver.crm_prd_info;
DROP TABLE IF EXISTS silver.crm_cust_info;
DROP TABLE IF EXISTS silver.erp_cust_az12;
DROP TABLE IF EXISTS silver.erp_loc_a101;
DROP TABLE IF EXISTS silver.erp_px_cat_g1v2;
GO

CREATE TABLE silver.crm_cust_info (
    cst_id INT NOT NULL CONSTRAINT pk_silver_crm_cust_info PRIMARY KEY,
    cst_key VARCHAR(20) NULL,
    cst_firstname VARCHAR(100) NULL,
    cst_lastname VARCHAR(100) NULL,
    cst_marital_status VARCHAR(10) NULL,
    cst_gndr VARCHAR(10) NULL,
    cst_create_date DATE NULL,
    meta_created_at DATETIME2(6) NOT NULL CONSTRAINT df_silver_crm_cust_info_created_at DEFAULT SYSUTCDATETIME(),
    meta_updated_at DATETIME2(6) NOT NULL CONSTRAINT df_silver_crm_cust_info_updated_at DEFAULT SYSUTCDATETIME()
);

CREATE INDEX ix_silver_crm_cust_info_cst_key ON silver.crm_cust_info(cst_key);
GO

CREATE TABLE silver.crm_prd_info (
    prd_id INT NOT NULL CONSTRAINT pk_silver_crm_prd_info PRIMARY KEY,
    prd_category VARCHAR(50) NOT NULL,
    prd_key VARCHAR(50) NOT NULL,
    prd_nm VARCHAR(255) NULL,
    prd_cost DECIMAL(19, 4) NULL,
    prd_line VARCHAR(15) NULL,
    prd_start_dt DATE NULL,
    prd_end_dt DATE NULL,
    meta_created_at DATETIME2(6) NOT NULL CONSTRAINT df_silver_crm_prd_info_created_at DEFAULT SYSUTCDATETIME(),
    meta_updated_at DATETIME2(6) NOT NULL CONSTRAINT df_silver_crm_prd_info_updated_at DEFAULT SYSUTCDATETIME()
);

CREATE INDEX ix_silver_crm_prd_info_prd_key ON silver.crm_prd_info(prd_key);
GO

CREATE TABLE silver.crm_sales_details (
    sls_ord_num VARCHAR(20) NOT NULL,
    sls_prd_key VARCHAR(50) NOT NULL,
    sls_cust_id INT NOT NULL,
    sls_order_dt DATE NULL,
    sls_ship_dt DATE NULL,
    sls_due_dt DATE NULL,
    sls_sales DECIMAL(19, 4) NULL,
    sls_quantity INT NULL,
    sls_price DECIMAL(19, 4) NULL,
    meta_created_at DATETIME2(6) NOT NULL CONSTRAINT df_silver_crm_sales_details_created_at DEFAULT SYSUTCDATETIME(),
    meta_updated_at DATETIME2(6) NOT NULL CONSTRAINT df_silver_crm_sales_details_updated_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT pk_silver_crm_sales_details PRIMARY KEY (sls_ord_num, sls_prd_key)
);

CREATE INDEX ix_silver_crm_sales_details_cust_id ON silver.crm_sales_details(sls_cust_id);
CREATE INDEX ix_silver_crm_sales_details_prd_key ON silver.crm_sales_details(sls_prd_key);
CREATE INDEX ix_silver_crm_sales_details_order_dt ON silver.crm_sales_details(sls_order_dt);
GO

CREATE TABLE silver.erp_cust_az12 (
    CID VARCHAR(20) NOT NULL CONSTRAINT pk_silver_erp_cust_az12 PRIMARY KEY,
    BDATE DATE NULL,
    GEN VARCHAR(10) NULL,
    meta_created_at DATETIME2(6) NOT NULL CONSTRAINT df_silver_erp_cust_az12_created_at DEFAULT SYSUTCDATETIME(),
    meta_updated_at DATETIME2(6) NOT NULL CONSTRAINT df_silver_erp_cust_az12_updated_at DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE silver.erp_loc_a101 (
    CID VARCHAR(20) NOT NULL CONSTRAINT pk_silver_erp_loc_a101 PRIMARY KEY,
    CNTRY VARCHAR(50) NULL,
    meta_created_at DATETIME2(6) NOT NULL CONSTRAINT df_silver_erp_loc_a101_created_at DEFAULT SYSUTCDATETIME(),
    meta_updated_at DATETIME2(6) NOT NULL CONSTRAINT df_silver_erp_loc_a101_updated_at DEFAULT SYSUTCDATETIME()
);
GO

CREATE TABLE silver.erp_px_cat_g1v2 (
    ID VARCHAR(20) NOT NULL CONSTRAINT pk_silver_erp_px_cat_g1v2 PRIMARY KEY,
    CAT VARCHAR(50) NULL,
    SUBCAT VARCHAR(50) NULL,
    MAINTENANCE BIT NULL,
    meta_created_at DATETIME2(6) NOT NULL CONSTRAINT df_silver_erp_px_cat_g1v2_created_at DEFAULT SYSUTCDATETIME(),
    meta_updated_at DATETIME2(6) NOT NULL CONSTRAINT df_silver_erp_px_cat_g1v2_updated_at DEFAULT SYSUTCDATETIME()
);
GO