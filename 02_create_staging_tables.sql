/*
================================================================================
SCRIPT: 02_create_staging_tables.sql
PURPOSE: Creates SQL Server staging tables used by the Python bronze loader.
================================================================================
*/

USE CustomerSales;
GO

IF OBJECT_ID(N'staging.crm_cust_info', N'U') IS NULL
BEGIN
    CREATE TABLE staging.crm_cust_info (
        cst_id VARCHAR(255) NULL,
        cst_key VARCHAR(255) NULL,
        cst_firstname VARCHAR(255) NULL,
        cst_lastname VARCHAR(255) NULL,
        cst_marital_status VARCHAR(255) NULL,
        cst_gndr VARCHAR(255) NULL,
        cst_create_date VARCHAR(255) NULL,
        batch_id UNIQUEIDENTIFIER NULL,
        source_file NVARCHAR(4000) NULL,
        source_row_number INT NULL,
        loaded_at DATETIME2(6) NULL,
        row_hash VARBINARY(32) NULL
    );
END;
GO

IF OBJECT_ID(N'staging.crm_prd_info', N'U') IS NULL
BEGIN
    CREATE TABLE staging.crm_prd_info (
        prd_id VARCHAR(255) NULL,
        prd_key VARCHAR(255) NULL,
        prd_nm VARCHAR(255) NULL,
        prd_cost VARCHAR(255) NULL,
        prd_line VARCHAR(255) NULL,
        prd_start_dt VARCHAR(255) NULL,
        prd_end_dt VARCHAR(255) NULL,
        batch_id UNIQUEIDENTIFIER NULL,
        source_file NVARCHAR(4000) NULL,
        source_row_number INT NULL,
        loaded_at DATETIME2(6) NULL,
        row_hash VARBINARY(32) NULL
    );
END;
GO

IF OBJECT_ID(N'staging.crm_sales_details', N'U') IS NULL
BEGIN
    CREATE TABLE staging.crm_sales_details (
        sls_ord_num VARCHAR(255) NULL,
        sls_prd_key VARCHAR(255) NULL,
        sls_cust_id VARCHAR(255) NULL,
        sls_order_dt VARCHAR(255) NULL,
        sls_ship_dt VARCHAR(255) NULL,
        sls_due_dt VARCHAR(255) NULL,
        sls_sales VARCHAR(255) NULL,
        sls_quantity VARCHAR(255) NULL,
        sls_price VARCHAR(255) NULL,
        batch_id UNIQUEIDENTIFIER NULL,
        source_file NVARCHAR(4000) NULL,
        source_row_number INT NULL,
        loaded_at DATETIME2(6) NULL,
        row_hash VARBINARY(32) NULL
    );
END;
GO

IF OBJECT_ID(N'staging.erp_cust_az12', N'U') IS NULL
BEGIN
    CREATE TABLE staging.erp_cust_az12 (
        CID VARCHAR(255) NULL,
        BDATE VARCHAR(255) NULL,
        GEN VARCHAR(255) NULL,
        batch_id UNIQUEIDENTIFIER NULL,
        source_file NVARCHAR(4000) NULL,
        source_row_number INT NULL,
        loaded_at DATETIME2(6) NULL,
        row_hash VARBINARY(32) NULL
    );
END;
GO

IF OBJECT_ID(N'staging.erp_loc_a101', N'U') IS NULL
BEGIN
    CREATE TABLE staging.erp_loc_a101 (
        CID VARCHAR(255) NULL,
        CNTRY VARCHAR(255) NULL,
        batch_id UNIQUEIDENTIFIER NULL,
        source_file NVARCHAR(4000) NULL,
        source_row_number INT NULL,
        loaded_at DATETIME2(6) NULL,
        row_hash VARBINARY(32) NULL
    );
END;
GO

IF OBJECT_ID(N'staging.erp_px_cat_g1v2', N'U') IS NULL
BEGIN
    CREATE TABLE staging.erp_px_cat_g1v2 (
        ID VARCHAR(255) NULL,
        CAT VARCHAR(255) NULL,
        SUBCAT VARCHAR(255) NULL,
        MAINTENANCE VARCHAR(255) NULL,
        batch_id UNIQUEIDENTIFIER NULL,
        source_file NVARCHAR(4000) NULL,
        source_row_number INT NULL,
        loaded_at DATETIME2(6) NULL,
        row_hash VARBINARY(32) NULL
    );
END;
GO
