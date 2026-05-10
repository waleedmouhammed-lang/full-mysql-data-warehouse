/*
================================================================================
SCRIPT: 01_create_bronze_tables.sql
PURPOSE: Creates SQL Server bronze tables for raw, string-first data capture.

Bronze tables keep source values as VARCHAR, plus load metadata:
  - batch_id
  - source_file
  - source_row_number
  - loaded_at
  - row_hash
================================================================================
*/

USE CustomerSales;
GO

IF OBJECT_ID(N'bronze.crm_cust_info', N'U') IS NULL
BEGIN
    CREATE TABLE bronze.crm_cust_info (
        bronze_record_id BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT pk_bronze_crm_cust_info PRIMARY KEY,
        cst_id VARCHAR(255) NULL,
        cst_key VARCHAR(255) NULL,
        cst_firstname VARCHAR(255) NULL,
        cst_lastname VARCHAR(255) NULL,
        cst_marital_status VARCHAR(255) NULL,
        cst_gndr VARCHAR(255) NULL,
        cst_create_date VARCHAR(255) NULL,
        batch_id UNIQUEIDENTIFIER NOT NULL,
        source_file NVARCHAR(4000) NOT NULL,
        source_row_number INT NOT NULL,
        loaded_at DATETIME2(6) NOT NULL,
        row_hash VARBINARY(32) NOT NULL
    );

    CREATE UNIQUE INDEX ux_bronze_crm_cust_info_cst_id
        ON bronze.crm_cust_info(cst_id)
        WHERE cst_id IS NOT NULL;
END;
GO

IF OBJECT_ID(N'bronze.crm_prd_info', N'U') IS NULL
BEGIN
    CREATE TABLE bronze.crm_prd_info (
        bronze_record_id BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT pk_bronze_crm_prd_info PRIMARY KEY,
        prd_id VARCHAR(255) NULL,
        prd_key VARCHAR(255) NULL,
        prd_nm VARCHAR(255) NULL,
        prd_cost VARCHAR(255) NULL,
        prd_line VARCHAR(255) NULL,
        prd_start_dt VARCHAR(255) NULL,
        prd_end_dt VARCHAR(255) NULL,
        batch_id UNIQUEIDENTIFIER NOT NULL,
        source_file NVARCHAR(4000) NOT NULL,
        source_row_number INT NOT NULL,
        loaded_at DATETIME2(6) NOT NULL,
        row_hash VARBINARY(32) NOT NULL
    );

    CREATE UNIQUE INDEX ux_bronze_crm_prd_info_prd_id
        ON bronze.crm_prd_info(prd_id)
        WHERE prd_id IS NOT NULL;
END;
GO

IF OBJECT_ID(N'bronze.crm_sales_details', N'U') IS NULL
BEGIN
    CREATE TABLE bronze.crm_sales_details (
        bronze_record_id BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT pk_bronze_crm_sales_details PRIMARY KEY,
        sls_ord_num VARCHAR(255) NULL,
        sls_prd_key VARCHAR(255) NULL,
        sls_cust_id VARCHAR(255) NULL,
        sls_order_dt VARCHAR(255) NULL,
        sls_ship_dt VARCHAR(255) NULL,
        sls_due_dt VARCHAR(255) NULL,
        sls_sales VARCHAR(255) NULL,
        sls_quantity VARCHAR(255) NULL,
        sls_price VARCHAR(255) NULL,
        batch_id UNIQUEIDENTIFIER NOT NULL,
        source_file NVARCHAR(4000) NOT NULL,
        source_row_number INT NOT NULL,
        loaded_at DATETIME2(6) NOT NULL,
        row_hash VARBINARY(32) NOT NULL
    );

    CREATE UNIQUE INDEX ux_bronze_crm_sales_details_order_product
        ON bronze.crm_sales_details(sls_ord_num, sls_prd_key)
        WHERE sls_ord_num IS NOT NULL AND sls_prd_key IS NOT NULL;
END;
GO

IF OBJECT_ID(N'bronze.erp_cust_az12', N'U') IS NULL
BEGIN
    CREATE TABLE bronze.erp_cust_az12 (
        bronze_record_id BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT pk_bronze_erp_cust_az12 PRIMARY KEY,
        CID VARCHAR(255) NULL,
        BDATE VARCHAR(255) NULL,
        GEN VARCHAR(255) NULL,
        batch_id UNIQUEIDENTIFIER NOT NULL,
        source_file NVARCHAR(4000) NOT NULL,
        source_row_number INT NOT NULL,
        loaded_at DATETIME2(6) NOT NULL,
        row_hash VARBINARY(32) NOT NULL
    );

    CREATE UNIQUE INDEX ux_bronze_erp_cust_az12_cid
        ON bronze.erp_cust_az12(CID)
        WHERE CID IS NOT NULL;
END;
GO

IF OBJECT_ID(N'bronze.erp_loc_a101', N'U') IS NULL
BEGIN
    CREATE TABLE bronze.erp_loc_a101 (
        bronze_record_id BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT pk_bronze_erp_loc_a101 PRIMARY KEY,
        CID VARCHAR(255) NULL,
        CNTRY VARCHAR(255) NULL,
        batch_id UNIQUEIDENTIFIER NOT NULL,
        source_file NVARCHAR(4000) NOT NULL,
        source_row_number INT NOT NULL,
        loaded_at DATETIME2(6) NOT NULL,
        row_hash VARBINARY(32) NOT NULL
    );

    CREATE UNIQUE INDEX ux_bronze_erp_loc_a101_cid
        ON bronze.erp_loc_a101(CID)
        WHERE CID IS NOT NULL;
END;
GO

IF OBJECT_ID(N'bronze.erp_px_cat_g1v2', N'U') IS NULL
BEGIN
    CREATE TABLE bronze.erp_px_cat_g1v2 (
        bronze_record_id BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT pk_bronze_erp_px_cat_g1v2 PRIMARY KEY,
        ID VARCHAR(255) NULL,
        CAT VARCHAR(255) NULL,
        SUBCAT VARCHAR(255) NULL,
        MAINTENANCE VARCHAR(255) NULL,
        batch_id UNIQUEIDENTIFIER NOT NULL,
        source_file NVARCHAR(4000) NOT NULL,
        source_row_number INT NOT NULL,
        loaded_at DATETIME2(6) NOT NULL,
        row_hash VARBINARY(32) NOT NULL
    );

    CREATE UNIQUE INDEX ux_bronze_erp_px_cat_g1v2_id
        ON bronze.erp_px_cat_g1v2(ID)
        WHERE ID IS NOT NULL;
END;
GO