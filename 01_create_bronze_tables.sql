/*
================================================================================
SCRIPT: 01_create_bronze_tables.sql
PURPOSE: Defines and creates all table structures in the Bronze layer.
RUN AS:  ETL User
RUN ON:  Run once during initial setup and again *only* when deploying
         a schema change (e.g., adding a new column).
         DO NOT run this daily.
================================================================================
*/

USE dw_bronze;

/*
================================================================================
Section 1: Bronze Table Schema Definition
Purpose: Create the table structures if they do not already exist.
Strategy: We use `CREATE TABLE IF NOT EXISTS` so this script is safe
          to re-run. It will not destroy or alter existing tables.
          All columns remain VARCHAR(255) for a resilient raw-data-capture.
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

-- End of script --