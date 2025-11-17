/*
================================================================================
SCRIPT: 01_create_bronze_tables.sql
PURPOSE: Defines and creates all table structures in the Bronze layer.
         ** INCREMENTAL LOAD VERSION **
STRATEGY: We add UNIQUE KEY constraints to the business keys.
          This allows us to use `INSERT IGNORE` or `ON DUPLICATE KEY UPDATE`
          to create an idempotent, incremental load.
          All columns remain VARCHAR(255) for resilience.
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
CREATE TABLE IF NOT EXISTS crm_cust_info(
    cst_id VARCHAR(255),
    cst_key VARCHAR(255),
    cst_firstname VARCHAR(255),
    cst_lastname VARCHAR(255),
    cst_marital_status VARCHAR(255),
    cst_gndr VARCHAR(255),
    cst_create_date VARCHAR(255),
    UNIQUE KEY idx_cst_id (cst_id) -- Business key for deduplication
);

-- Table: CRM Product Info
CREATE TABLE IF NOT EXISTS crm_prd_info(
    prd_id VARCHAR(255),
    prd_key VARCHAR(255),
    prd_nm VARCHAR(255),
    prd_cost VARCHAR(255),
    prd_line VARCHAR(255),
    prd_start_dt VARCHAR(255),
    prd_end_dt VARCHAR(255),
    UNIQUE key idx_prd_id (prd_id) -- Business key for deduplication
);

-- Table: CRM Sales Details
CREATE TABLE IF NOT EXISTS crm_sales_details(
    sls_ord_num VARCHAR(255),
    sls_prd_key VARCHAR(255),
    sls_cust_id VARCHAR(255),
    sls_order_dt VARCHAR(255),
    sls_ship_dt VARCHAR(255),
    sls_due_dt VARCHAR(255),
    sls_sales VARCHAR(255),
    sls_quantity VARCHAR(255),
    sls_price VARCHAR(255),
    UNIQUE KEY idx_sales_detail (sls_ord_num, sls_prd_key) -- Business key for deduplication
);

-- Table: ERP Customer Demographics
CREATE TABLE IF NOT EXISTS erp_cust_az12(
    CID VARCHAR(255),
    BDATE VARCHAR(255),
    GEN VARCHAR(255),
    UNIQUE KEY idx_cid (CID) -- Business key for deduplication
);

-- Table: ERP Customer Location
CREATE TABLE IF NOT EXISTS erp_loc_a101(
    CID VARCHAR(255),
    CNTRY VARCHAR(255),
    UNIQUE KEY idx_cid (CID) -- Business key for deduplication
);

-- Table: ERP Product Category
CREATE TABLE IF NOT EXISTS erp_px_cat_g1v2(
    ID VARCHAR(255),
    CAT VARCHAR(255),
    SUBCAT VARCHAR(255),
    MAINTENANCE VARCHAR(255),
    UNIQUE KEY idx_id (ID) -- Business key for deduplication
);

-- End of script --