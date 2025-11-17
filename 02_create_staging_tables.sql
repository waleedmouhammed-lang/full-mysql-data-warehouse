/*
================================================================================
SCRIPT: 02_create_staging_tables.sql
PURPOSE: Creates transient staging tables for the incremental Bronze load.
RUN AS:  ETL User
RUN ON:  Run once during initial setup.
STRATEGY: These tables are structurally identical to the final
          Bronze tables but DO NOT have constraints (like UNIQUE KEYs).
          This allows them to be rapidly truncated and loaded with
          the raw file content, including duplicates, before the
          SQL merge logic is applied.
================================================================================
*/

USE dw_bronze;

SELECT 'Creating staging tables...' AS 'Admin_Status';

-- Staging table for: crm_cust_info
DROP TABLE IF EXISTS stg_crm_cust_info;
CREATE TABLE stg_crm_cust_info LIKE crm_cust_info;
-- We must drop the unique key that `LIKE` copies
ALTER TABLE stg_crm_cust_info DROP KEY idx_cst_id;


-- Staging table for: crm_prd_info
DROP TABLE IF EXISTS stg_crm_prd_info;
CREATE TABLE stg_crm_prd_info LIKE crm_prd_info;
ALTER TABLE stg_crm_prd_info DROP KEY idx_prd_id;


-- Staging table for: crm_sales_details
DROP TABLE IF EXISTS stg_crm_sales_details;
CREATE TABLE stg_crm_sales_details LIKE crm_sales_details;
ALTER TABLE stg_crm_sales_details DROP KEY idx_sales_detail;


-- Staging table for: erp_cust_az12
DROP TABLE IF EXISTS stg_erp_cust_az12;
CREATE TABLE stg_erp_cust_az12 LIKE erp_cust_az12;
ALTER TABLE stg_erp_cust_az12 DROP KEY idx_cid;


-- Staging table for: erp_loc_a101
DROP TABLE IF EXISTS stg_erp_loc_a101;
CREATE TABLE stg_erp_loc_a101 LIKE erp_loc_a101;
ALTER TABLE stg_erp_loc_a101 DROP KEY idx_cid;


-- Staging table for: erp_px_cat_g1v2
DROP TABLE IF EXISTS stg_erp_px_cat_g1v2;
CREATE TABLE stg_erp_px_cat_g1v2 LIKE erp_px_cat_g1v2;
ALTER TABLE stg_erp_px_cat_g1v2 DROP KEY idx_id;


SELECT 'Staging tables created successfully.' AS 'Admin_Status';

-- End of script --