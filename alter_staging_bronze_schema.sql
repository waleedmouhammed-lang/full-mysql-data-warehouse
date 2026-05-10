/*
================================================================================
SCRIPT: 07_load_bronze_practice_sql.sql
PURPOSE: Practice SQL-only loader for staging and bronze.

WHAT THIS SCRIPT TEACHES:
  1. SQL Server reads CSV files directly.
  2. Staging tables are fully refreshed with TRUNCATE + INSERT.
  3. Bronze tables are loaded with an upsert pattern: UPDATE first, INSERT second.

DOCKER / MAC / LINUX WORKFLOW WITH AN EXISTING sqlserver2022 CONTAINER:
  This script expects the CSV files to be available inside the SQL Server
  container at:

      /var/opt/mssql/datasets

  If you do not want to recreate the existing sqlserver2022 container, copy the
  datasets folder into it:

      docker cp datasets sqlserver2022:/var/opt/mssql/datasets

  Then SQL Server can read paths such as:

      /var/opt/mssql/datasets/source_crm/cust_info.csv

WINDOWS WORKFLOW:
  Option A: SQL Server in Docker Desktop on Windows.
  Copy the datasets folder into the existing container:

      docker cp datasets sqlserver2022:/var/opt/mssql/datasets

  Keep the Linux container paths in this script.

  Option B: SQL Server running directly on Windows.
  Copy the datasets folder to a path SQL Server can read, for example:

      C:\sql\dwh_project\datasets

  Then replace each BULK path in this script with the Windows equivalent, for
  example:

      /var/opt/mssql/datasets/source_crm/cust_info.csv

  becomes:

      C:\sql\dwh_project\datasets\source_crm\cust_info.csv

IMPORTANT:
  This practice script inserts only the source/business columns. It assumes the
  metadata columns in staging and bronze allow NULL values.
================================================================================
*/
USE CustomerSales;
GO

PRINT 'Making metadata columns nullable for the practice loader...';
GO

ALTER TABLE staging.crm_cust_info ALTER COLUMN batch_id UNIQUEIDENTIFIER NULL;
ALTER TABLE staging.crm_cust_info ALTER COLUMN source_file NVARCHAR(4000) NULL;
ALTER TABLE staging.crm_cust_info ALTER COLUMN source_row_number INT NULL;
ALTER TABLE staging.crm_cust_info ALTER COLUMN loaded_at DATETIME2(6) NULL;
ALTER TABLE staging.crm_cust_info ALTER COLUMN row_hash VARBINARY(32) NULL;

ALTER TABLE staging.crm_prd_info ALTER COLUMN batch_id UNIQUEIDENTIFIER NULL;
ALTER TABLE staging.crm_prd_info ALTER COLUMN source_file NVARCHAR(4000) NULL;
ALTER TABLE staging.crm_prd_info ALTER COLUMN source_row_number INT NULL;
ALTER TABLE staging.crm_prd_info ALTER COLUMN loaded_at DATETIME2(6) NULL;
ALTER TABLE staging.crm_prd_info ALTER COLUMN row_hash VARBINARY(32) NULL;

ALTER TABLE staging.crm_sales_details ALTER COLUMN batch_id UNIQUEIDENTIFIER NULL;
ALTER TABLE staging.crm_sales_details ALTER COLUMN source_file NVARCHAR(4000) NULL;
ALTER TABLE staging.crm_sales_details ALTER COLUMN source_row_number INT NULL;
ALTER TABLE staging.crm_sales_details ALTER COLUMN loaded_at DATETIME2(6) NULL;
ALTER TABLE staging.crm_sales_details ALTER COLUMN row_hash VARBINARY(32) NULL;

ALTER TABLE staging.erp_cust_az12 ALTER COLUMN batch_id UNIQUEIDENTIFIER NULL;
ALTER TABLE staging.erp_cust_az12 ALTER COLUMN source_file NVARCHAR(4000) NULL;
ALTER TABLE staging.erp_cust_az12 ALTER COLUMN source_row_number INT NULL;
ALTER TABLE staging.erp_cust_az12 ALTER COLUMN loaded_at DATETIME2(6) NULL;
ALTER TABLE staging.erp_cust_az12 ALTER COLUMN row_hash VARBINARY(32) NULL;

ALTER TABLE staging.erp_loc_a101 ALTER COLUMN batch_id UNIQUEIDENTIFIER NULL;
ALTER TABLE staging.erp_loc_a101 ALTER COLUMN source_file NVARCHAR(4000) NULL;
ALTER TABLE staging.erp_loc_a101 ALTER COLUMN source_row_number INT NULL;
ALTER TABLE staging.erp_loc_a101 ALTER COLUMN loaded_at DATETIME2(6) NULL;
ALTER TABLE staging.erp_loc_a101 ALTER COLUMN row_hash VARBINARY(32) NULL;

ALTER TABLE staging.erp_px_cat_g1v2 ALTER COLUMN batch_id UNIQUEIDENTIFIER NULL;
ALTER TABLE staging.erp_px_cat_g1v2 ALTER COLUMN source_file NVARCHAR(4000) NULL;
ALTER TABLE staging.erp_px_cat_g1v2 ALTER COLUMN source_row_number INT NULL;
ALTER TABLE staging.erp_px_cat_g1v2 ALTER COLUMN loaded_at DATETIME2(6) NULL;
ALTER TABLE staging.erp_px_cat_g1v2 ALTER COLUMN row_hash VARBINARY(32) NULL;

ALTER TABLE bronze.crm_cust_info ALTER COLUMN batch_id UNIQUEIDENTIFIER NULL;
ALTER TABLE bronze.crm_cust_info ALTER COLUMN source_file NVARCHAR(4000) NULL;
ALTER TABLE bronze.crm_cust_info ALTER COLUMN source_row_number INT NULL;
ALTER TABLE bronze.crm_cust_info ALTER COLUMN loaded_at DATETIME2(6) NULL;
ALTER TABLE bronze.crm_cust_info ALTER COLUMN row_hash VARBINARY(32) NULL;

ALTER TABLE bronze.crm_prd_info ALTER COLUMN batch_id UNIQUEIDENTIFIER NULL;
ALTER TABLE bronze.crm_prd_info ALTER COLUMN source_file NVARCHAR(4000) NULL;
ALTER TABLE bronze.crm_prd_info ALTER COLUMN source_row_number INT NULL;
ALTER TABLE bronze.crm_prd_info ALTER COLUMN loaded_at DATETIME2(6) NULL;
ALTER TABLE bronze.crm_prd_info ALTER COLUMN row_hash VARBINARY(32) NULL;

ALTER TABLE bronze.crm_sales_details ALTER COLUMN batch_id UNIQUEIDENTIFIER NULL;
ALTER TABLE bronze.crm_sales_details ALTER COLUMN source_file NVARCHAR(4000) NULL;
ALTER TABLE bronze.crm_sales_details ALTER COLUMN source_row_number INT NULL;
ALTER TABLE bronze.crm_sales_details ALTER COLUMN loaded_at DATETIME2(6) NULL;
ALTER TABLE bronze.crm_sales_details ALTER COLUMN row_hash VARBINARY(32) NULL;

ALTER TABLE bronze.erp_cust_az12 ALTER COLUMN batch_id UNIQUEIDENTIFIER NULL;
ALTER TABLE bronze.erp_cust_az12 ALTER COLUMN source_file NVARCHAR(4000) NULL;
ALTER TABLE bronze.erp_cust_az12 ALTER COLUMN source_row_number INT NULL;
ALTER TABLE bronze.erp_cust_az12 ALTER COLUMN loaded_at DATETIME2(6) NULL;
ALTER TABLE bronze.erp_cust_az12 ALTER COLUMN row_hash VARBINARY(32) NULL;

ALTER TABLE bronze.erp_loc_a101 ALTER COLUMN batch_id UNIQUEIDENTIFIER NULL;
ALTER TABLE bronze.erp_loc_a101 ALTER COLUMN source_file NVARCHAR(4000) NULL;
ALTER TABLE bronze.erp_loc_a101 ALTER COLUMN source_row_number INT NULL;
ALTER TABLE bronze.erp_loc_a101 ALTER COLUMN loaded_at DATETIME2(6) NULL;
ALTER TABLE bronze.erp_loc_a101 ALTER COLUMN row_hash VARBINARY(32) NULL;

ALTER TABLE bronze.erp_px_cat_g1v2 ALTER COLUMN batch_id UNIQUEIDENTIFIER NULL;
ALTER TABLE bronze.erp_px_cat_g1v2 ALTER COLUMN source_file NVARCHAR(4000) NULL;
ALTER TABLE bronze.erp_px_cat_g1v2 ALTER COLUMN source_row_number INT NULL;
ALTER TABLE bronze.erp_px_cat_g1v2 ALTER COLUMN loaded_at DATETIME2(6) NULL;
ALTER TABLE bronze.erp_px_cat_g1v2 ALTER COLUMN row_hash VARBINARY(32) NULL;
GO