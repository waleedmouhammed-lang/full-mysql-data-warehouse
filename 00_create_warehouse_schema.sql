/*
================================================================================
SCRIPT: 00_create_warehouse_schema.sql
PURPOSE: One-time SQL Server setup for the CustomerSales database and schemas.

TARGET: SQL Server in Docker
DATABASE: CustomerSales
SCHEMAS: bronze, staging, silver, gold, ops

NOTE:
  This script intentionally does not create logins with hardcoded passwords.
  Create SQL Server logins through Docker/env configuration or your admin tool,
  then map them to users/roles in this database as needed.
================================================================================
*/

USE master;
GO

IF DB_ID(N'CustomerSales') IS NULL
BEGIN
    CREATE DATABASE CustomerSales;
END;
GO

USE CustomerSales;
GO

IF SCHEMA_ID(N'bronze') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA bronze');
END;
GO

IF SCHEMA_ID(N'staging') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA staging');
END;
GO

IF SCHEMA_ID(N'silver') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA silver');
END;
GO

IF SCHEMA_ID(N'gold') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA gold');
END;
GO

IF SCHEMA_ID(N'ops') IS NULL
BEGIN
    EXEC(N'CREATE SCHEMA ops');
END;
GO

IF NOT EXISTS (
    SELECT 1
    FROM sys.database_principals
    WHERE name = N'dw_etl_executor'
      AND type = N'R'
)
BEGIN
    CREATE ROLE dw_etl_executor;
END;
GO


GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::bronze TO dw_etl_executor;
GRANT SELECT, INSERT, UPDATE, DELETE, ALTER ON SCHEMA::staging TO dw_etl_executor;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::silver TO dw_etl_executor;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::gold TO dw_etl_executor;
GRANT SELECT, INSERT, UPDATE, DELETE ON SCHEMA::ops TO dw_etl_executor;
GO