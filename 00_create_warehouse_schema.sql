/*
================================================================================
SCRIPT: 00_create_warehouse_schema.sql
PURPOSE: One-time SQL Server setup for the warehouse database and schemas.

TARGET: SQL Server in Docker
DATABASE: DataWarehouse
SCHEMAS: bronze, staging, silver, gold, ops

NOTE:
  This script intentionally does not create logins with hardcoded passwords.
  Create SQL Server logins through Docker/env configuration or your admin tool,
  then map them to users/roles in this database as needed.
================================================================================
*/

USE master;
GO

IF DB_ID(N'DataWarehouse') IS NULL
BEGIN
    CREATE DATABASE DataWarehouse;
END;
GO

USE DataWarehouse;
GO

IF SCHEMA_ID(N'bronze') IS NULL
    EXEC(N'CREATE SCHEMA bronze');
GO

IF SCHEMA_ID(N'staging') IS NULL
    EXEC(N'CREATE SCHEMA staging');
GO

IF SCHEMA_ID(N'silver') IS NULL
    EXEC(N'CREATE SCHEMA silver');
GO

IF SCHEMA_ID(N'gold') IS NULL
    EXEC(N'CREATE SCHEMA gold');
GO

IF SCHEMA_ID(N'ops') IS NULL
    EXEC(N'CREATE SCHEMA ops');
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

/*
Optional login mapping:

IF SUSER_ID(N'etl_user') IS NOT NULL AND USER_ID(N'etl_user') IS NULL
BEGIN
    CREATE USER etl_user FOR LOGIN etl_user;
    ALTER ROLE dw_etl_executor ADD MEMBER etl_user;
END;
GO
*/
