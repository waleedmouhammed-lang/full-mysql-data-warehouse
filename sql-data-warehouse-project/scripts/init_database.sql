/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'CustomerSales' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'CustomerSales' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/

USE master;
GO

-- Drop and recreate the 'CustomerSales' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'CustomerSales')
BEGIN
    ALTER DATABASE CustomerSales SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE CustomerSales;
END;
GO

-- Create the 'CustomerSales' database
CREATE DATABASE CustomerSales;
GO

USE CustomerSales;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
