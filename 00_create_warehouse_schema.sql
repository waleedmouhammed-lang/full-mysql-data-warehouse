/*
================================================================================
SCRIPT: 00_create_warehouse_schema.sql
PURPOSE: One-time environment setup for the Medallion Data Warehouse.
         This script creates the three logical databases (schemas).

================================================================================
==                                                                            ==
==  WARNING: ADMINISTRATOR ACTION REQUIRED                                    ==
==                                                                            ==
==  This script is a one-time setup task that defines the warehouse structure.  ==
==  It should ONLY be run by a Database Administrator (DBA) with sufficient     ==
==  privileges (e.g., CREATE DATABASE).                                       ==
==                                                                            ==
==  DO NOT include this script in any daily or automated ETL process.         ==
==                                                                            ==
================================================================================
*/

/*
================================================================================
Section 1: Database (Schema) Creation
Purpose: Create the three core databases for the Medallion architecture.
         We use `CREATE DATABASE IF NOT EXISTS` to make this script
         idempotent (safe to re-run without causing errors).
================================================================================
*/

-- Create the Bronze layer database (Raw data ingestion)
DROP DATABASE IF EXISTS dw_bronze;
CREATE DATABASE IF NOT EXISTS dw_bronze
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

-- Create the Silver layer database (Cleaned, conformed, and validated data)
DROP DATABASE IF EXISTS dw_silver;
CREATE DATABASE IF NOT EXISTS dw_silver
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

-- Create the Gold layer database (Business-ready, aggregated data)
DROP DATABASE IF EXISTS dw_gold;
CREATE DATABASE IF NOT EXISTS dw_gold
    CHARACTER SET utf8mb4
    COLLATE utf8mb4_unicode_ci;

/*
================================================================================
Section 2: User and Privilege Management (Example)
Purpose: (Optional but Recommended) Create a dedicated, low-privilege
         user for running the daily ETL processes. This user should NOT
         have admin rights.
================================================================================
*/

/*
-- Example: Create an ETL user
CREATE USER 'etl_user'@'localhost' IDENTIFIED BY 'a-very-strong-password';

-- Grant ONLY the necessary permissions for the Bronze load
-- The ETL user needs to create, drop, and insert into the Bronze tables.
GRANT CREATE, DROP, SELECT, INSERT 
ON dw_bronze.* TO 'etl_user'@'localhost';

-- The ETL user may only need to read from Bronze and write to Silver
GRANT SELECT ON dw_bronze.* TO 'etl_user'@'localhost';
GRANT CREATE, DROP, SELECT, INSERT, UPDATE, DELETE 
ON dw_silver.* TO 'etl_user'@'localhost';

-- Flush privileges to apply the changes
FLUSH PRIVILEGES;
*/

-- End of script --