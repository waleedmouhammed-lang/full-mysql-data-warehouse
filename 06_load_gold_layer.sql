/*
================================================================================
SCRIPT: 06_load_gold_layer.sql
PURPOSE: Loads SQL Server gold star schema from silver tables.
STRATEGY: Ordered full refresh using DELETE to respect foreign keys.
================================================================================
*/

USE DataWarehouse;
GO

SET XACT_ABORT ON;
BEGIN TRANSACTION;

DELETE FROM gold.fact_sales;
DELETE FROM gold.dim_products;
DELETE FROM gold.dim_customers;

DBCC CHECKIDENT ('gold.fact_sales', RESEED, 0);
DBCC CHECKIDENT ('gold.dim_products', RESEED, 0);
DBCC CHECKIDENT ('gold.dim_customers', RESEED, 0);

WITH customer_source AS (
    SELECT
        ci.cst_id AS customer_id,
        ci.cst_key AS customer_number,
        ci.cst_firstname AS first_name,
        ci.cst_lastname AS last_name,
        CONCAT(ci.cst_firstname, ' ', ci.cst_lastname) AS full_name,
        COALESCE(la.CNTRY, 'n/a') AS country,
        COALESCE(ci.cst_marital_status, 'n/a') AS marital_status,
        CASE
            WHEN UPPER(TRIM(ci.cst_gndr)) <> 'UNKNOWN' THEN ci.cst_gndr
            ELSE COALESCE(ca.GEN, 'n/a')
        END AS gender,
        ca.BDATE AS birthdate,
        ci.cst_create_date AS create_date,
        CASE
            WHEN ca.BDATE IS NULL THEN NULL
            ELSE
                DATEDIFF(YEAR, ca.BDATE, CAST(GETDATE() AS DATE))
                - CASE
                    WHEN DATEADD(YEAR, DATEDIFF(YEAR, ca.BDATE, CAST(GETDATE() AS DATE)), ca.BDATE) > CAST(GETDATE() AS DATE)
                    THEN 1 ELSE 0
                  END
        END AS age
    FROM silver.crm_cust_info AS ci
    LEFT JOIN silver.erp_cust_az12 AS ca ON ci.cst_key = ca.CID
    LEFT JOIN silver.erp_loc_a101 AS la ON ci.cst_key = la.CID
)
INSERT INTO gold.dim_customers (
    customer_id,
    customer_number,
    first_name,
    last_name,
    full_name,
    country,
    marital_status,
    gender,
    birthdate,
    create_date,
    age,
    age_group
)
SELECT
    customer_id,
    customer_number,
    first_name,
    last_name,
    full_name,
    country,
    marital_status,
    gender,
    birthdate,
    create_date,
    age,
    CASE
        WHEN age < 20 THEN 'Under 20'
        WHEN age BETWEEN 20 AND 29 THEN '20-29'
        WHEN age BETWEEN 30 AND 39 THEN '30-39'
        WHEN age BETWEEN 40 AND 49 THEN '40-49'
        WHEN age >= 50 THEN '50+'
        ELSE 'Unknown'
    END AS age_group
FROM customer_source;

INSERT INTO gold.dim_products (
    product_id,
    product_number,
    product_name,
    category_id,
    category_name,
    subcategory_name,
    maintenance_flag,
    product_cost,
    product_line,
    start_date,
    end_date,
    is_current
)
SELECT
    pn.prd_id,
    pn.prd_key,
    pn.prd_nm,
    pc.ID AS category_id,
    pc.CAT AS category_name,
    pc.SUBCAT AS subcategory_name,
    CASE WHEN pc.MAINTENANCE = 1 THEN 'Yes' ELSE 'No' END AS maintenance_flag,
    pn.prd_cost,
    pn.prd_line,
    pn.prd_start_dt,
    pn.prd_end_dt,
    CASE WHEN pn.prd_end_dt IS NULL THEN 1 ELSE 0 END AS is_current
FROM silver.crm_prd_info AS pn
LEFT JOIN silver.erp_px_cat_g1v2 AS pc
    ON pn.prd_category = pc.ID;

INSERT INTO gold.fact_sales (
    order_number,
    product_key,
    customer_key,
    order_date,
    shipping_date,
    due_date,
    sales_amount,
    quantity,
    price
)
SELECT
    sd.sls_ord_num,
    dp.product_key,
    dc.customer_key,
    sd.sls_order_dt,
    sd.sls_ship_dt,
    sd.sls_due_dt,
    sd.sls_sales,
    sd.sls_quantity,
    sd.sls_price
FROM silver.crm_sales_details AS sd
LEFT JOIN gold.dim_products AS dp
    ON sd.sls_prd_key = dp.product_number
   AND sd.sls_order_dt >= dp.start_date
   AND (sd.sls_order_dt <= dp.end_date OR dp.end_date IS NULL)
LEFT JOIN gold.dim_customers AS dc
    ON sd.sls_cust_id = dc.customer_id;

COMMIT TRANSACTION;
GO
