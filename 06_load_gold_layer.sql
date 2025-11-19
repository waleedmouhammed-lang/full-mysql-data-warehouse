/*
================================================================================
SCRIPT: 06_load_gold_layer.sql
PURPOSE: Transforms and Loads data into the Gold Layer tables.
STRATEGY: Full Refresh (TRUNCATE + INSERT).
         - Dimensions are loaded first.
         - Facts are loaded last (depending on Dimensions for Surrogate Keys).
================================================================================
*/

USE dw_gold;

-- =============================================================================
-- DEVELOPMENT SWITCH: Disable Foreign Key Checks
-- =============================================================================
-- In Development or Full Refresh mode, we need to truncate tables.
-- MySQL prevents truncating a Dimension table if a Fact table refers to it.
-- We disable checks temporarily to allow the TRUNCATE commands to pass.
-- Comment this line out in Production if you do not want to bypass constraints.
SET FOREIGN_KEY_CHECKS = 0;


-- =============================================================================
-- 1. Load Dimension: dim_customers
-- =============================================================================
TRUNCATE TABLE dim_customers;

INSERT INTO dim_customers (
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
    ci.cst_id,
    ci.cst_key,
    ci.cst_firstname,
    ci.cst_lastname,
    CONCAT(ci.cst_firstname, ' ', ci.cst_lastname) AS full_name,
    COALESCE(la.CNTRY, 'n/a') AS country,
    COALESCE(ci.cst_marital_status, 'n/a') AS marital_status,
    CASE 
        WHEN UPPER(TRIM(ci.cst_gndr)) != 'UNKNOWN' THEN ci.cst_gndr
        ELSE COALESCE(ca.GEN, 'n/a')
    END AS gender,
    ca.BDATE,
    ci.cst_create_date,
    TIMESTAMPDIFF(YEAR, ca.BDATE, CURDATE()) AS age,
    CASE
        WHEN TIMESTAMPDIFF(YEAR, ca.BDATE, CURDATE()) < 20 THEN 'Under 20'
        WHEN TIMESTAMPDIFF(YEAR, ca.BDATE, CURDATE()) BETWEEN 20 AND 29 THEN '20-29'
        WHEN TIMESTAMPDIFF(YEAR, ca.BDATE, CURDATE()) BETWEEN 30 AND 39 THEN '30-39'
        WHEN TIMESTAMPDIFF(YEAR, ca.BDATE, CURDATE()) BETWEEN 40 AND 49 THEN '40-49'
        WHEN TIMESTAMPDIFF(YEAR, ca.BDATE, CURDATE()) >= 50 THEN '50+'
        ELSE 'Unknown'
    END AS age_group
FROM dw_silver.crm_cust_info ci
LEFT JOIN dw_silver.erp_cust_az12 ca ON ci.cst_key = ca.CID
LEFT JOIN dw_silver.erp_loc_a101 la ON ci.cst_key = la.CID;


-- =============================================================================
-- 2. Load Dimension: dim_products
-- =============================================================================
TRUNCATE TABLE dim_products;

INSERT INTO dim_products (
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
    CASE 
        WHEN pn.prd_end_dt IS NULL THEN 1 
        ELSE 0 
    END AS is_current
FROM dw_silver.crm_prd_info pn
LEFT JOIN dw_silver.erp_px_cat_g1v2 pc 
    ON pn.prd_category = pc.ID;


-- =============================================================================
-- 3. Load Fact Table: fact_sales
-- =============================================================================
TRUNCATE TABLE fact_sales;

INSERT INTO fact_sales (
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
    
    -- Surrogate Key Lookup (Product)
    -- Must match the product version active at the time of the order
    dp.product_key,
    
    -- Surrogate Key Lookup (Customer)
    dc.customer_key,
    
    sd.sls_order_dt,
    sd.sls_ship_dt,
    sd.sls_due_dt,
    sd.sls_sales,
    sd.sls_quantity,
    sd.sls_price
FROM dw_silver.crm_sales_details sd
LEFT JOIN dim_products dp
    ON sd.sls_prd_key = dp.product_number
    AND sd.sls_order_dt >= dp.start_date 
    AND (sd.sls_order_dt <= dp.end_date OR dp.end_date IS NULL)
LEFT JOIN dim_customers dc
    ON sd.sls_cust_id = dc.customer_id;


-- =============================================================================
-- RE-ENABLE Foreign Key Checks
-- =============================================================================
SET FOREIGN_KEY_CHECKS = 1;