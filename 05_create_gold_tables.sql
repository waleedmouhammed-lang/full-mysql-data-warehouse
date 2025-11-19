/*
================================================================================
SCRIPT: 05_create_gold_tables.sql
PURPOSE: Creates the structure (DDL) for the Gold Layer (Star Schema).
         Contains Dimension tables and Fact tables.
WARNING: Running this script will DROP existing tables and data.
================================================================================
*/

USE dw_gold;

-- =============================================================================
-- 1. Dimension: dim_customers
-- =============================================================================
DROP TABLE IF EXISTS dim_customers;

CREATE TABLE dim_customers (
    -- Surrogate Key (PK)
    customer_key INT AUTO_INCREMENT PRIMARY KEY,
    
    -- Business Keys
    customer_id INT,
    customer_number VARCHAR(50),
    
    -- Attributes
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    full_name VARCHAR(100), -- Derived column
    country VARCHAR(50),
    marital_status VARCHAR(20),
    gender VARCHAR(20),
    birthdate DATE,
    create_date DATE,
    
    -- Derived Metrics
    age INT,
    age_group VARCHAR(20),
    
    -- Metadata
    meta_created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes for Performance
    INDEX idx_customer_id (customer_id),
    INDEX idx_country (country),
    INDEX idx_age_group (age_group)
);

-- =============================================================================
-- 2. Dimension: dim_products
-- =============================================================================
DROP TABLE IF EXISTS dim_products;

CREATE TABLE dim_products (
    -- Surrogate Key (PK)
    product_key INT AUTO_INCREMENT PRIMARY KEY,
    
    -- Business Keys
    product_id INT,
    product_number VARCHAR(50),
    product_name VARCHAR(255),
    
    -- Category Hierarchy (Joined from ERP)
    category_id VARCHAR(20),
    category_name VARCHAR(50),
    subcategory_name VARCHAR(50),
    maintenance_flag VARCHAR(10), -- 'Yes'/'No' based on boolean
    
    -- Attributes
    product_cost DECIMAL(19, 4),
    product_line VARCHAR(50),
    
    -- SCD Type 2 History
    start_date DATE,
    end_date DATE,
    is_current BOOLEAN, -- Helper flag for current records
    
    -- Metadata
    meta_created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    -- Indexes
    INDEX idx_product_number (product_number),
    INDEX idx_category (category_name),
    INDEX idx_is_current (is_current)
);

-- =============================================================================
-- 3. Fact Table: fact_sales
-- =============================================================================
DROP TABLE IF EXISTS fact_sales;

CREATE TABLE fact_sales (
    -- Surrogate Key (PK)
    sales_key INT AUTO_INCREMENT PRIMARY KEY,
    
    -- Foreign Keys (Links to Dimensions)
    customer_key INT,
    product_key INT,
    
    -- Business Keys (for traceability)
    order_number VARCHAR(20),
    
    -- Dates
    order_date DATE,
    shipping_date DATE,
    due_date DATE,
    
    -- Measures (Quantitative Data)
    sales_amount DECIMAL(19, 4),
    quantity INT,
    price DECIMAL(19, 4),
    
    -- Metadata
    meta_created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    -- Foreign Key Constraints (Optional but recommended for integrity)
    FOREIGN KEY (customer_key) REFERENCES dim_customers(customer_key),
    FOREIGN KEY (product_key) REFERENCES dim_products(product_key),
    
    -- Indexes
    INDEX idx_order_date (order_date),
    INDEX idx_customer_key (customer_key),
    INDEX idx_product_key (product_key)
);