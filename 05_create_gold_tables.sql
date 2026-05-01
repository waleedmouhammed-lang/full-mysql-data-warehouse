/*
================================================================================
SCRIPT: 05_create_gold_tables.sql
PURPOSE: Creates SQL Server gold star schema tables.
================================================================================
*/

USE DataWarehouse;
GO

DROP TABLE IF EXISTS gold.fact_sales;
DROP TABLE IF EXISTS gold.dim_products;
DROP TABLE IF EXISTS gold.dim_customers;
GO

CREATE TABLE gold.dim_customers (
    customer_key INT IDENTITY(1,1) NOT NULL CONSTRAINT pk_gold_dim_customers PRIMARY KEY,
    customer_id INT NULL,
    customer_number VARCHAR(50) NULL,
    first_name VARCHAR(50) NULL,
    last_name VARCHAR(50) NULL,
    full_name VARCHAR(100) NULL,
    country VARCHAR(50) NULL,
    marital_status VARCHAR(20) NULL,
    gender VARCHAR(20) NULL,
    birthdate DATE NULL,
    create_date DATE NULL,
    age INT NULL,
    age_group VARCHAR(20) NULL,
    meta_created_at DATETIME2(6) NOT NULL CONSTRAINT df_gold_dim_customers_created_at DEFAULT SYSUTCDATETIME()
);

CREATE INDEX ix_gold_dim_customers_customer_id ON gold.dim_customers(customer_id);
CREATE INDEX ix_gold_dim_customers_country ON gold.dim_customers(country);
CREATE INDEX ix_gold_dim_customers_age_group ON gold.dim_customers(age_group);
GO

CREATE TABLE gold.dim_products (
    product_key INT IDENTITY(1,1) NOT NULL CONSTRAINT pk_gold_dim_products PRIMARY KEY,
    product_id INT NULL,
    product_number VARCHAR(50) NULL,
    product_name VARCHAR(255) NULL,
    category_id VARCHAR(20) NULL,
    category_name VARCHAR(50) NULL,
    subcategory_name VARCHAR(50) NULL,
    maintenance_flag VARCHAR(10) NULL,
    product_cost DECIMAL(19, 4) NULL,
    product_line VARCHAR(50) NULL,
    start_date DATE NULL,
    end_date DATE NULL,
    is_current BIT NULL,
    meta_created_at DATETIME2(6) NOT NULL CONSTRAINT df_gold_dim_products_created_at DEFAULT SYSUTCDATETIME()
);

CREATE INDEX ix_gold_dim_products_product_number ON gold.dim_products(product_number);
CREATE INDEX ix_gold_dim_products_category ON gold.dim_products(category_name);
CREATE INDEX ix_gold_dim_products_is_current ON gold.dim_products(is_current);
GO

CREATE TABLE gold.fact_sales (
    sales_key INT IDENTITY(1,1) NOT NULL CONSTRAINT pk_gold_fact_sales PRIMARY KEY,
    customer_key INT NULL,
    product_key INT NULL,
    order_number VARCHAR(20) NULL,
    order_date DATE NULL,
    shipping_date DATE NULL,
    due_date DATE NULL,
    sales_amount DECIMAL(19, 4) NULL,
    quantity INT NULL,
    price DECIMAL(19, 4) NULL,
    meta_created_at DATETIME2(6) NOT NULL CONSTRAINT df_gold_fact_sales_created_at DEFAULT SYSUTCDATETIME(),
    CONSTRAINT fk_gold_fact_sales_dim_customers FOREIGN KEY (customer_key) REFERENCES gold.dim_customers(customer_key),
    CONSTRAINT fk_gold_fact_sales_dim_products FOREIGN KEY (product_key) REFERENCES gold.dim_products(product_key)
);

CREATE INDEX ix_gold_fact_sales_order_date ON gold.fact_sales(order_date);
CREATE INDEX ix_gold_fact_sales_customer_key ON gold.fact_sales(customer_key);
CREATE INDEX ix_gold_fact_sales_product_key ON gold.fact_sales(product_key);
GO
