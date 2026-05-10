USE CustomerSales;
GO
PRINT 'Loading CSV files into staging tables...';
GO

TRUNCATE TABLE staging.crm_cust_info;

INSERT INTO staging.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
FROM OPENROWSET(
    BULK '/var/opt/mssql/datasets/source_crm/cust_info.csv',
    FORMAT = 'CSV',
    FIRSTROW = 2
)
WITH (
    cst_id VARCHAR(255) 1,
    cst_key VARCHAR(255) 2,
    cst_firstname VARCHAR(255) 3,
    cst_lastname VARCHAR(255) 4,
    cst_marital_status VARCHAR(255) 5,
    cst_gndr VARCHAR(255) 6,
    cst_create_date VARCHAR(255) 7
) AS rows;

TRUNCATE TABLE staging.crm_prd_info;

INSERT INTO staging.crm_prd_info (
    prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
    prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
FROM OPENROWSET(
    BULK '/var/opt/mssql/datasets/source_crm/prd_info.csv',
    FORMAT = 'CSV',
    FIRSTROW = 2
)
WITH (
    prd_id VARCHAR(255) 1,
    prd_key VARCHAR(255) 2,
    prd_nm VARCHAR(255) 3,
    prd_cost VARCHAR(255) 4,
    prd_line VARCHAR(255) 5,
    prd_start_dt VARCHAR(255) 6,
    prd_end_dt VARCHAR(255) 7
) AS rows;

TRUNCATE TABLE staging.crm_sales_details;

INSERT INTO staging.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
FROM OPENROWSET(
    BULK '/var/opt/mssql/datasets/source_crm/sales_details.csv',
    FORMAT = 'CSV',
    FIRSTROW = 2
)
WITH (
    sls_ord_num VARCHAR(255) 1,
    sls_prd_key VARCHAR(255) 2,
    sls_cust_id VARCHAR(255) 3,
    sls_order_dt VARCHAR(255) 4,
    sls_ship_dt VARCHAR(255) 5,
    sls_due_dt VARCHAR(255) 6,
    sls_sales VARCHAR(255) 7,
    sls_quantity VARCHAR(255) 8,
    sls_price VARCHAR(255) 9
) AS rows;

TRUNCATE TABLE staging.erp_cust_az12;

INSERT INTO staging.erp_cust_az12 (
    CID,
    BDATE,
    GEN
)
SELECT
    CID,
    BDATE,
    GEN
FROM OPENROWSET(
    BULK '/var/opt/mssql/datasets/source_erp/CUST_AZ12.csv',
    FORMAT = 'CSV',
    FIRSTROW = 2
)
WITH (
    CID VARCHAR(255) 1,
    BDATE VARCHAR(255) 2,
    GEN VARCHAR(255) 3
) AS rows;

TRUNCATE TABLE staging.erp_loc_a101;

INSERT INTO staging.erp_loc_a101 (
    CID,
    CNTRY
)
SELECT
    CID,
    CNTRY
FROM OPENROWSET(
    BULK '/var/opt/mssql/datasets/source_erp/LOC_A101.csv',
    FORMAT = 'CSV',
    FIRSTROW = 2
)
WITH (
    CID VARCHAR(255) 1,
    CNTRY VARCHAR(255) 2
) AS rows;

TRUNCATE TABLE staging.erp_px_cat_g1v2;

INSERT INTO staging.erp_px_cat_g1v2 (
    ID,
    CAT,
    SUBCAT,
    MAINTENANCE
)
SELECT
    ID,
    CAT,
    SUBCAT,
    MAINTENANCE
FROM OPENROWSET(
    BULK '/var/opt/mssql/datasets/source_erp/PX_CAT_G1V2.csv',
    FORMAT = 'CSV',
    FIRSTROW = 2
)
WITH (
    ID VARCHAR(255) 1,
    CAT VARCHAR(255) 2,
    SUBCAT VARCHAR(255) 3,
    MAINTENANCE VARCHAR(255) 4
) AS rows;
GO

-- This is the same upsert pattern as in the SQL Server module, but without the metadata columns.
PRINT 'Upserting staging tables into bronze tables...';
GO

-- This is the update pattern for a table with a single natural key column (cst_id).
;WITH source AS (
    SELECT
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id
            ORDER BY cst_create_date DESC
        ) AS rn
    FROM staging.crm_cust_info
    WHERE cst_id IS NOT NULL
      AND LTRIM(RTRIM(cst_id)) <> ''
)
UPDATE target
SET
    target.cst_key = source.cst_key,
    target.cst_firstname = source.cst_firstname,
    target.cst_lastname = source.cst_lastname,
    target.cst_marital_status = source.cst_marital_status,
    target.cst_gndr = source.cst_gndr,
    target.cst_create_date = source.cst_create_date
FROM bronze.crm_cust_info AS target
INNER JOIN source
    ON target.cst_id = source.cst_id
WHERE source.rn = 1;

;WITH source AS (
    SELECT
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date,
        ROW_NUMBER() OVER (
            PARTITION BY cst_id
            ORDER BY cst_create_date DESC
        ) AS rn
    FROM staging.crm_cust_info
    WHERE cst_id IS NOT NULL
      AND LTRIM(RTRIM(cst_id)) <> ''
)
INSERT INTO bronze.crm_cust_info (
    cst_id,
    cst_key,
    cst_firstname,
    cst_lastname,
    cst_marital_status,
    cst_gndr,
    cst_create_date
)
SELECT
    source.cst_id,
    source.cst_key,
    source.cst_firstname,
    source.cst_lastname,
    source.cst_marital_status,
    source.cst_gndr,
    source.cst_create_date
FROM source
WHERE source.rn = 1
  AND NOT EXISTS (
      SELECT 1
      FROM bronze.crm_cust_info AS target
      WHERE target.cst_id = source.cst_id
  );

;WITH source AS (
    SELECT
        prd_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt,
        ROW_NUMBER() OVER (
            PARTITION BY prd_id
            ORDER BY prd_id
        ) AS rn
    FROM staging.crm_prd_info
    WHERE prd_id IS NOT NULL
      AND LTRIM(RTRIM(prd_id)) <> ''
)
UPDATE target
SET
    target.prd_key = source.prd_key,
    target.prd_nm = source.prd_nm,
    target.prd_cost = source.prd_cost,
    target.prd_line = source.prd_line,
    target.prd_start_dt = source.prd_start_dt,
    target.prd_end_dt = source.prd_end_dt
FROM bronze.crm_prd_info AS target
INNER JOIN source
    ON target.prd_id = source.prd_id
WHERE source.rn = 1;

;WITH source AS (
    SELECT
        prd_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt,
        ROW_NUMBER() OVER (
            PARTITION BY prd_id
            ORDER BY prd_id
        ) AS rn
    FROM staging.crm_prd_info
    WHERE prd_id IS NOT NULL
      AND LTRIM(RTRIM(prd_id)) <> ''
)
INSERT INTO bronze.crm_prd_info (
    prd_id,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    prd_end_dt
)
SELECT
    source.prd_id,
    source.prd_key,
    source.prd_nm,
    source.prd_cost,
    source.prd_line,
    source.prd_start_dt,
    source.prd_end_dt
FROM source
WHERE source.rn = 1
  AND NOT EXISTS (
      SELECT 1
      FROM bronze.crm_prd_info AS target
      WHERE target.prd_id = source.prd_id
  );

;WITH source AS (
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price,
        ROW_NUMBER() OVER (
            PARTITION BY sls_ord_num, sls_prd_key
            ORDER BY sls_ord_num, sls_prd_key
        ) AS rn
    FROM staging.crm_sales_details
    WHERE sls_ord_num IS NOT NULL
      AND LTRIM(RTRIM(sls_ord_num)) <> ''
      AND sls_prd_key IS NOT NULL
      AND LTRIM(RTRIM(sls_prd_key)) <> ''
)
UPDATE target
SET
    target.sls_cust_id = source.sls_cust_id,
    target.sls_order_dt = source.sls_order_dt,
    target.sls_ship_dt = source.sls_ship_dt,
    target.sls_due_dt = source.sls_due_dt,
    target.sls_sales = source.sls_sales,
    target.sls_quantity = source.sls_quantity,
    target.sls_price = source.sls_price
FROM bronze.crm_sales_details AS target
INNER JOIN source
    ON target.sls_ord_num = source.sls_ord_num
   AND target.sls_prd_key = source.sls_prd_key
WHERE source.rn = 1;

;WITH source AS (
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price,
        ROW_NUMBER() OVER (
            PARTITION BY sls_ord_num, sls_prd_key
            ORDER BY sls_ord_num, sls_prd_key
        ) AS rn
    FROM staging.crm_sales_details
    WHERE sls_ord_num IS NOT NULL
      AND LTRIM(RTRIM(sls_ord_num)) <> ''
      AND sls_prd_key IS NOT NULL
      AND LTRIM(RTRIM(sls_prd_key)) <> ''
)
INSERT INTO bronze.crm_sales_details (
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    sls_order_dt,
    sls_ship_dt,
    sls_due_dt,
    sls_sales,
    sls_quantity,
    sls_price
)
SELECT
    source.sls_ord_num,
    source.sls_prd_key,
    source.sls_cust_id,
    source.sls_order_dt,
    source.sls_ship_dt,
    source.sls_due_dt,
    source.sls_sales,
    source.sls_quantity,
    source.sls_price
FROM source
WHERE source.rn = 1
  AND NOT EXISTS (
      SELECT 1
      FROM bronze.crm_sales_details AS target
      WHERE target.sls_ord_num = source.sls_ord_num
        AND target.sls_prd_key = source.sls_prd_key
  );

;WITH source AS (
    SELECT
        CID,
        BDATE,
        GEN,
        ROW_NUMBER() OVER (
            PARTITION BY CID
            ORDER BY CID
        ) AS rn
    FROM staging.erp_cust_az12
    WHERE CID IS NOT NULL
      AND LTRIM(RTRIM(CID)) <> ''
)
UPDATE target
SET
    target.BDATE = source.BDATE,
    target.GEN = source.GEN
FROM bronze.erp_cust_az12 AS target
INNER JOIN source
    ON target.CID = source.CID
WHERE source.rn = 1;

;WITH source AS (
    SELECT
        CID,
        BDATE,
        GEN,
        ROW_NUMBER() OVER (
            PARTITION BY CID
            ORDER BY CID
        ) AS rn
    FROM staging.erp_cust_az12
    WHERE CID IS NOT NULL
      AND LTRIM(RTRIM(CID)) <> ''
)
INSERT INTO bronze.erp_cust_az12 (
    CID,
    BDATE,
    GEN
)
SELECT
    source.CID,
    source.BDATE,
    source.GEN
FROM source
WHERE source.rn = 1
  AND NOT EXISTS (
      SELECT 1
      FROM bronze.erp_cust_az12 AS target
      WHERE target.CID = source.CID
  );

;WITH source AS (
    SELECT
        CID,
        CNTRY,
        ROW_NUMBER() OVER (
            PARTITION BY CID
            ORDER BY CID
        ) AS rn
    FROM staging.erp_loc_a101
    WHERE CID IS NOT NULL
      AND LTRIM(RTRIM(CID)) <> ''
)
UPDATE target
SET
    target.CNTRY = source.CNTRY
FROM bronze.erp_loc_a101 AS target
INNER JOIN source
    ON target.CID = source.CID
WHERE source.rn = 1;

;WITH source AS (
    SELECT
        CID,
        CNTRY,
        ROW_NUMBER() OVER (
            PARTITION BY CID
            ORDER BY CID
        ) AS rn
    FROM staging.erp_loc_a101
    WHERE CID IS NOT NULL
      AND LTRIM(RTRIM(CID)) <> ''
)
INSERT INTO bronze.erp_loc_a101 (
    CID,
    CNTRY
)
SELECT
    source.CID,
    source.CNTRY
FROM source
WHERE source.rn = 1
  AND NOT EXISTS (
      SELECT 1
      FROM bronze.erp_loc_a101 AS target
      WHERE target.CID = source.CID
  );

;WITH source AS (
    SELECT
        ID,
        CAT,
        SUBCAT,
        MAINTENANCE,
        ROW_NUMBER() OVER (
            PARTITION BY ID
            ORDER BY ID
        ) AS rn
    FROM staging.erp_px_cat_g1v2
    WHERE ID IS NOT NULL
      AND LTRIM(RTRIM(ID)) <> ''
)
UPDATE target
SET
    target.CAT = source.CAT,
    target.SUBCAT = source.SUBCAT,
    target.MAINTENANCE = source.MAINTENANCE
FROM bronze.erp_px_cat_g1v2 AS target
INNER JOIN source
    ON target.ID = source.ID
WHERE source.rn = 1;

;WITH source AS (
    SELECT
        ID,
        CAT,
        SUBCAT,
        MAINTENANCE,
        ROW_NUMBER() OVER (
            PARTITION BY ID
            ORDER BY ID
        ) AS rn
    FROM staging.erp_px_cat_g1v2
    WHERE ID IS NOT NULL
      AND LTRIM(RTRIM(ID)) <> ''
)
INSERT INTO bronze.erp_px_cat_g1v2 (
    ID,
    CAT,
    SUBCAT,
    MAINTENANCE
)
SELECT
    source.ID,
    source.CAT,
    source.SUBCAT,
    source.MAINTENANCE
FROM source
WHERE source.rn = 1
  AND NOT EXISTS (
      SELECT 1
      FROM bronze.erp_px_cat_g1v2 AS target
      WHERE target.ID = source.ID
  );
GO

PRINT 'Practice SQL bronze load completed.';
GO
