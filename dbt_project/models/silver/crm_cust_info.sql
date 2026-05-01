{{ config(alias='crm_cust_info') }}

select
    try_convert(int, nullif(trim(cst_id), '')) as cst_id,
    convert(varchar(20), nullif(trim(cst_key), '')) as cst_key,
    convert(varchar(100), nullif(trim(cst_firstname), '')) as cst_firstname,
    convert(varchar(100), nullif(trim(cst_lastname), '')) as cst_lastname,
    convert(varchar(10), case
        when upper(trim(cst_marital_status)) = 'S' then 'Single'
        when upper(trim(cst_marital_status)) = 'M' then 'Married'
        else 'UnKnown'
    end) as cst_marital_status,
    convert(varchar(10), case
        when upper(trim(cst_gndr)) = 'M' then 'Male'
        when upper(trim(cst_gndr)) = 'F' then 'Female'
        else 'UnKnown'
    end) as cst_gndr,
    try_convert(date, nullif(trim(cst_create_date), ''), 23) as cst_create_date,
    sysutcdatetime() as meta_created_at,
    sysutcdatetime() as meta_updated_at
from {{ source('bronze', 'crm_cust_info') }}
where try_convert(int, nullif(trim(cst_id), '')) is not null
  and nullif(trim(cst_key), '') is not null
