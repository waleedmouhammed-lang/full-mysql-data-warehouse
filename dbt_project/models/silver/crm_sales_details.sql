{{ config(alias='crm_sales_details') }}

select
    convert(varchar(20), nullif(trim(sls_ord_num), '')) as sls_ord_num,
    convert(varchar(50), nullif(trim(sls_prd_key), '')) as sls_prd_key,
    try_convert(int, nullif(trim(sls_cust_id), '')) as sls_cust_id,
    try_convert(date, nullif(trim(sls_order_dt), ''), 112) as sls_order_dt,
    try_convert(date, nullif(trim(sls_ship_dt), ''), 112) as sls_ship_dt,
    try_convert(date, nullif(trim(sls_due_dt), ''), 112) as sls_due_dt,
    try_convert(decimal(19, 4), coalesce(nullif(trim(sls_sales), ''), '0')) as sls_sales,
    try_convert(int, coalesce(nullif(trim(sls_quantity), ''), '0')) as sls_quantity,
    try_convert(decimal(19, 4), coalesce(nullif(trim(sls_price), ''), '0')) as sls_price,
    sysutcdatetime() as meta_created_at,
    sysutcdatetime() as meta_updated_at
from {{ source('bronze', 'crm_sales_details') }}
where nullif(trim(sls_ord_num), '') is not null
  and nullif(trim(sls_prd_key), '') is not null
  and try_convert(int, nullif(trim(sls_cust_id), '')) is not null
