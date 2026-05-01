{{ config(alias='fact_sales') }}

select
    row_number() over (order by sd.sls_ord_num, sd.sls_prd_key) as sales_key,
    dc.customer_key,
    dp.product_key,
    sd.sls_ord_num as order_number,
    sd.sls_order_dt as order_date,
    sd.sls_ship_dt as shipping_date,
    sd.sls_due_dt as due_date,
    sd.sls_sales as sales_amount,
    sd.sls_quantity as quantity,
    sd.sls_price as price,
    sysutcdatetime() as meta_created_at
from {{ ref('crm_sales_details') }} as sd
left join {{ ref('dim_products') }} as dp
    on sd.sls_prd_key = dp.product_number
   and sd.sls_order_dt >= dp.start_date
   and (sd.sls_order_dt <= dp.end_date or dp.end_date is null)
left join {{ ref('dim_customers') }} as dc
    on sd.sls_cust_id = dc.customer_id
