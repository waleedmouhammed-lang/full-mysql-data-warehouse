{{ config(alias='dim_products') }}

select
    row_number() over (order by pn.prd_id, pn.prd_start_dt) as product_key,
    pn.prd_id as product_id,
    pn.prd_key as product_number,
    pn.prd_nm as product_name,
    pc.ID as category_id,
    pc.CAT as category_name,
    pc.SUBCAT as subcategory_name,
    case when pc.MAINTENANCE = 1 then 'Yes' else 'No' end as maintenance_flag,
    pn.prd_cost as product_cost,
    pn.prd_line as product_line,
    pn.prd_start_dt as start_date,
    pn.prd_end_dt as end_date,
    case when pn.prd_end_dt is null then convert(bit, 1) else convert(bit, 0) end as is_current,
    sysutcdatetime() as meta_created_at
from {{ ref('crm_prd_info') }} as pn
left join {{ ref('erp_px_cat_g1v2') }} as pc
    on pn.prd_category = pc.ID
