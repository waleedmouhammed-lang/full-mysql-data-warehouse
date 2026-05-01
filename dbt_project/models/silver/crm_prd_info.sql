{{ config(alias='crm_prd_info') }}

with product_source as (
    select
        try_convert(int, nullif(trim(prd_id), '')) as prd_id,
        convert(varchar(50), replace(substring(trim(prd_key), 1, 5), '-', '_')) as prd_category,
        convert(varchar(50), substring(trim(prd_key), 7, 255)) as prd_key,
        convert(varchar(255), nullif(trim(prd_nm), '')) as prd_nm,
        try_convert(decimal(19, 4), coalesce(nullif(trim(prd_cost), ''), '0')) as prd_cost,
        convert(varchar(15), case upper(trim(prd_line))
            when 'M' then 'Mountain'
            when 'S' then 'Other Sales'
            when 'R' then 'Road'
            when 'T' then 'Touring'
            else 'N/A'
        end) as prd_line,
        try_convert(date, nullif(trim(prd_start_dt), ''), 23) as prd_start_dt,
        try_convert(date, nullif(trim(prd_end_dt), ''), 23) as prd_end_dt,
        trim(prd_key) as raw_prd_key
    from {{ source('bronze', 'crm_prd_info') }}
    where try_convert(int, nullif(trim(prd_id), '')) is not null
      and nullif(trim(prd_key), '') is not null
),
product_history as (
    select
        *,
        lead(prd_start_dt) over (partition by raw_prd_key order by prd_start_dt) as next_start_dt
    from product_source
)
select
    prd_id,
    prd_category,
    prd_key,
    prd_nm,
    prd_cost,
    prd_line,
    prd_start_dt,
    case
        when prd_end_dt < prd_start_dt then dateadd(day, -1, next_start_dt)
        else prd_end_dt
    end as prd_end_dt,
    sysutcdatetime() as meta_created_at,
    sysutcdatetime() as meta_updated_at
from product_history
