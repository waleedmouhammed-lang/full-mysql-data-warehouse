{{ config(alias='dim_customers') }}

with customer_source as (
    select
        ci.cst_id as customer_id,
        ci.cst_key as customer_number,
        ci.cst_firstname as first_name,
        ci.cst_lastname as last_name,
        concat(ci.cst_firstname, ' ', ci.cst_lastname) as full_name,
        coalesce(la.CNTRY, 'n/a') as country,
        coalesce(ci.cst_marital_status, 'n/a') as marital_status,
        case
            when upper(trim(ci.cst_gndr)) <> 'UNKNOWN' then ci.cst_gndr
            else coalesce(ca.GEN, 'n/a')
        end as gender,
        ca.BDATE as birthdate,
        ci.cst_create_date as create_date,
        case
            when ca.BDATE is null then null
            else
                datediff(year, ca.BDATE, cast(getdate() as date))
                - case
                    when dateadd(year, datediff(year, ca.BDATE, cast(getdate() as date)), ca.BDATE) > cast(getdate() as date)
                    then 1 else 0
                  end
        end as age
    from {{ ref('crm_cust_info') }} as ci
    left join {{ ref('erp_cust_az12') }} as ca on ci.cst_key = ca.CID
    left join {{ ref('erp_loc_a101') }} as la on ci.cst_key = la.CID
)
select
    row_number() over (order by customer_id) as customer_key,
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
    case
        when age < 20 then 'Under 20'
        when age between 20 and 29 then '20-29'
        when age between 30 and 39 then '30-39'
        when age between 40 and 49 then '40-49'
        when age >= 50 then '50+'
        else 'Unknown'
    end as age_group,
    sysutcdatetime() as meta_created_at
from customer_source
