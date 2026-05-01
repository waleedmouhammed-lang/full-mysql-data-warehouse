{{ config(alias='erp_loc_a101') }}

select
    convert(varchar(20), replace(nullif(trim(CID), ''), '-', '')) as CID,
    convert(varchar(50), case
        when trim(CNTRY) in ('US', 'USA') then 'United States'
        when trim(CNTRY) = 'DE' then 'Denmark'
        else trim(CNTRY)
    end) as CNTRY,
    sysutcdatetime() as meta_created_at,
    sysutcdatetime() as meta_updated_at
from {{ source('bronze', 'erp_loc_a101') }}
where nullif(trim(CID), '') is not null
