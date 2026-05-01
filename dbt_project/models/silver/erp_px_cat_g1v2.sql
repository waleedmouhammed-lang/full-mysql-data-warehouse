{{ config(alias='erp_px_cat_g1v2') }}

select
    convert(varchar(20), nullif(trim(ID), '')) as ID,
    convert(varchar(50), nullif(trim(CAT), '')) as CAT,
    convert(varchar(50), nullif(trim(SUBCAT), '')) as SUBCAT,
    convert(bit, case
        when trim(MAINTENANCE) = 'Yes' then 1
        when trim(MAINTENANCE) = 'No' then 0
        else null
    end) as MAINTENANCE,
    sysutcdatetime() as meta_created_at,
    sysutcdatetime() as meta_updated_at
from {{ source('bronze', 'erp_px_cat_g1v2') }}
where nullif(trim(ID), '') is not null
