{{ config(alias='erp_cust_az12') }}

select
    convert(varchar(20), case
        when left(trim(CID), 3) = 'NAS' then substring(trim(CID), 4, 255)
        else trim(CID)
    end) as CID,
    try_convert(date, nullif(trim(BDATE), ''), 23) as BDATE,
    convert(varchar(10), nullif(trim(GEN), '')) as GEN,
    sysutcdatetime() as meta_created_at,
    sysutcdatetime() as meta_updated_at
from {{ source('bronze', 'erp_cust_az12') }}
where nullif(trim(CID), '') is not null
