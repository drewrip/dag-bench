{{
    config(
        materialized='view',
        tags=['staging', 'sap']
    )
}}

with source as (
    select * from {{ source('sap', 'BSID') }}
),

deduped as (
    select
        _id,
        _loaded_at,
        _source_system,
        _source_table,
        _row_hash,
        mandt,
        bukrs,
        kunnr,
        umsks,
        umskz,
        augdt,
        augbl,
        gjahr,
        belnr,
        buzei,
        budat,
        bldat,
        waers,
        dmbtr,
        wrbtr,
        sgtxt,
        zuonr,
        shkzg
    from (
        select *,
            row_number() over (partition by _id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where _id is not null
    )
    where _rn = 1
)

select * from deduped
