{{
    config(
        materialized='view',
        tags=['staging', 'sap']
    )
}}

with source as (
    select * from {{ source('sap', 'LIPS') }}
),

deduped as (
    select
        _id,
        _loaded_at,
        _source_system,
        _source_table,
        _row_hash,
        mandt,
        vbeln,
        posnr,
        matnr,
        werks,
        lgort,
        charg,
        lfimg,
        meins,
        vgbel,
        vgpos,
        brgew,
        ntgew,
        gewei,
        volum,
        voleh,
        arktx
    from (
        select *,
            row_number() over (partition by _id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where _id is not null
    )
    where _rn = 1
)

select * from deduped
