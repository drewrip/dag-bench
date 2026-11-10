{{
    config(
        materialized='view',
        tags=['staging', 'sap']
    )
}}

with source as (
    select * from {{ source('sap', 'VBAK_QT') }}
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
        auart,
        vkorg,
        vtweg,
        spart,
        kunnr,
        erdat,
        angdt,
        bnddt,
        waerk,
        netwr,
        vbtyp,
        lifsk
    from (
        select *,
            row_number() over (partition by _id order by _loaded_at DESC NULLS LAST) as _rn
        from source
        where _id is not null
    )
    where _rn = 1
)

select * from deduped
