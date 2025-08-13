{{ config(materialized='incremental') }}

{{ automate_dv.hub(
    src_pk="PHONE_HK",
    src_nk="PHONE",
    src_ldts="LOAD_DATETIME",
    src_source="RECORD_SOURCE",
    source_model=ref('stg_phone')
) }}