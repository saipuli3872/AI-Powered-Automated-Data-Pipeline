{{ config(materialized='incremental') }}

{{ automate_dv.hub(
    src_pk="EMAIL_HK",
    src_nk="EMAIL",
    src_ldts="LOAD_DATETIME",
    src_source="RECORD_SOURCE",
    source_model=ref('stg_email')
) }}