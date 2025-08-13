{{ config(materialized='incremental') }}

{{ automate_dv.hub(
    src_pk="CUSTOMER_ID_HK",
    src_nk="CUSTOMER_ID",
    src_ldts="LOAD_DATETIME",
    src_source="RECORD_SOURCE",
    source_model=ref('stg_customer_id')
) }}