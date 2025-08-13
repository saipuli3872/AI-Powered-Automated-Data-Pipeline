{{ config(materialized='incremental') }}

{{ automate_dv.sat(
    src_pk="CUSTOMER_HK",
    src_hashdiff="CUSTOMER_DETAILS_HD",
    src_payload=["EMAIL", "PHONE", "ADDRESS", "DATE_OF_BIRTH", "REGISTRATION_DATE", "CREDIT_SCORE", "ANNUAL_INCOME", "IS_ACTIVE", "UPDATED_AT"],
    src_eff="EFFECTIVE_FROM",
    src_ldts="LOAD_DATETIME",
    src_source="RECORD_SOURCE",
    source_model=ref('stg_customer')
) }}