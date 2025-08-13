{{ config(materialized='view') }}

{{ automate_dv.stage(
    include_source_columns=true,
    source_model=source('customers'),
    derived_columns={}
) }}