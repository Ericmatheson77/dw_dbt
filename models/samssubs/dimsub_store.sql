{{ config(
    materialized = 'table',
    schema = 'dw_subs'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['storeid']) }} as Storekey,
storeid,
address,
city,
state,
zip
FROM {{ source('orders_landing', 'store') }}