{{ config(
    materialized = 'table',
    schema = 'dw_subs'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['customerid']) }} as Customerkey,
CustomerID,
customerfname,
customerlname,
customerbday,
customerphone
FROM {{ source('orders_landing', 'customer') }}