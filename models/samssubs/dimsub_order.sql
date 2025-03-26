{{ config(
    materialized = 'table',
    schema = 'dw_subs'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['ordernumber']) }} as Orderkey,
ordernumber,
ordermethod
FROM {{ source('orders_landing', '"ORDER"') }}