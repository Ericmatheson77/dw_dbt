{{ config(
    materialized = 'table',
    schema = 'dw_subs'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['p.productid']) }} as productKey,
p.productid,
producttype,
productname,
productcalories,
length,
breadtype
FROM {{ source('orders_landing', 'product') }} p
JOIN {{ source('orders_landing', 'sandwich')}} s on s.productid = p.productid