{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
) }}

SELECT
    p.product_name,
    QUANTITY,
    TOTAL_AMOUNT,
    UNIT_PRICE,
    store_name,
    d.date_day,
    customer_key
FROM {{ ref('oliver_dim_product') }} p
INNER JOIN {{ ref('fact_sales') }} sf ON sf.product_key = p.product_key
INNER JOIN {{ ref('oliver_dim_store') }} s ON s.store_key = sf.store_key
INNER JOIN {{ ref('oliver_dim_date') }} d ON d.date_key = sf.date_day