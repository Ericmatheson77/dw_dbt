{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
) }}

SELECT
    c.customer_key,
    d.date_day,
    s.store_key,
    ol.product_key,
    e.employee_key,
    ol.QUANTITY,
    o.TOTAL_AMOUNT,
    ol.UNIT_PRICE
FROM {{ source('oliver_landing', 'orderline') }} ol
INNER JOIN {{ source('oliver_landing', 'orders') }} o ON o.ORDER_ID = ol.ORDER_ID
INNER JOIN {{ ref('oliver_dim_date') }} d ON o.ORDER_DATE = d.date_day
INNER JOIN {{ ref('oliver_dim_customer') }} c ON o.CUSTOMER_ID = c.CUSTOMER_ID
INNER JOIN {{ ref('oliver_dim_employee') }} e ON o.EMPLOYEE_ID = e.EMPLOYEE_ID
INNER JOIN {{ ref('oliver_dim_product') }} p ON ol.PRODUCT_ID = p.PRODUCT_ID
INNER JOIN {{ ref('oliver_dim_store') }} s ON s.STORE_ID = o.STORE_ID