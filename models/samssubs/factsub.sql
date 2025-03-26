{{ config(
    materialized = 'table',
    schema = 'dw_subs'
) }}

SELECT
    dc.customerkey,
    dp.productKey,
    dimo.orderKey,
    ds.storekey,
    de.employeekey,
    dd.dateKey,
    od.ORDERLINEQTY,
    p.productcost,
    o.ordertotal,
    od.PointsEarned
FROM {{ source('orders_landing', 'orderdetails') }} od
INNER JOIN {{ source('orders_landing', '"ORDER"') }} o ON o.ordernumber = od.ordernumber
INNER JOIN {{ source('orders_landing', 'product') }} p ON p.productid = od.productid
INNER JOIN {{ source('orders_landing', 'employee') }} e ON e.employeeid = o.employeeid
INNER JOIN {{ source('orders_landing', 'store') }} s ON s.storeid = e.storeid

INNER JOIN {{ ref('dimsub_customer') }} dc ON dc.customerid = o.customerid
INNER JOIN {{ ref('dimsub_order') }} dimo ON dimo.ordernumber = o.ordernumber
INNER JOIN {{ ref('dimsub_employee') }} de ON o.EMPLOYEEID = de.EMPLOYEEkey
INNER JOIN {{ ref('dimsub_product') }} dp ON od.PRODUCTID = dp.PRODUCTID
INNER JOIN {{ ref('dimsub_store') }} ds ON ds.STOREID = s.STOREID
INNER JOIN {{ ref('dimsub_date') }} dd ON o.ORDERDATE = dd.date_day