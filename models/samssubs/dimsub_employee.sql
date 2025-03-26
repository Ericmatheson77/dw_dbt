{{ config(
    materialized = 'table',
    schema = 'dw_subs'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['employeeid']) }} as EmployeeKey,
EmployeeID,
employeefname,
employeelname,
employeebday
FROM {{ source('orders_landing', 'employee') }}