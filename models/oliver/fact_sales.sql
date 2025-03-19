{{ config(
    materialized = 'table',
    schema = 'dw_oliver'
) }}

SELECT
    p.policy_key,
    cu.customer_key,
    a.agent_key,
    d.date_key,
    c.ClaimAmount
FROM {{ source('oliver_landing', 'orderline') }} ol
INNER JOIN {{ source('oliver_landing', 'orders') }} o ON c.PolicyID = pd.PolicyID
INNER JOIN {{ ref('oliver_dim_date') }} p ON pd.PolicyID = p.policyid 
INNER JOIN {{ ref('oliver_dim_customer') }} cu ON pd.CustomerID = cu.customerid 
INNER JOIN {{ ref('oliver_dim_employee') }} a ON pd.AgentID = a.agentid 
INNER JOIN {{ ref('oliver_dim_product') }} d ON d.date_day = c.ClaimDate
INNER JOIN {{ ref('oliver_dim_store') }} d ON d.date_day = c.ClaimDate

#not finished