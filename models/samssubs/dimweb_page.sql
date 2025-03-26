{{ config(
    materialized = 'table',
    schema = 'dw_subs'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['page_URL']) }} as PageKey,
page_URL
FROM {{ source('web_landing', 'web_traffic_events') }}