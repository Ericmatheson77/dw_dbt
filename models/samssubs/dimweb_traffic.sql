{{ config(
    materialized = 'table',
    schema = 'dw_subs'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['traffic_source']) }} as TrafficKey,
traffic_source
FROM {{ source('web_landing', 'web_traffic_events') }}