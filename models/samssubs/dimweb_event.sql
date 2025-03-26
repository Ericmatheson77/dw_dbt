{{ config(
    materialized = 'table',
    schema = 'dw_subs'
    )
}}

SELECT
{{ dbt_utils.generate_surrogate_key(['event_name']) }} as EventKey,
event_Name
FROM {{ source('web_landing', 'web_traffic_events') }}