{{ config(
    materialized = 'table',
    schema = 'dw_subs'
) }}

SELECT
    dateKey,
    PageKey,
    EventKey,
    TrafficKey,
    count(wt.event_Name) as activity

FROM {{ source('web_landing', 'web_traffic_events') }} wt

INNER JOIN {{ ref('dimweb_page') }} dp ON dp.page_url = wt.page_url
INNER JOIN {{ ref('dimweb_event') }} de ON wt.event_name = de.event_name
INNER JOIN {{ ref('dimweb_traffic') }} dt ON wt.traffic_source = dt.traffic_source
INNER JOIN {{ ref('dimsub_date') }} dd ON cast(wt.event_timestamp as DATE) = dd.date_day
GROUP BY datekey, PageKey, EventKey, TrafficKey