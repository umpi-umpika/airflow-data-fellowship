-- Mart model: aggregate events per user
-- Demonstrates a ref() to the staging model

SELECT
    user_id,
    count(*) AS total_events,
    countIf(event_type = 'page_view') AS page_views,
    countIf(event_type = 'click') AS clicks,
    countIf(event_type = 'purchase') AS purchases,
    countIf(event_type = 'signup') AS signups,
    min(event_timestamp) AS first_event_at,
    max(event_timestamp) AS last_event_at
FROM {{ ref('stg_events') }}
GROUP BY user_id
