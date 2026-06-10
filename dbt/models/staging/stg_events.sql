-- Sample staging model: generates mock event data
-- This uses ClickHouse's built-in functions to create test data

SELECT
    number + 1 AS event_id,
    arrayElement(
        ['page_view', 'click', 'purchase', 'signup', 'logout'],
        (number % 5) + 1
    ) AS event_type,
    arrayElement(
        ['user_001', 'user_002', 'user_003', 'user_004', 'user_005'],
        (number % 5) + 1
    ) AS user_id,
    toDateTime('2024-01-01 00:00:00') + toIntervalHour(number) AS event_timestamp
FROM numbers(100)
