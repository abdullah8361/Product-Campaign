WITH interaction_counts AS (
    SELECT
        campaign_id,
        SUM(CASE WHEN interaction_type = 'click' THEN 1 ELSE 0 END) AS total_clicks,
        SUM(CASE WHEN interaction_type = 'conversion' THEN 1 ELSE 0 END) AS total_conversions
    FROM
        joined_campaign_interaction_table
    GROUP BY
        campaign_id
),
conversion_rates AS (
    SELECT
        campaign_id,
        total_conversions,
        total_clicks,
        CASE WHEN total_clicks > 0 THEN (total_conversions::FLOAT / total_clicks) * 100 ELSE 0 END AS conversion_rate
    FROM
        interaction_counts
)
SELECT
    campaign_id,
    total_conversions,
    total_clicks,
    conversion_rate
FROM
    conversion_rates;