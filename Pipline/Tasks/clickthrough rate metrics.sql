WITH InteractionCounts AS (
    SELECT 
        campaign_id,
        SUM(CASE WHEN interaction_type = 'click' THEN 1 ELSE 0 END) AS total_clicks,
        SUM(CASE WHEN interaction_type = 'impression' THEN 1 ELSE 0 END) AS total_impressions
    FROM 
        joined_campaign_interaction_table
    GROUP BY 
        campaign_id
),
CTRCalculation AS (
    SELECT 
        campaign_id,
        total_clicks,
        total_impressions,
        CASE 
            WHEN total_impressions = 0 THEN 0
            ELSE (total_clicks::FLOAT / total_impressions) * 100
        END AS ctr
    FROM 
        InteractionCounts
)
SELECT 
    campaign_id,
    total_clicks,
    total_impressions,
    ctr
FROM 
    CTRCalculation;