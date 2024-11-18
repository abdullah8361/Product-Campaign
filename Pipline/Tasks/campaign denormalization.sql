WITH CustomerInteractions AS (
    SELECT 
        ci.campaign_id,
        ci.interaction_type,
        ci.interaction_timestamp,
        ci.source,
        ci.device_id
    FROM 
        CustomerInteraction ci
),
CampaignDetails AS (
    SELECT 
        c.campaign_id,
        c.channel,
        c.campaign_start,
        c.campaign_end,
        c.budget,
        c.target_audience,
        c.campaign_type
    FROM 
        Campaign c
),
Conversions AS (
    SELECT 
        o.order_id,
        o.customer_id,
        o.product_id,
        o.campaign_id,
        o.conversion_timestamp,
        o.order_amount
    FROM 
        Orders o
)
SELECT 
    ci.customer_id,
    ci.interaction_type,
    ci.interaction_timestamp,
    ci.device_type,
    ci.source,
    cd.channel,
    cd.campaign_start,
    cd.campaign_end,
    cd.budget,
    cd.target_audience,
    cd.campaign_type,
    co.order_id,
    co.product_id,
    co.conversion_timestamp,
    co.order_amount
FROM 
    CustomerInteractions ci
JOIN 
    CampaignDetails cd ON ci.campaign_id = cd.campaign_id
LEFT JOIN 
    Conversions co ON ci.customer_id = co.customer_id AND ci.campaign_id = co.campaign_id;