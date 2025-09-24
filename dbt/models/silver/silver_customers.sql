{{ config(
    materialized='table',
    indexes=[
      {'columns': ['customer_id'], 'unique': True},
      {'columns': ['email'], 'unique': True},
      {'columns': ['country']},
      {'columns': ['created_at']}
    ]
) }}

WITH customer_events AS (
    SELECT 
        customer_id,
        email,
        country,
        device_type,
        MIN(timestamp::timestamp) as first_seen,
        MAX(timestamp::timestamp) as last_seen,
        COUNT(*) as total_events,
        COUNT(DISTINCT DATE(timestamp::timestamp)) as active_days
    FROM {{ ref('bronze_customer_events') }}
    WHERE customer_id IS NOT NULL
      AND email IS NOT NULL
      AND email LIKE '%@%'
    GROUP BY customer_id, email, country, device_type
),

customer_transactions AS (
    SELECT 
        customer_id,
        COUNT(*) as total_transactions,
        SUM(amount) as total_spent,
        AVG(amount) as avg_transaction_value,
        MAX(amount) as max_transaction_value,
        COUNT(CASE WHEN status = 'completed' THEN 1 END) as successful_transactions,
        COUNT(CASE WHEN fraud_score > {{ var('fraud_score_threshold') }} THEN 1 END) as high_risk_transactions,
        MAX(timestamp::timestamp) as last_transaction_date
    FROM {{ ref('bronze_transaction_events') }}
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
),

customer_products AS (
    SELECT 
        customer_id,
        COUNT(DISTINCT product_id) as unique_products_viewed,
        COUNT(CASE WHEN action = 'purchase' THEN 1 END) as products_purchased,
        AVG(CASE WHEN rating IS NOT NULL THEN rating END) as avg_rating_given
    FROM {{ ref('bronze_product_events') }}
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
)

SELECT 
    ce.customer_id,
    ce.email,
    ce.country,
    ce.device_type,
    ce.first_seen as created_at,
    ce.last_seen as last_activity_at,
    ce.total_events,
    ce.active_days,
    
    -- Transaction metrics
    COALESCE(ct.total_transactions, 0) as total_transactions,
    COALESCE(ct.total_spent, 0) as total_spent,
    COALESCE(ct.avg_transaction_value, 0) as avg_transaction_value,
    COALESCE(ct.max_transaction_value, 0) as max_transaction_value,
    COALESCE(ct.successful_transactions, 0) as successful_transactions,
    COALESCE(ct.high_risk_transactions, 0) as high_risk_transactions,
    ct.last_transaction_date,
    
    -- Product metrics
    COALESCE(cp.unique_products_viewed, 0) as unique_products_viewed,
    COALESCE(cp.products_purchased, 0) as products_purchased,
    cp.avg_rating_given,
    
    -- Calculated fields
    CASE 
        WHEN ct.total_transactions > 0 
        THEN ROUND(ct.successful_transactions::numeric / ct.total_transactions * 100, 2)
        ELSE 0 
    END as success_rate_pct,
    
    CASE 
        WHEN ce.last_seen >= CURRENT_DATE - INTERVAL '{{ var("active_customer_days") }} days' 
        THEN 'Active'
        ELSE 'Inactive'
    END as customer_status,
    
    CASE 
        WHEN ct.total_spent >= {{ var('high_value_transaction_threshold') }} THEN 'High Value'
        WHEN ct.total_spent >= 100 THEN 'Medium Value'
        ELSE 'Low Value'
    END as customer_segment,
    
    CURRENT_TIMESTAMP as processed_at

FROM customer_events ce
LEFT JOIN customer_transactions ct ON ce.customer_id = ct.customer_id
LEFT JOIN customer_products cp ON ce.customer_id = cp.customer_id