{{ config(
    materialized='table',
    indexes=[
      {'columns': ['customer_id'], 'unique': True},
      {'columns': ['customer_segment']},
      {'columns': ['risk_level']},
      {'columns': ['ltv_category']}
    ]
) }}

WITH customer_base AS (
    SELECT * FROM {{ ref('silver_customers') }}
),

transaction_metrics AS (
    SELECT 
        customer_id,
        COUNT(*) as total_transactions_detailed,
        SUM(amount_usd) as total_revenue_usd,
        AVG(amount_usd) as avg_transaction_usd,
        STDDEV(amount_usd) as transaction_variance,
        
        -- Frequency metrics
        COUNT(DISTINCT transaction_date) as transaction_days,
        MAX(transaction_date) as last_transaction_date,
        MIN(transaction_date) as first_transaction_date,
        
        -- Risk metrics
        COUNT(CASE WHEN is_high_risk THEN 1 END) as high_risk_transactions,
        AVG(fraud_score) as avg_fraud_score,
        
        -- Behavioral patterns
        COUNT(CASE WHEN time_of_day = 'Night' THEN 1 END) as night_transactions,
        COUNT(CASE WHEN day_type = 'Weekend' THEN 1 END) as weekend_transactions,
        COUNT(CASE WHEN amount_category = 'High' THEN 1 END) as high_value_transactions,
        
        -- Payment preferences
        MODE() WITHIN GROUP (ORDER BY payment_method) as preferred_payment_method,
        COUNT(DISTINCT payment_method) as payment_methods_used,
        
        -- Category preferences  
        MODE() WITHIN GROUP (ORDER BY category) as preferred_category,
        COUNT(DISTINCT category) as categories_purchased
        
    FROM {{ ref('silver_transactions') }}
    WHERE status = 'completed'
    GROUP BY customer_id
),

customer_lifetime_value AS (
    SELECT 
        customer_id,
        total_revenue_usd,
        transaction_days,
        CASE 
            WHEN transaction_days > 0 
            THEN total_revenue_usd / transaction_days 
            ELSE 0 
        END as daily_value,
        
        -- LTV calculation (simplified)
        CASE 
            WHEN transaction_days >= 30 
            THEN total_revenue_usd * (365.0 / transaction_days) * 0.8  -- 80% retention assumption
            ELSE total_revenue_usd * 2  -- New customer projection
        END as estimated_ltv
        
    FROM transaction_metrics
),

risk_scoring AS (
    SELECT 
        customer_id,
        avg_fraud_score,
        high_risk_transactions,
        total_transactions_detailed,
        night_transactions,
        
        -- Composite risk score
        (
            (avg_fraud_score * 0.4) +
            (CASE WHEN total_transactions_detailed > 0 
             THEN (high_risk_transactions::float / total_transactions_detailed) * 0.3 
             ELSE 0 END) +
            (CASE WHEN total_transactions_detailed > 0 
             THEN (night_transactions::float / total_transactions_detailed) * 0.2 
             ELSE 0 END) +
            (CASE WHEN transaction_variance > 1000 THEN 0.1 ELSE 0 END)
        ) as composite_risk_score
        
    FROM transaction_metrics
)

SELECT 
    cb.customer_id,
    cb.email,
    cb.country,
    cb.device_type,
    cb.created_at,
    cb.last_activity_at,
    cb.customer_status,
    cb.customer_segment,
    
    -- Enhanced transaction metrics
    tm.total_transactions_detailed,
    tm.total_revenue_usd,
    tm.avg_transaction_usd,
    tm.transaction_days,
    tm.first_transaction_date,
    tm.last_transaction_date,
    
    -- LTV metrics
    clv.estimated_ltv,
    CASE 
        WHEN clv.estimated_ltv >= 5000 THEN 'High LTV'
        WHEN clv.estimated_ltv >= 1000 THEN 'Medium LTV'
        ELSE 'Low LTV'
    END as ltv_category,
    
    -- Risk assessment
    rs.composite_risk_score,
    CASE 
        WHEN rs.composite_risk_score >= 0.7 THEN 'High Risk'
        WHEN rs.composite_risk_score >= 0.4 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END as risk_level,
    
    -- Behavioral insights
    tm.preferred_payment_method,
    tm.payment_methods_used,
    tm.preferred_category,
    tm.categories_purchased,
    tm.high_value_transactions,
    tm.weekend_transactions,
    tm.night_transactions,
    
    -- Engagement metrics
    CASE 
        WHEN tm.transaction_days >= 30 AND cb.last_activity_at >= CURRENT_DATE - INTERVAL '7 days' 
        THEN 'Highly Engaged'
        WHEN tm.transaction_days >= 10 AND cb.last_activity_at >= CURRENT_DATE - INTERVAL '30 days'
        THEN 'Moderately Engaged'
        ELSE 'Low Engagement'
    END as engagement_level,
    
    -- Recency, Frequency, Monetary (RFM) components
    CASE 
        WHEN tm.last_transaction_date >= CURRENT_DATE - INTERVAL '30 days' THEN 5
        WHEN tm.last_transaction_date >= CURRENT_DATE - INTERVAL '90 days' THEN 4
        WHEN tm.last_transaction_date >= CURRENT_DATE - INTERVAL '180 days' THEN 3
        WHEN tm.last_transaction_date >= CURRENT_DATE - INTERVAL '365 days' THEN 2
        ELSE 1
    END as recency_score,
    
    CASE 
        WHEN tm.total_transactions_detailed >= 50 THEN 5
        WHEN tm.total_transactions_detailed >= 20 THEN 4
        WHEN tm.total_transactions_detailed >= 10 THEN 3
        WHEN tm.total_transactions_detailed >= 5 THEN 2
        ELSE 1
    END as frequency_score,
    
    CASE 
        WHEN tm.total_revenue_usd >= 5000 THEN 5
        WHEN tm.total_revenue_usd >= 1000 THEN 4
        WHEN tm.total_revenue_usd >= 500 THEN 3
        WHEN tm.total_revenue_usd >= 100 THEN 2
        ELSE 1
    END as monetary_score,
    
    CURRENT_TIMESTAMP as processed_at

FROM customer_base cb
LEFT JOIN transaction_metrics tm ON cb.customer_id = tm.customer_id
LEFT JOIN customer_lifetime_value clv ON cb.customer_id = clv.customer_id  
LEFT JOIN risk_scoring rs ON cb.customer_id = rs.customer_id

WHERE cb.total_transactions > 0  -- Only customers with transactions