{{ config(
    materialized='table',
    indexes=[
      {'columns': ['metric_date']},
      {'columns': ['metric_type']},
      {'columns': ['dimension_value']}
    ]
) }}

WITH daily_transactions AS (
    SELECT 
        transaction_date,
        COUNT(*) as transaction_count,
        SUM(amount_usd) as total_revenue,
        AVG(amount_usd) as avg_transaction_value,
        COUNT(DISTINCT customer_id) as unique_customers,
        COUNT(CASE WHEN is_high_risk THEN 1 END) as high_risk_count,
        COUNT(CASE WHEN status = 'completed' THEN 1 END) as successful_count,
        COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed_count
    FROM {{ ref('silver_transactions') }}
    GROUP BY transaction_date
),

daily_customers AS (
    SELECT 
        DATE(created_at) as signup_date,
        COUNT(*) as new_customers,
        COUNT(CASE WHEN customer_segment = 'High Value' THEN 1 END) as high_value_signups
    FROM {{ ref('silver_customers') }}
    GROUP BY DATE(created_at)
),

-- Revenue metrics by date
revenue_metrics AS (
    SELECT 
        transaction_date as metric_date,
        'revenue' as metric_type,
        'daily' as dimension_type,
        'total' as dimension_value,
        total_revenue as metric_value,
        transaction_count as supporting_metric_1,
        avg_transaction_value as supporting_metric_2
    FROM daily_transactions
    
    UNION ALL
    
    SELECT 
        transaction_date as metric_date,
        'conversion' as metric_type,
        'daily' as dimension_type,
        'success_rate' as dimension_value,
        CASE WHEN transaction_count > 0 
             THEN (successful_count::float / transaction_count * 100)
             ELSE 0 END as metric_value,
        successful_count as supporting_metric_1,
        failed_count as supporting_metric_2
    FROM daily_transactions
),

-- Customer acquisition metrics
acquisition_metrics AS (
    SELECT 
        signup_date as metric_date,
        'acquisition' as metric_type,
        'daily' as dimension_type,
        'new_customers' as dimension_value,
        new_customers as metric_value,
        high_value_signups as supporting_metric_1,
        NULL as supporting_metric_2
    FROM daily_customers
),

-- Risk metrics
risk_metrics AS (
    SELECT 
        transaction_date as metric_date,
        'risk' as metric_type,
        'daily' as dimension_type,
        'high_risk_percentage' as dimension_value,
        CASE WHEN transaction_count > 0 
             THEN (high_risk_count::float / transaction_count * 100)
             ELSE 0 END as metric_value,
        high_risk_count as supporting_metric_1,
        transaction_count as supporting_metric_2
    FROM daily_transactions
),

-- Category performance
category_metrics AS (
    SELECT 
        transaction_date as metric_date,
        'category_performance' as metric_type,
        'daily' as dimension_type,
        category as dimension_value,
        SUM(amount_usd) as metric_value,
        COUNT(*) as supporting_metric_1,
        AVG(amount_usd) as supporting_metric_2
    FROM {{ ref('silver_transactions') }}
    WHERE status = 'completed'
    GROUP BY transaction_date, category
),

-- Payment method performance
payment_metrics AS (
    SELECT 
        transaction_date as metric_date,
        'payment_performance' as metric_type,
        'daily' as dimension_type,
        payment_method as dimension_value,
        COUNT(*) as metric_value,
        SUM(amount_usd) as supporting_metric_1,
        COUNT(CASE WHEN status = 'completed' THEN 1 END) as supporting_metric_2
    FROM {{ ref('silver_transactions') }}
    GROUP BY transaction_date, payment_method
),

-- Country performance
country_metrics AS (
    SELECT 
        DATE(t.transaction_date) as metric_date,
        'country_performance' as metric_type,
        'daily' as dimension_type,
        c.country as dimension_value,
        SUM(t.amount_usd) as metric_value,
        COUNT(DISTINCT t.customer_id) as supporting_metric_1,
        COUNT(*) as supporting_metric_2
    FROM {{ ref('silver_transactions') }} t
    JOIN {{ ref('silver_customers') }} c ON t.customer_id = c.customer_id
    WHERE t.status = 'completed'
    GROUP BY DATE(t.transaction_date), c.country
)

-- Union all metrics
SELECT 
    metric_date,
    metric_type,
    dimension_type,
    dimension_value,
    metric_value,
    supporting_metric_1,
    supporting_metric_2,
    CURRENT_TIMESTAMP as processed_at
FROM revenue_metrics

UNION ALL

SELECT 
    metric_date,
    metric_type,
    dimension_type,
    dimension_value,
    metric_value,
    supporting_metric_1,
    supporting_metric_2,
    CURRENT_TIMESTAMP as processed_at
FROM acquisition_metrics

UNION ALL

SELECT 
    metric_date,
    metric_type,
    dimension_type,
    dimension_value,
    metric_value,
    supporting_metric_1,
    supporting_metric_2,
    CURRENT_TIMESTAMP as processed_at
FROM risk_metrics

UNION ALL

SELECT 
    metric_date,
    metric_type,
    dimension_type,
    dimension_value,
    metric_value,
    supporting_metric_1,
    supporting_metric_2,
    CURRENT_TIMESTAMP as processed_at
FROM category_metrics

UNION ALL

SELECT 
    metric_date,
    metric_type,
    dimension_type,
    dimension_value,
    metric_value,
    supporting_metric_1,
    supporting_metric_2,
    CURRENT_TIMESTAMP as processed_at
FROM payment_metrics

UNION ALL

SELECT 
    metric_date,
    metric_type,
    dimension_type,
    dimension_value,
    metric_value,
    supporting_metric_1,
    supporting_metric_2,
    CURRENT_TIMESTAMP as processed_at
FROM country_metrics