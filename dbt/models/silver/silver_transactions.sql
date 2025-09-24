{{ config(
    materialized='table',
    indexes=[
      {'columns': ['transaction_id'], 'unique': True},
      {'columns': ['customer_id']},
      {'columns': ['transaction_date']},
      {'columns': ['status']},
      {'columns': ['is_high_risk']}
    ]
) }}

WITH cleaned_transactions AS (
    SELECT 
        transaction_id,
        customer_id,
        amount,
        currency,
        status,
        payment_method,
        merchant_id,
        category,
        fraud_score,
        timestamp::timestamp as transaction_timestamp,
        DATE(timestamp::timestamp) as transaction_date,
        EXTRACT(HOUR FROM timestamp::timestamp) as transaction_hour,
        EXTRACT(DOW FROM timestamp::timestamp) as day_of_week,
        processed_at::timestamp as processed_at
    FROM {{ ref('bronze_transaction_events') }}
    WHERE transaction_id IS NOT NULL
      AND customer_id IS NOT NULL
      AND amount > 0
      AND amount < 100000  -- Remove outliers extremos
),

transaction_enriched AS (
    SELECT 
        *,
        -- Risk categorization
        CASE 
            WHEN fraud_score >= {{ var('fraud_score_threshold') }} THEN TRUE
            ELSE FALSE
        END as is_high_risk,
        
        -- Amount categorization
        CASE 
            WHEN amount >= {{ var('high_value_transaction_threshold') }} THEN 'High'
            WHEN amount >= 100 THEN 'Medium'
            ELSE 'Low'
        END as amount_category,
        
        -- Time categorization
        CASE 
            WHEN transaction_hour BETWEEN 6 AND 12 THEN 'Morning'
            WHEN transaction_hour BETWEEN 13 AND 18 THEN 'Afternoon'
            WHEN transaction_hour BETWEEN 19 AND 23 THEN 'Evening'
            ELSE 'Night'
        END as time_of_day,
        
        CASE 
            WHEN day_of_week IN (0, 6) THEN 'Weekend'
            ELSE 'Weekday'
        END as day_type,
        
        -- Conversion to USD (simplified)
        CASE 
            WHEN currency = 'EUR' THEN amount * 1.1
            WHEN currency = 'BRL' THEN amount * 0.2
            WHEN currency = 'GBP' THEN amount * 1.25
            ELSE amount
        END as amount_usd

    FROM cleaned_transactions
)

SELECT 
    transaction_id,
    customer_id,
    amount,
    amount_usd,
    currency,
    status,
    payment_method,
    merchant_id,
    category,
    fraud_score,
    is_high_risk,
    amount_category,
    transaction_timestamp,
    transaction_date,
    transaction_hour,
    day_of_week,
    time_of_day,
    day_type,
    processed_at
    
FROM transaction_enriched