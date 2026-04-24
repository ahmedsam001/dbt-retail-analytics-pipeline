WITH sales AS (
    SELECT *
    FROM {{ ref('silver_sales') }}
),

dates AS (
    SELECT *
    FROM {{ ref('silver_data') }}
), 

stores AS (
    SELECT *
    FROM {{ ref('silver_store') }}
), 

returns AS (
    SELECT *
    FROM {{ ref('silver_returns') }}
),

daily_returns AS (
    SELECT
        date_sk,
        store_sk,
        COUNT(*) AS return_transactions,
        SUM(coalesce(returned_qty, 0)) AS total_returned_qty,
        SUM(coalesce(refund_amount, 0)) AS total_refund_amount   
    FROM returns
    GROUP BY date_sk, store_sk
),

aggregated AS (
    SELECT
        d.date,
        d.month_year_label,

        s.store_sk,
        s.store_name,
        s.city,
        s.state_province,
        s.region,
        s.store_size_category,
    
        COUNT(sal.sales_id) AS total_transactions,

        SUM(sal.gross_amount) AS total_gross_amount,
        SUM(sal.discount_amount) AS total_discount_amount,
        SUM(sal.net_amount) AS net_amount,

        ROUND(AVG(sal.net_amount), 2) AS avg_transaction_amount,
        ROUND(AVG(sal.discount_pct), 2) AS avg_discount_pct,

        SUM(CASE WHEN sal.has_promotion THEN 1 ELSE 0 END) AS promotion_transactions,

        ROUND(
            100.0 * SUM(CASE WHEN sal.has_promotion THEN 1 ELSE 0 END)
            / NULLIF(COUNT(sal.sales_id), 0),
        2) AS promo_attach_rate_pct,

        COALESCE(dr.return_transactions, 0) AS return_transactions,
        COALESCE(dr.total_returned_qty, 0) AS total_returned_qty,
        COALESCE(dr.total_refund_amount, 0) AS total_refund_amount,

        SUM(sal.net_amount) - COALESCE(dr.total_refund_amount, 0) AS net_revenue_after_returns

    FROM sales sal
    INNER JOIN dates d  ON sal.date_sk  = d.date_sk
    INNER JOIN stores s ON sal.store_sk = s.store_sk
    LEFT JOIN daily_returns dr
        ON sal.date_sk  = dr.date_sk
       AND sal.store_sk = dr.store_sk

    GROUP BY
        d.date,
        d.month_year_label,
        s.store_sk,
        s.store_name,
        s.city,
        s.state_province,
        s.region,
        s.store_size_category,
        dr.return_transactions,
        dr.total_returned_qty,
        dr.total_refund_amount
)

SELECT *
FROM aggregated
ORDER BY date, store_sk