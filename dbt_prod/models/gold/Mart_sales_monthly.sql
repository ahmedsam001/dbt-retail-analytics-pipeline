WITH sales AS (
    SELECT * FROM {{ ref('silver_sales') }}
),

dates AS (
    SELECT * FROM {{ ref('silver_data') }}
),

stores AS (
    SELECT * FROM {{ ref('silver_store') }}
),

returns AS (
    SELECT * FROM {{ ref('silver_returns') }}
),

monthly_returns AS (
    SELECT
        d.year,
        d.month,
        r.store_sk,
        COUNT(*) AS return_transactions,
        SUM(COALESCE(r.returned_qty, 0)) AS total_returned_qty,
        SUM(COALESCE(r.refund_amount, 0)) AS total_refund_amount
    FROM returns r
    INNER JOIN dates d ON r.date_sk = d.date_sk
    GROUP BY d.year, d.month, r.store_sk
),

aggregated AS (
    SELECT
        d.year,
        d.quarter,
        d.month,
        d.month_name,
        d.month_year_label,

        s.store_sk,
        s.store_name,
        s.region,
        s.store_size_category,

        COUNT(DISTINCT d.date) AS selling_days,
        COUNT(sal.sales_id) AS total_transactions,
        SUM(sal.quantity) AS total_units_sold,


        SUM(sal.gross_amount) AS gross_revenue,
        SUM(sal.discount_amount) AS total_discounts,
        SUM(sal.net_amount) AS net_revenue,


        ROUND(AVG(sal.net_amount), 2) AS avg_transaction_value,
        ROUND(
            SUM(sal.net_amount) / NULLIF(COUNT(DISTINCT d.date), 0),
        2) AS avg_daily_revenue,
        ROUND(AVG(sal.discount_pct), 2) AS avg_discount_pct,

        SUM(CASE WHEN sal.has_promotion THEN 1 ELSE 0 END) AS promoted_transactions,

        ROUND(
            100.0 * SUM(CASE WHEN sal.has_promotion THEN 1 ELSE 0 END)
            / NULLIF(COUNT(sal.sales_id), 0),
        2) AS promo_attach_rate_pct,


        SUM(CASE WHEN UPPER(sal.payment_method) = 'CASH' THEN sal.net_amount ELSE 0 END) AS cash_revenue,
        SUM(CASE WHEN UPPER(sal.payment_method) = 'CREDIT CARD' THEN sal.net_amount ELSE 0 END) AS credit_card_revenue,
        SUM(CASE WHEN UPPER(sal.payment_method) = 'DIGITAL WALLET' THEN sal.net_amount ELSE 0 END) AS digital_wallet_revenue,

        COALESCE(mr.return_transactions, 0) AS return_transactions,
        COALESCE(mr.total_returned_qty, 0) AS total_returned_qty,
        COALESCE(mr.total_refund_amount, 0) AS total_refund_amount,


        SUM(sal.net_amount) - COALESCE(mr.total_refund_amount, 0) AS net_revenue_after_returns,


        ROUND(
            100.0 * COALESCE(mr.return_transactions, 0)
            / NULLIF(COUNT(sal.sales_id), 0),
        2) AS return_rate_pct

    FROM sales sal
    INNER JOIN dates d  ON sal.date_sk  = d.date_sk
    INNER JOIN stores s ON sal.store_sk = s.store_sk
    LEFT JOIN monthly_returns mr
        ON d.year = mr.year
       AND d.month = mr.month
       AND sal.store_sk = mr.store_sk

    GROUP BY
        d.year, d.quarter, d.month, d.month_name, d.month_year_label,
        s.store_sk, s.store_name, s.region, s.store_size_category,
        mr.return_transactions, mr.total_returned_qty, mr.total_refund_amount
)

SELECT *
FROM aggregated
ORDER BY year, month, store_sk