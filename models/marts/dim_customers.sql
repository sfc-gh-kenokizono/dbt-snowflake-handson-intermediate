WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

order_items AS (
    SELECT * FROM {{ ref('int_order_items_products') }}
),

order_summary AS (
    SELECT
        o.customer_id,
        COUNT(DISTINCT o.order_id) AS total_orders,
        COUNT(oi.item_id) AS total_items,
        SUM(oi.unit_price) AS total_revenue,
        SUM(oi.gross_profit) AS total_profit,
        AVG(oi.unit_price) AS avg_item_price,
        MIN(o.order_date) AS first_order_date,
        MAX(o.order_date) AS last_order_date,
        COUNT(DISTINCT o.store_id) AS stores_visited
    FROM orders o
    LEFT JOIN order_items oi ON o.order_id = oi.order_id
    GROUP BY o.customer_id
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['c.customer_id']) }} AS customer_sk,
    c.customer_id,
    c.customer_name,
    COALESCE(os.total_orders, 0) AS total_orders,
    COALESCE(os.total_items, 0) AS total_items,
    COALESCE(os.total_revenue, 0) AS total_revenue,
    COALESCE(os.total_profit, 0) AS total_profit,
    COALESCE(os.avg_item_price, 0) AS avg_item_price,
    CASE
        WHEN os.total_revenue >= 500 THEN 'Gold'
        WHEN os.total_revenue >= 200 THEN 'Silver'
        ELSE 'Bronze'
    END AS customer_tier,
    os.first_order_date,
    os.last_order_date,
    DATEDIFF('day', os.first_order_date, os.last_order_date) AS customer_lifetime_days,
    COALESCE(os.stores_visited, 0) AS stores_visited,
    CURRENT_TIMESTAMP() AS _created_at
FROM customers c
LEFT JOIN order_summary os ON c.customer_id = os.customer_id

{{ limit_rows(1000) }}
