WITH orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

order_items AS (
    SELECT * FROM {{ ref('int_order_items_products') }}
),

order_totals AS (
    SELECT
        order_id,
        SUM(unit_price) AS order_total,
        SUM(gross_profit) AS order_profit,
        COUNT(*) AS item_count
    FROM order_items
    GROUP BY order_id
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['o.order_id']) }} AS order_sk,
    o.order_id,
    o.customer_id,
    c.customer_name,
    o.store_id,
    o.order_timestamp,
    o.order_date,
    COALESCE(ot.order_total, 0) AS order_total,
    COALESCE(ot.order_profit, 0) AS order_profit,
    COALESCE(ot.item_count, 0) AS item_count,
    CURRENT_TIMESTAMP() AS _created_at
FROM orders o
LEFT JOIN customers c ON o.customer_id = c.customer_id
LEFT JOIN order_totals ot ON o.order_id = ot.order_id

{{ limit_rows(10000) }}
