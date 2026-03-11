WITH order_items AS (
    SELECT * FROM {{ ref('stg_order_items') }}
),

products AS (
    SELECT * FROM {{ ref('stg_products') }}
),

supply_costs AS (
    SELECT
        product_sku,
        SUM(cost_dollars) AS total_supply_cost
    FROM {{ ref('stg_supplies') }}
    GROUP BY product_sku
)

SELECT
    oi.item_id,
    oi.order_id,
    oi.product_sku,
    p.product_name,
    p.product_type,
    p.price_dollars AS unit_price,
    COALESCE(sc.total_supply_cost, 0) AS supply_cost,
    p.price_dollars - COALESCE(sc.total_supply_cost, 0) AS gross_profit
FROM order_items oi
LEFT JOIN products p ON oi.product_sku = p.product_sku
LEFT JOIN supply_costs sc ON oi.product_sku = sc.product_sku
