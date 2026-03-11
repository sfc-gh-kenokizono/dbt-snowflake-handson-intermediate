SELECT
    order_id,
    order_total
FROM {{ ref('fct_orders') }}
WHERE order_total < 0
