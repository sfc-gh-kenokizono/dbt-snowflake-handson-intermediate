WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_orders') }}
)

SELECT
    id AS order_id,
    customer AS customer_id,
    ordered_at AS order_timestamp,
    DATE(ordered_at) AS order_date,
    store_id,
    subtotal AS subtotal_cents,
    {{ cents_to_dollars('subtotal') }} AS subtotal_dollars,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM source
