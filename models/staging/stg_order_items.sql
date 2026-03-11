WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_items') }}
)

SELECT
    id AS item_id,
    order_id,
    sku AS product_sku,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM source
