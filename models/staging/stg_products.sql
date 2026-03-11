WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_products') }}
)

SELECT
    sku AS product_sku,
    name AS product_name,
    type AS product_type,
    price AS price_cents,
    {{ cents_to_dollars('price') }} AS price_dollars,
    description AS product_description,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM source
