WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_supplies') }}
)

SELECT
    id AS supply_id,
    name AS supply_name,
    sku AS product_sku,
    cost AS cost_cents,
    {{ cents_to_dollars('cost') }} AS cost_dollars,
    perishable AS is_perishable,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM source
