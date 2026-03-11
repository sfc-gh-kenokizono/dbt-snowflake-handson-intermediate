WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_stores') }}
)

SELECT
    id AS store_id,
    name AS store_name,
    opened_at AS store_opened_at,
    tax_rate,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM source
