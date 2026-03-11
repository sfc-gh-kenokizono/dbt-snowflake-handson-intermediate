WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_customers') }}
)

SELECT
    id AS customer_id,
    TRIM(name) AS customer_name,
    CURRENT_TIMESTAMP() AS _loaded_at
FROM source
