SELECT *
FROM {{ source('raw', 'raw_ingested_data') }}