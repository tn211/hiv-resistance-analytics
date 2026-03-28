{{ config(materialized="table") }}

SELECT * FROM {{ source("hivdb", "pi") }} LIMIT 10