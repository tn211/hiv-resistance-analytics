{{ config(materialized="table") }}

SELECT * FROM {{ source("hiv_analysis", "genotype_phenotype_pi") }}
