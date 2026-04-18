with base as (

    select
        xml_filename,
        treatment_type,
        drug_code,
        drug_class
    from {{ source('hivdb', 'tce_treatments') }}
    where drug_code is not null

),

agg as (

    select
        xml_filename,
        treatment_type,
        count(distinct drug_code) as drug_count,

        max(case when drug_class = 'PI' then 1 else 0 end) as contains_pi,
        max(case when drug_class = 'NRTI' then 1 else 0 end) as contains_nrti,
        max(case when drug_class = 'NNRTI' then 1 else 0 end) as contains_nnrti,
        max(case when drug_class = 'INSTI' then 1 else 0 end) as contains_insti,
        array_join(array_sort(array_agg(distinct drug_code)), '+') as regimen_signature

    from base
    group by 1, 2

)

select
    row_number() over (order by xml_filename, treatment_type) as regimen_key,
    xml_filename,
    treatment_type,
    drug_count,
    contains_pi,
    contains_nrti,
    contains_nnrti,
    contains_insti,
    regimen_signature
from agg