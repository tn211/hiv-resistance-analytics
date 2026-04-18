select
    rc.class_signature,
    count(*) as tce_count
from {{ ref('fact_tce') }} f
join (
    select
        regimen_key,
        array_join(
            array_sort(
                filter(
                    array[
                        case when contains_cri = 1 then 'CRI' end,
                        case when contains_fi = 1 then 'FI' end,
                        case when contains_insti = 1 then 'INI' end,
                        case when contains_nnrti = 1 then 'NNRTI' end,
                        case when contains_nrti = 1 then 'NRTI' end,
                        case when contains_pi = 1 then 'PI' end
                    ],
                    x -> x is not null
                )
            ),
            '+'
        ) as class_signature
    from {{ ref('dim_regimen') }}
    where treatment_type = 'new'
) rc
    on f.new_regimen_key = rc.regimen_key
group by rc.class_signature
order by tce_count desc