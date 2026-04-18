select
    t.baseline_year,
    count(*) as tce_count
from {{ ref('fact_tce') }} f
join {{ ref('dim_time') }} t
    on f.time_key = t.time_key
group by t.baseline_year