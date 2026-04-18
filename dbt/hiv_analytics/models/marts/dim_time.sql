with base as (

    select distinct
        cast(baseline_year as integer) as baseline_year
    from {{ source('hivdb', 'tce_case') }}
    where baseline_year is not null

),

final as (

    select
        baseline_year,

        -- decade
        concat(cast(floor(baseline_year / 10) * 10 as varchar), 's') as decade,

        -- year bucket
        case
            when baseline_year < 2000 then '<2000'
            when baseline_year between 2000 and 2005 then '2000-2005'
            when baseline_year between 2006 and 2010 then '2006-2010'
            else '2011+'
        end as year_bucket

    from base

)

select
    row_number() over (order by baseline_year) as time_key,
    *
from final