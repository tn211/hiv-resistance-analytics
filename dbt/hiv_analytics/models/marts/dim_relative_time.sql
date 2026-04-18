with all_dates as (

    select relative_date, xml_filename
    from {{ source('hivdb', 'tce_measurements') }}

    union

    select relative_date, xml_filename
    from {{ source('hivdb', 'tce_isolates') }}

    union

    select relative_date, xml_filename
    from {{ source('hivdb', 'tce_mutations') }}

    union

    select relative_start_date as relative_date, xml_filename
    from {{ source('hivdb', 'tce_treatments') }}

    union

    select relative_stop_date as relative_date, xml_filename
    from {{ source('hivdb', 'tce_treatments') }}

),

joined as (

    select
        d.relative_date,
        c.date_unit
    from all_dates d
    join {{ source('hivdb', 'tce_case') }} c
        on d.xml_filename = c.xml_filename

),

final as (

    select distinct
        relative_date,
        date_unit,

        case
            when date_unit = 'months' then cast(relative_date as double) * 4
            else cast(relative_date as double)
        end as relative_weeks,

        case
            when cast(relative_date as double) < 0 then 'pre'
            when cast(relative_date as double) = 0 then 'baseline'
            else 'post'
        end as timepoint_phase,

        case
            when cast(relative_date as double) < -52 then 'far_pre'
            when cast(relative_date as double) between -52 and -1 then 'recent_pre'
            when cast(relative_date as double) = 0 then 'baseline'
            when cast(relative_date as double) between 1 and 24 then 'early_post'
            else 'late_post'
        end as timepoint_bucket,

        case
            when cast(relative_date as double) = 0 then true
            else false
        end as is_baseline

    from joined
    where relative_date is not null

)

select
    row_number() over (order by relative_date, date_unit) as relative_time_key,
    *
from final