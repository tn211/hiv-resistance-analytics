with case_data as (

    select
        xml_filename,
        cast(baseline_year as integer) as baseline_year,
        cast(cd4_nadir_before_tce as double) as cd4_nadir_before_tce
    from {{ source('hivdb', 'tce_case') }}

),

meas as (

    select
        xml_filename,

        max(case when timepoint_tag = 'baselineRNA' and value_type = 'logLoad'
            then cast(value as double) end) as baseline_log_rna,

        max(case when timepoint_tag = 'baselineCD4' and value_type = 'count'
            then cast(value as double) end) as baseline_cd4,

        max(case when timepoint_tag = 'followupRNA' and value_type = 'logLoad'
            then cast(value as double) end) as followup_log_rna,

        max(case when timepoint_tag = 'followupCD4' and value_type = 'count'
            then cast(value as double) end) as followup_cd4

    from {{ source('hivdb', 'tce_measurements') }}
    group by xml_filename

),

mut as (

    select
        xml_filename,
        count(*) as baseline_mutation_count
    from {{ source('hivdb', 'tce_mutations') }}
    where isolate_type = 'baseline'
    group by xml_filename

),

past_reg as (

    select
        xml_filename,
        regimen_key as past_regimen_key
    from {{ ref('dim_regimen') }}
    where treatment_type = 'past'

),

new_reg as (

    select
        xml_filename,
        regimen_key as new_regimen_key
    from {{ ref('dim_regimen') }}
    where treatment_type = 'new'

),

joined as (

    select
        c.xml_filename,
        c.baseline_year,
        c.cd4_nadir_before_tce,

        m.baseline_log_rna,
        m.baseline_cd4,
        m.followup_log_rna,
        m.followup_cd4,

        coalesce(mu.baseline_mutation_count, 0) as baseline_mutation_count,

        p.past_regimen_key,
        n.new_regimen_key

    from case_data c
    left join meas m on c.xml_filename = m.xml_filename
    left join mut mu on c.xml_filename = mu.xml_filename
    left join past_reg p on c.xml_filename = p.xml_filename
    left join new_reg n on c.xml_filename = n.xml_filename

)

select
    row_number() over (order by xml_filename) as tce_id,
    j.*,
    t.time_key

from joined j
left join {{ ref('dim_time') }} t
    on j.baseline_year = t.baseline_year