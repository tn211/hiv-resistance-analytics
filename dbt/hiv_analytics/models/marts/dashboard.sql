with regimen_counts as (
    select r.regimen_signature
    from {{ ref('fact_tce') }} f
    join {{ ref('dim_regimen') }} r
        on f.new_regimen_key = r.regimen_key
    group by r.regimen_signature
    having count(*) >= 10
)

select
    f.tce_id,
    f.xml_filename,
    t.baseline_year,
    t.decade,
    t.year_bucket,
    f.baseline_log_rna,
    f.baseline_cd4,
    f.followup_log_rna,
    f.followup_cd4,
    f.baseline_mutation_count,
    r.regimen_signature,
    r.drug_count,
    r.contains_pi,
    r.contains_nrti,
    r.contains_nnrti,
    r.contains_insti,
    r.contains_cri,
    r.contains_fi

from {{ ref('fact_tce') }} f
join {{ ref('dim_time') }} t
    on f.time_key = t.time_key
join {{ ref('dim_regimen') }} r
    on f.new_regimen_key = r.regimen_key
join regimen_counts rc
    on r.regimen_signature = rc.regimen_signature