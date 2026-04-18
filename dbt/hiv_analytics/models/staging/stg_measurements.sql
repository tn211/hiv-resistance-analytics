select
    xml_filename,
    patient_alias,
    cast(relative_date as integer) as relative_date,
    measurement_type,
    cast(value as double) as value,
    value_type
from {{ source('hivdb', 'tce_measurements') }}