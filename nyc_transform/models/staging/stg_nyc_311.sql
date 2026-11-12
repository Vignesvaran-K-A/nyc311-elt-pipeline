{{
    config(
        materialized      = 'incremental',
        incremental_strategy = 'merge',
        unique_key        = 'complaint_id',
        partition_by      = {
            'field'       : 'created_at',
            'data_type'   : 'timestamp',
            'granularity' : 'day'
        },
        cluster_by        = ['agency'],
        on_schema_change  = 'sync_all_columns'
    )
}}

with raw_data as (
    select
        *,
        -- Handle Duplicates: If the API returns the same complaint twice, assign
        -- a row number to pick the newest one based on created_date ordering.
        row_number() over (partition by unique_key order by created_date desc) as rn
    from {{ source('bronze', 'raw_nyc_311') }}
    where unique_key is not null

    -- Incremental filter: on subsequent runs only process rows from the last
    -- loaded partition (plus 1-day safety buffer for late-arriving data).
    {% if is_incremental() %}
        and safe_cast(created_date as timestamp) >=
            timestamp_sub(
                (select max(created_at) from {{ this }}),
                interval 1 day
            )
    {% endif %}
),

deduplicated as (
    select * from raw_data where rn = 1
)

select
    cast(unique_key as string) as complaint_id,

    -- Safe casting prevents the pipeline from crashing if a date format changes
    safe_cast(created_date as timestamp) as created_at,
    safe_cast(closed_date  as timestamp) as closed_at,

    -- Text cleaning: trim whitespace and convert empty strings to true NULLs
    nullif(trim(agency),         '') as agency,
    nullif(trim(agency_name),    '') as agency_name,
    nullif(trim(complaint_type), '') as complaint_type,
    nullif(trim(descriptor),     '') as descriptor,

    -- Geographic edge cases: nullify coordinates outside NYC bounding box
    case
        when safe_cast(latitude as float64) between 40.4 and 41.0
        then safe_cast(latitude as float64)
        else null
    end as latitude,

    case
        when safe_cast(longitude as float64) between -74.3 and -73.7
        then safe_cast(longitude as float64)
        else null
    end as longitude,

    -- ZIP code validation: exactly 5 numeric digits only
    case
        when length(trim(incident_zip)) = 5
         and regexp_contains(trim(incident_zip), r'^[0-9]+$')
        then trim(incident_zip)
        else null
    end as zip_code,

    -- City standardisation: force uppercase so BROOKLYN = brooklyn in BI tools
    upper(nullif(trim(city),   '')) as city,
    nullif(trim(status),       '') as status

from deduplicated
-- Logical guard: reject tickets closed before they were created
where safe_cast(created_date as timestamp)
    <= coalesce(safe_cast(closed_date as timestamp), current_timestamp())
