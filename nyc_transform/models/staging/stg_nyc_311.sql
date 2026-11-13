{{
    config(
        incremental_predicates = [
            'DBT_INTERNAL_DEST.created_at >= timestamp_sub(current_timestamp(), interval 90 day)'
        ],
        on_schema_change = 'sync_all_columns'
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

    -- Incremental filter: captures both newly created records AND records updated
    -- after initial filing (e.g. status changed from Open to Closed).
    -- Falls back gracefully across available timestamp indicators.
    {% if is_incremental() %}
        and (
            coalesce(
                safe_cast(resolution_action_updated_date as timestamp),
                safe_cast(closed_date as timestamp),
                safe_cast(created_date as timestamp)
            ) >= timestamp_sub(
                (select max(updated_at) from {{ this }}),
                interval 3 day
            )
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
    coalesce(
        safe_cast(resolution_action_updated_date as timestamp),
        safe_cast(closed_date as timestamp),
        safe_cast(created_date as timestamp)
    ) as updated_at,

    -- Text cleaning: trim whitespace and convert empty strings or literal 'nan'/'null' to true NULLs
    nullif(nullif(nullif(trim(agency), ''), 'nan'), 'null') as agency,
    nullif(nullif(nullif(trim(agency_name), ''), 'nan'), 'null') as agency_name,
    nullif(nullif(nullif(trim(complaint_type), ''), 'nan'), 'null') as complaint_type,
    nullif(nullif(nullif(trim(descriptor), ''), 'nan'), 'null') as descriptor,

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
        when length(trim(incident_zip)) = 5 and regexp_contains(trim(incident_zip), r'^[0-9]+$') 
        then trim(incident_zip)
        else null 
    end as zip_code,
    
    -- City standardisation: force uppercase so BROOKLYN = brooklyn in BI tools, reject 'nan'/'null'
    upper(nullif(nullif(nullif(trim(city), ''), 'nan'), 'null')) as city,
    nullif(nullif(nullif(trim(status), ''), 'nan'), 'null') as status

from deduplicated
-- Logical guard: reject tickets closed before they were created
where safe_cast(created_date as timestamp)
    <= coalesce(safe_cast(closed_date as timestamp), current_timestamp())
