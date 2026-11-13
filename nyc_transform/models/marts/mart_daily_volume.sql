-- mart_daily_volume: pre-aggregated daily complaint counts with complaint type
-- and agency breakdown. Powers the "Daily Complaint Volume" chart in the
-- Streamlit dashboard and replaces the ad-hoc GROUP BY run in app.py at query time.

select
    date(created_at)                                        as complaint_date,
    date_trunc(date(created_at), month)                     as complaint_month,
    agency,
    complaint_type,

    count(complaint_id)                                     as total_complaints,
    countif(upper(trim(status)) = 'CLOSED')                 as closed_complaints,
    countif(upper(trim(status)) != 'CLOSED')                as open_complaints,

    -- Average resolution only for rows that have a valid closed timestamp
    round(
        avg(
            case
                when closed_at is not null and closed_at >= created_at
                then timestamp_diff(closed_at, created_at, second) / 3600.0
            end
        ),
        2
    )                                                       as avg_resolution_hours

from {{ ref('stg_nyc_311') }}
where created_at is not null
group by 1, 2, 3, 4
