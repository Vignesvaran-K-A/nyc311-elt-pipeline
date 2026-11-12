-- Custom singular test: assert that no row has closed_at earlier than created_at.
-- This would indicate logically impossible data that slipped past the staging WHERE filter.
-- A non-empty result set means the test fails.

select
    complaint_id,
    created_at,
    closed_at,
    timestamp_diff(closed_at, created_at, second) as diff_seconds
from {{ ref('stg_nyc_311') }}
where
    closed_at is not null
    and closed_at < created_at
