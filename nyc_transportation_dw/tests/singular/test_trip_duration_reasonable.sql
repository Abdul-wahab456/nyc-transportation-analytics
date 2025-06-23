{{ config(severity='warn') }}

with suspicious_trips as (
    select
        taxi_type,
        trip_id,
        pickup_datetime,
        dropoff_datetime,
        trip_duration_minutes,
        trip_distance,
        fare_amount,
        case
            when trip_duration_minutes < 0 then 'negative_duration'
            when trip_duration_minutes > (72 * 60) then 'excessive_duration'  -- More than 3 days
            when trip_duration_minutes < 0.1 and fare_amount > 20 then 'impossible_short_expensive'
        end as issue_type
    from {{ ref('stg_yellow_taxi') }}
    where 
        trip_duration_minutes < 0  -- Definitely wrong
        or trip_duration_minutes > (72 * 60)  -- More than 3 days is suspicious
        or (trip_duration_minutes < 0.1 and fare_amount > 20)  -- Less than 6 seconds but expensive
)

select
    taxi_type,
    issue_type,
    count(*) as issue_count,
    min(trip_duration_minutes) as min_duration,
    max(trip_duration_minutes) as max_duration,
    avg(trip_duration_minutes) as avg_duration
from suspicious_trips
group by taxi_type, issue_type
having count(*) > 100  -- Only flag if there are many problematic records