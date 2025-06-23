{{
  config(
    materialized='table',
    schema='STAGING'
  )
}}

/*
Staging model for Yellow Taxi data
- Cleans and standardizes raw yellow taxi data
- Applies data quality filters
- Adds calculated fields for analysis
*/

with source_data as (
    
    select * from {{ source('raw_data', 'yellow_taxi_raw') }}
    
),

cleaned_data as (
    
    select
        -- Primary keys and identifiers
        {{ dbt_utils.generate_surrogate_key(['vendorid', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'pulocationid']) }} as trip_id,
        vendorid,
        
        -- Temporal fields
        tpep_pickup_datetime as pickup_datetime,
        tpep_dropoff_datetime as dropoff_datetime,
        
        -- Location fields
        pulocationid as pickup_location_id,
        dolocationid as dropoff_location_id,
        
        -- Trip characteristics
        passenger_count,
        trip_distance,
        ratecodeid as rate_code_id,
        store_and_fwd_flag,
        
        -- Financial fields
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        
        -- Calculated fields
        datediff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) as trip_duration_minutes,
        case 
            when trip_distance > 0 then fare_amount / trip_distance 
            else null 
        end as fare_per_mile,
        
        case 
            when datediff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) > 0 
            then trip_distance / (datediff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) / 60.0)
            else null 
        end as avg_speed_mph,
        
        -- Date dimensions
        date(tpep_pickup_datetime) as pickup_date,
        hour(tpep_pickup_datetime) as pickup_hour,
        dayofweek(tpep_pickup_datetime) as pickup_day_of_week,
        dayname(tpep_pickup_datetime) as pickup_day_name,
        month(tpep_pickup_datetime) as pickup_month,
        year(tpep_pickup_datetime) as pickup_year,
        
        case 
            when dayofweek(tpep_pickup_datetime) in (1, 7) then true
            else false 
        end as is_weekend,
        
        case 
            when hour(tpep_pickup_datetime) between 7 and 9 
                or hour(tpep_pickup_datetime) between 17 and 19 
            then true
            else false 
        end as is_rush_hour,
        
        -- Data quality flags
        case 
            when tpep_pickup_datetime >= tpep_dropoff_datetime then 'invalid_timestamps'
            when trip_distance < 0 then 'negative_distance'
            when fare_amount < 0 then 'negative_fare'
            when total_amount < 0 then 'negative_total'
            when passenger_count <= 0 then 'invalid_passenger_count'
            when trip_distance > {{ var('max_trip_distance') }} then 'excessive_distance'
            when fare_amount > {{ var('max_fare_amount') }} then 'excessive_fare'
            when datediff('hour', tpep_pickup_datetime, tpep_dropoff_datetime) > {{ var('max_trip_duration_hours') }} then 'excessive_duration'
            when datediff('minute', tpep_pickup_datetime, tpep_dropoff_datetime) < {{ var('min_trip_duration_minutes') }} then 'too_short_duration'
            else 'valid'
        end as data_quality_flag,
        
        -- Taxi type
        'yellow' as taxi_type,
        
        -- Metadata
        _source_file,
        _loaded_at,
        current_timestamp() as dbt_updated_at
        
    from source_data
    
),

final as (
    
    select 
        *,
        -- Additional business logic flags
        case 
            when data_quality_flag = 'valid' 
                and pickup_location_id is not null 
                and dropoff_location_id is not null
                and fare_amount > 0
                and trip_distance > 0
            then true
            else false 
        end as is_valid_trip
        
    from cleaned_data
    
)

select * from final

-- Data quality tests will be defined in schema.yml