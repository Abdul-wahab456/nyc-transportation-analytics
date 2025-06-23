{{
  config(
    materialized='view',
    docs={'node_color': 'purple'}
  )
}}

/*
Staging model for For-Hire Vehicle (FHV) data
- Standardizes FHV data to match taxi data structure
- Handles Uber, Lyft, and other ride-sharing services
*/

with source_data as (
    
    select * from {{ source('raw_data', 'fhv_raw') }}
    
),

cleaned_data as (
    
    select
        -- Primary keys and identifiers
        {{ dbt_utils.generate_surrogate_key(['hvfhs_license_num', 'pickup_datetime', 'dropoff_datetime', 'pulocationid']) }} as trip_id,
        hvfhs_license_num as vendor_id,  -- Standardized as vendor_id
        
        -- Temporal fields (standardized column names)
        pickup_datetime,
        dropoff_datetime,
        request_datetime,
        on_scene_datetime,
        
        -- Location fields
        pulocationid as pickup_location_id,
        dolocationid as dropoff_location_id,
        
        -- Trip characteristics
        case 
            when shared_request_flag = 'Y' then 1 
            else 0 
        end as passenger_count,  -- Estimated for FHV
        trip_miles as trip_distance,
        dispatching_base_num,
        originating_base_num,
        
        -- Financial fields
        1 as payment_type,  -- Assume electronic payment for FHV
        base_passenger_fare as fare_amount,
        tolls,
        bcf,
        sales_tax,
        tips as tip_amount,
        driver_pay,
        base_passenger_fare + coalesce(tolls, 0) + coalesce(bcf, 0) + 
        coalesce(sales_tax, 0) + coalesce(congestion_surcharge, 0) + 
        coalesce(airport_fee, 0) + coalesce(tips, 0) as total_amount,
        congestion_surcharge,
        airport_fee,
        
        -- FHV-specific fields
        shared_request_flag,
        shared_match_flag,
        access_a_ride_flag,
        wav_request_flag,
        wav_match_flag,
        trip_time as trip_time_seconds,
        
        -- Calculated fields
        datediff('minute', pickup_datetime, dropoff_datetime) as trip_duration_minutes,
        case 
            when trip_miles > 0 then base_passenger_fare / trip_miles 
            else null 
        end as fare_per_mile,
        
        case 
            when datediff('minute', pickup_datetime, dropoff_datetime) > 0 
            then trip_miles / (datediff('minute', pickup_datetime, dropoff_datetime) / 60.0)
            else null 
        end as avg_speed_mph,
        
        -- Wait time analysis
        datediff('minute', request_datetime, pickup_datetime) as wait_time_minutes,
        datediff('minute', request_datetime, on_scene_datetime) as response_time_minutes,
        
        -- Date dimensions
        date(pickup_datetime) as pickup_date,
        hour(pickup_datetime) as pickup_hour,
        dayofweek(pickup_datetime) as pickup_day_of_week,
        dayname(pickup_datetime) as pickup_day_name,
        month(pickup_datetime) as pickup_month,
        year(pickup_datetime) as pickup_year,
        
        case 
            when dayofweek(pickup_datetime) in (1, 7) then true
            else false 
        end as is_weekend,
        
        case 
            when hour(pickup_datetime) between 7 and 9 
                or hour(pickup_datetime) between 17 and 19 
            then true
            else false 
        end as is_rush_hour,
        
        -- Data quality flags
        case 
            when pickup_datetime >= dropoff_datetime then 'invalid_timestamps'
            when trip_miles < 0 then 'negative_distance'
            when base_passenger_fare < 0 then 'negative_fare'
            when trip_miles > {{ var('max_trip_distance') }} then 'excessive_distance'
            when base_passenger_fare > {{ var('max_fare_amount') }} then 'excessive_fare'
            when datediff('hour', pickup_datetime, dropoff_datetime) > {{ var('max_trip_duration_hours') }} then 'excessive_duration'
            when datediff('minute', pickup_datetime, dropoff_datetime) < {{ var('min_trip_duration_minutes') }} then 'too_short_duration'
            when request_datetime > pickup_datetime then 'invalid_request_time'
            else 'valid'
        end as data_quality_flag,
        
        -- Service provider identification
        case 
            when hvfhs_license_num = 'HV0002' then 'Juno'
            when hvfhs_license_num = 'HV0003' then 'Uber'
            when hvfhs_license_num = 'HV0004' then 'Via'
            when hvfhs_license_num = 'HV0005' then 'Lyft'
            else 'Other'
        end as service_provider,
        
        -- Taxi type
        'fhv' as taxi_type,
        
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
        end as is_valid_trip,
        
        -- Service quality metrics
        case 
            when wait_time_minutes <= 5 then 'excellent'
            when wait_time_minutes <= 10 then 'good'
            when wait_time_minutes <= 15 then 'fair'
            else 'poor'
        end as service_quality_rating
        
    from cleaned_data
    
)

select * from final