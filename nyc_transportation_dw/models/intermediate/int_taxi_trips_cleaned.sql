{{
  config(
    materialized='view',
    docs={'node_color': 'orange'}
  )
}}

/*
Intermediate model that combines all taxi types into a unified, clean dataset
- Combines Yellow, Green, and FHV data
- Applies consistent business rules across all taxi types
- Creates standardized fields for downstream analysis
*/

with yellow_taxi_cleaned as (
    
    select
        trip_id,
        'yellow' as taxi_type,
        taxi_type as taxi_type_detail,
        vendorid::string as vendor_id,
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount,
        payment_type,
        trip_duration_minutes,
        fare_per_mile,
        avg_speed_mph,
        pickup_date,
        pickup_hour,
        pickup_day_of_week,
        pickup_day_name,
        pickup_month,
        pickup_year,
        is_weekend,
        is_rush_hour,
        data_quality_flag,
        is_valid_trip,
        
        -- Standardized fields
        null::string as service_provider,
        null::integer as wait_time_minutes,
        null::string as service_quality_rating,
        rate_code_id,
        store_and_fwd_flag,
        
        _source_file,
        _loaded_at,
        dbt_updated_at
        
    from {{ ref('stg_yellow_taxi') }}
    where is_valid_trip = true  -- Only include clean data
    
),

green_taxi_cleaned as (
    
    select
        trip_id,
        'green' as taxi_type,
        taxi_type as taxi_type_detail,
        vendorid::string as vendor_id,
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount,
        payment_type,
        trip_duration_minutes,
        fare_per_mile,
        avg_speed_mph,
        pickup_date,
        pickup_hour,
        pickup_day_of_week,
        pickup_day_name,
        pickup_month,
        pickup_year,
        is_weekend,
        is_rush_hour,
        data_quality_flag,
        is_valid_trip,
        
        -- Standardized fields
        null::string as service_provider,
        null::integer as wait_time_minutes,
        null::string as service_quality_rating,
        rate_code_id,
        store_and_fwd_flag,
        
        _source_file,
        _loaded_at,
        dbt_updated_at
        
    from {{ ref('stg_green_taxi') }}
    where is_valid_trip = true  -- Only include clean data
    
),

fhv_cleaned as (
    
    select
        trip_id,
        'fhv' as taxi_type,
        taxi_type as taxi_type_detail,
        vendor_id,
        pickup_datetime,
        dropoff_datetime,
        pickup_location_id,
        dropoff_location_id,
        passenger_count,
        trip_distance,
        fare_amount,
        tip_amount,
        tolls,
        total_amount,
        payment_type,
        trip_duration_minutes,
        fare_per_mile,
        avg_speed_mph,
        pickup_date,
        pickup_hour,
        pickup_day_of_week,
        pickup_day_name,
        pickup_month,
        pickup_year,
        is_weekend,
        is_rush_hour,
        data_quality_flag,
        is_valid_trip,
        
        -- FHV-specific fields
        service_provider,
        wait_time_minutes,
        service_quality_rating,
        null::integer as rate_code_id,
        null::string as store_and_fwd_flag,
        
        _source_file,
        _loaded_at,
        dbt_updated_at
        
    from {{ ref('stg_fhv') }}
    where is_valid_trip = true  -- Only include clean data
    
),

all_trips_combined as (
    
    select * from yellow_taxi_cleaned
    union all
    select * from green_taxi_cleaned  
    union all
    select * from fhv_cleaned
    
),

final_enriched as (
    
    select
        *,
        
        -- Trip categorization
        case 
            when trip_distance <= 1 then 'short'
            when trip_distance <= 5 then 'medium'
            when trip_distance <= 15 then 'long'
            else 'very_long'
        end as trip_category,
        
        case 
            when fare_amount <= 10 then 'low'
            when fare_amount <= 25 then 'medium'
            when fare_amount <= 50 then 'high'
            else 'premium'
        end as fare_category,
        
        case 
            when trip_duration_minutes <= 15 then 'quick'
            when trip_duration_minutes <= 30 then 'normal'
            when trip_duration_minutes <= 60 then 'slow'
            else 'very_slow'
        end as duration_category,
        
        -- Time period classification
        case 
            when pickup_hour between 6 and 11 then 'morning'
            when pickup_hour between 12 and 17 then 'afternoon'
            when pickup_hour between 18 and 21 then 'evening'
            else 'night'
        end as time_period,
        
        -- Business metrics
        case 
            when trip_distance > 0 and trip_duration_minutes > 0 
            then fare_amount / (trip_distance * (trip_duration_minutes / 60.0))
            else null 
        end as revenue_efficiency,
        
        case 
            when fare_amount > 0 then tip_amount / fare_amount 
            else null 
        end as tip_percentage,
        
        -- Geographic patterns
        case 
            when pickup_location_id = dropoff_location_id then 'same_zone'
            when abs(pickup_location_id - dropoff_location_id) <= 5 then 'nearby_zones'
            else 'distant_zones'
        end as trip_pattern,
        
        -- Seasonal analysis
        case 
            when pickup_month in (12, 1, 2) then 'winter'
            when pickup_month in (3, 4, 5) then 'spring'
            when pickup_month in (6, 7, 8) then 'summer'
            when pickup_month in (9, 10, 11) then 'fall'
        end as season,
        
        -- Quality scoring
        case 
            when avg_speed_mph between 5 and 45 
                and fare_per_mile between 1 and 10
                and tip_percentage between 0 and 0.5
            then 'high_quality'
            when avg_speed_mph between 2 and 60
                and fare_per_mile between 0.5 and 15
            then 'medium_quality'
            else 'low_quality'
        end as trip_quality_score
        
    from all_trips_combined
    
)

select * from final_enriched