{{
  config(
    materialized='table',
    docs={'node_color': 'green'},
    indexes=[
      {'columns': ['pickup_date_key'], 'type': 'btree'},
      {'columns': ['pickup_location_key'], 'type': 'btree'},
      {'columns': ['dropoff_location_key'], 'type': 'btree'},
      {'columns': ['taxi_type_key'], 'type': 'btree'}
    ]
  )
}}

/*
Fact table for NYC taxi trips - the heart of our data warehouse
- Combines all cleaned trip data with dimensional keys
- Optimized for analytical queries and reporting
- Includes pre-calculated business metrics
*/

with trip_facts as (
    
    select
        -- Fact table primary key
        trip_id as trip_key,
        
        -- Dimensional foreign keys
        pickup_date as pickup_date_key,
        pickup_location_id as pickup_location_key,
        dropoff_location_id as dropoff_location_key,
        taxi_type as taxi_type_key,
        
        -- Temporal attributes
        pickup_datetime,
        dropoff_datetime,
        pickup_hour,
        pickup_day_of_week,
        pickup_day_name,
        pickup_month,
        pickup_year,
        is_weekend,
        is_rush_hour,
        time_period,
        season,
        
        -- Trip characteristics
        vendor_id,
        passenger_count,
        trip_distance,
        trip_duration_minutes,
        payment_type,
        service_provider,
        
        -- Financial measures (additive facts)
        fare_amount,
        tip_amount,
        tolls_amount,
        total_amount,
        
        -- Calculated measures
        fare_per_mile,
        avg_speed_mph,
        tip_percentage,
        revenue_efficiency,
        
        -- Trip categorization
        trip_category,
        fare_category,
        duration_category,
        trip_pattern,
        trip_quality_score,
        
        -- Business flags
        wait_time_minutes,
        service_quality_rating,
        rate_code_id,
        store_and_fwd_flag,
        
        -- Data lineage
        _source_file,
        _loaded_at,
        dbt_updated_at
        
    from {{ ref('int_taxi_trips_cleaned') }}
    
),

fact_with_enrichments as (
    
    select
        tf.*,
        
        -- Date dimension enrichments
        dd.is_federal_holiday,
        dd.federal_holiday,
        dd.special_event,
        dd.quarter,
        dd.week_of_year,
        dd.is_business_day,
        dd.school_period,
        
        -- Pickup location enrichments
        pl.borough as pickup_borough,
        pl.zone_name as pickup_zone_name,
        pl.district_type as pickup_district_type,
        pl.is_tourism_area as pickup_is_tourism_area,
        pl.is_high_traffic as pickup_is_high_traffic,
        pl.zone_classification as pickup_zone_classification,
        pl.fare_tier as pickup_fare_tier,
        pl.is_airport as pickup_is_airport,
        
        -- Dropoff location enrichments
        dl.borough as dropoff_borough,
        dl.zone_name as dropoff_zone_name,
        dl.district_type as dropoff_district_type,
        dl.is_tourism_area as dropoff_is_tourism_area,
        dl.is_high_traffic as dropoff_is_high_traffic,
        dl.zone_classification as dropoff_zone_classification,
        dl.fare_tier as dropoff_fare_tier,
        dl.is_airport as dropoff_is_airport,
        
        -- Taxi type enrichments
        tt.taxi_type_name,
        tt.is_traditional_taxi,
        tt.is_ride_share,
        tt.service_positioning,
        tt.pricing_model,
        
        -- Trip geography analysis
        case 
            when pl.borough = dl.borough then 'Intra-Borough'
            else 'Inter-Borough'
        end as trip_geography_type,
        
        case 
            when pl.borough = dl.borough then pl.borough || ' Local'
            else pl.borough || ' to ' || dl.borough
        end as trip_route_description,
        
        -- Business context
        case 
            when pickup_is_airport or dropoff_is_airport then 'Airport Trip'
            when pl.is_tourism_area or dl.is_tourism_area then 'Tourism Trip'
            when pl.district_type = 'Business District' and dl.district_type = 'Business District' then 'Business Trip'
            when tf.is_rush_hour then 'Commuter Trip'
            else 'General Trip'
        end as trip_purpose_category,
        
        -- Premium service indicators
        case 
            when tf.fare_amount > tt.avg_fare_amount * 1.5 then 'Premium Fare'
            when tf.tip_percentage > 0.2 then 'High Tip'
            when pl.fare_tier = 'High Value' or dl.fare_tier = 'High Value' then 'High Value Zone'
            else 'Standard Service'
        end as service_level_indicator
        
    from trip_facts tf
    left join {{ ref('dim_date') }} dd 
        on tf.pickup_date_key = dd.date_day
    left join {{ ref('dim_location') }} pl 
        on tf.pickup_location_key = pl.location_id
    left join {{ ref('dim_location') }} dl 
        on tf.dropoff_location_key = dl.location_id
    left join {{ ref('dim_taxi_type') }} tt 
        on tf.taxi_type_key = tt.taxi_type_id
    
),

fact_with_derived_metrics as (
    
    select
        *,
        
        -- Advanced business metrics
        case 
            when trip_distance > 0 and trip_duration_minutes > 0
            then round(
                (fare_amount * 60.0) / trip_duration_minutes,  -- Revenue per hour
                2
            )
            else 0
        end as revenue_per_hour_actual,
        
        case 
            when trip_distance > 0
            then round(fare_amount / trip_distance, 2)  -- Revenue per mile
            else 0
        end as revenue_per_mile_actual,
        
        -- Efficiency scoring
        case 
            when avg_speed_mph between 8 and 35 
                and trip_duration_minutes >= 5 
                and fare_per_mile between 2 and 8
            then 1
            else 0
        end as is_efficient_trip,
        
        -- Customer satisfaction proxy
        case 
            when tip_percentage > 0.15 
                and trip_quality_score = 'high_quality'
                and (service_quality_rating = 'excellent' or service_quality_rating is null)
            then 1
            else 0
        end as is_likely_satisfied_customer,
        
        -- Demand pattern indicators
        case 
            when is_rush_hour and trip_geography_type = 'Inter-Borough' then 1
            else 0
        end as is_peak_demand_trip,
        
        case 
            when is_weekend and (pickup_is_tourism_area or dropoff_is_tourism_area) then 1
            else 0
        end as is_leisure_trip,
        
        -- Revenue quality
        case 
            when total_amount > 0 and fare_amount > 0
            then round((fare_amount / total_amount) * 100, 2)
            else 0
        end as base_fare_percentage,
        
        -- Operational flags for reporting
        case 
            when pickup_is_airport and not dropoff_is_airport then 'Airport Departure'
            when dropoff_is_airport and not pickup_is_airport then 'Airport Arrival'
            when pickup_is_airport and dropoff_is_airport then 'Airport Transfer'
            else 'Non-Airport'
        end as airport_trip_type
        
    from fact_with_enrichments
    
)

select 
    -- Keys and identifiers
    trip_key,
    pickup_date_key,
    pickup_location_key,
    dropoff_location_key,
    taxi_type_key,
    
    -- Core trip facts
    pickup_datetime,
    dropoff_datetime,
    vendor_id,
    passenger_count,
    trip_distance,
    trip_duration_minutes,
    payment_type,
    
    -- Financial measures
    fare_amount,
    tip_amount,
    tolls_amount,
    total_amount,
    
    -- Calculated measures
    fare_per_mile,
    avg_speed_mph,
    tip_percentage,
    revenue_per_hour_actual,
    revenue_per_mile_actual,
    base_fare_percentage,
    
    -- Time attributes
    pickup_hour,
    pickup_day_of_week,
    pickup_month,
    pickup_year,
    is_weekend,
    is_rush_hour,
    is_federal_holiday,
    is_business_day,
    time_period,
    season,
    quarter,
    
    -- Geographic attributes
    pickup_borough,
    dropoff_borough,
    pickup_zone_name,
    dropoff_zone_name,
    pickup_district_type,
    dropoff_district_type,
    pickup_zone_classification,
    dropoff_zone_classification,
    trip_geography_type,
    trip_route_description,
    
    -- Business context
    taxi_type_name,
    service_provider,
    trip_purpose_category,
    trip_category,
    fare_category,
    duration_category,
    service_level_indicator,
    airport_trip_type,
    
    -- Quality and efficiency flags
    is_efficient_trip,
    is_likely_satisfied_customer,
    is_peak_demand_trip,
    is_leisure_trip,
    trip_quality_score,
    service_quality_rating,
    
    -- Special indicators
    pickup_is_tourism_area,
    dropoff_is_tourism_area,
    pickup_is_airport,
    dropoff_is_airport,
    federal_holiday,
    special_event,
    
    -- Data lineage
    _source_file,
    _loaded_at,
    dbt_updated_at,
    current_timestamp() as fact_created_at

from fact_with_derived_metrics