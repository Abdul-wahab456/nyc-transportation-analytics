{{
  config(
    materialized='table',
    docs={'node_color': 'green'}
  )
}}

/*
Taxi Type dimension table for NYC Transportation Data Warehouse
- Defines different types of taxi and ride-sharing services
- Provides business context and operational characteristics
- Supports comparative analysis across service types
*/

with taxi_types_base as (
    
    select
        'yellow' as taxi_type_id,
        'Yellow Taxi' as taxi_type_name,
        'Traditional NYC Yellow Cab' as description,
        'TLC Licensed' as regulation_type,
        'Street Hail + App' as booking_method,
        'All 5 Boroughs' as service_area,
        'Metered + Credit Card' as payment_options,
        true as is_traditional_taxi,
        false as is_ride_share,
        'Yellow' as display_color,
        1 as sort_order
    
    union all
    
    select
        'green' as taxi_type_id,
        'Green Taxi' as taxi_type_name,
        'NYC Green Cab (Boro Taxi)' as description,
        'TLC Licensed' as regulation_type,
        'Street Hail + App' as booking_method,
        'Outer Boroughs + Northern Manhattan' as service_area,
        'Metered + Credit Card' as payment_options,
        true as is_traditional_taxi,
        false as is_ride_share,
        'Green' as display_color,
        2 as sort_order
    
    union all
    
    select
        'fhv' as taxi_type_id,
        'For-Hire Vehicle' as taxi_type_name,
        'App-based Ride Sharing Services' as description,
        'TLC Licensed FHV' as regulation_type,
        'Mobile App Only' as booking_method,
        'All 5 Boroughs + Surrounding Areas' as service_area,
        'Credit Card + Digital Payment' as payment_options,
        false as is_traditional_taxi,
        true as is_ride_share,
        'Various' as display_color,
        3 as sort_order
        
),

taxi_types_with_stats as (
    
    select
        tb.*,
        
        -- Statistics from actual trip data
        coalesce(stats.trip_count, 0) as total_trips,
        coalesce(stats.avg_fare, 0) as avg_fare_amount,
        coalesce(stats.avg_distance, 0) as avg_trip_distance,
        coalesce(stats.avg_duration, 0) as avg_trip_duration_minutes,
        coalesce(stats.avg_speed, 0) as avg_speed_mph,
        coalesce(stats.total_revenue, 0) as total_revenue,
        
        -- Service providers (for FHV)
        case 
            when tb.taxi_type_id = 'fhv' then 'Uber, Lyft, Via, Juno'
            else 'NYC TLC Licensed Operators'
        end as service_providers
        
    from taxi_types_base tb
    left join (
        select 
            taxi_type,
            count(*) as trip_count,
            round(avg(fare_amount), 2) as avg_fare,
            round(avg(trip_distance), 2) as avg_distance,
            round(avg(trip_duration_minutes), 2) as avg_duration,
            round(avg(avg_speed_mph), 2) as avg_speed,
            round(sum(total_amount), 2) as total_revenue
        from {{ ref('int_taxi_trips_cleaned') }}
        group by taxi_type
    ) stats on tb.taxi_type_id = stats.taxi_type
    
),

final as (
    
    select
        taxi_type_id,
        taxi_type_name,
        description,
        regulation_type,
        booking_method,
        service_area,
        service_providers,
        payment_options,
        display_color,
        
        -- Classification flags
        is_traditional_taxi,
        is_ride_share,
        case 
            when taxi_type_id in ('yellow', 'green') then true 
            else false 
        end as is_medallion_taxi,
        
        case 
            when taxi_type_id = 'yellow' then true 
            else false 
        end as can_pickup_street_hail_manhattan,
        
        case 
            when taxi_type_id in ('green', 'fhv') then true 
            else false 
        end as serves_outer_boroughs_primarily,
        
        -- Performance metrics
        total_trips,
        avg_fare_amount,
        avg_trip_distance,
        avg_trip_duration_minutes,
        avg_speed_mph,
        total_revenue,
        
        -- Market share calculations
        case 
            when (select sum(total_trips) from taxi_types_with_stats) > 0
            then round(
                total_trips * 100.0 / (select sum(total_trips) from taxi_types_with_stats), 
                2
            )
            else 0
        end as market_share_trips_pct,
        
        case 
            when (select sum(total_revenue) from taxi_types_with_stats) > 0
            then round(
                total_revenue * 100.0 / (select sum(total_revenue) from taxi_types_with_stats), 
                2
            )
            else 0
        end as market_share_revenue_pct,
        
        -- Efficiency metrics
        case 
            when avg_trip_duration_minutes > 0 
            then round(avg_fare_amount / (avg_trip_duration_minutes / 60.0), 2)
            else 0
        end as revenue_per_hour,
        
        case 
            when avg_trip_distance > 0 
            then round(avg_fare_amount / avg_trip_distance, 2)
            else 0
        end as revenue_per_mile,
        
        -- Service characteristics
        case 
            when taxi_type_id = 'yellow' then 'Premium Urban Service'
            when taxi_type_id = 'green' then 'Accessible Outer Borough Service'
            when taxi_type_id = 'fhv' then 'On-Demand Digital Service'
        end as service_positioning,
        
        case 
            when taxi_type_id in ('yellow', 'green') then 'Immediate Availability'
            when taxi_type_id = 'fhv' then 'Scheduled Pickup'
        end as availability_model,
        
        -- Regulatory information
        case 
            when taxi_type_id in ('yellow', 'green') then 'TLC Taxi & Limousine Commission'
            when taxi_type_id = 'fhv' then 'TLC For-Hire Vehicle'
        end as regulatory_body,
        
        case 
            when taxi_type_id in ('yellow', 'green') then 'Medallion Required'
            when taxi_type_id = 'fhv' then 'FHV License Required'
        end as licensing_requirement,
        
        -- Operational characteristics
        case 
            when taxi_type_id = 'yellow' then 'Metered Fare + Surcharges'
            when taxi_type_id = 'green' then 'Metered Fare + Surcharges'
            when taxi_type_id = 'fhv' then 'Dynamic Pricing'
        end as pricing_model,
        
        sort_order,
        current_timestamp() as created_at,
        current_timestamp() as last_updated
        
    from taxi_types_with_stats
    
)

select * from final
order by sort_order