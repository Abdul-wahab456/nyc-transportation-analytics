{{
  config(
    materialized='table',
    docs={'node_color': 'blue'}
  )
}}

/*
Location-based analysis report for NYC taxi operations
- Analyzes pickup and dropoff patterns by location
- Identifies high-demand routes and zones
- Supports location-based business decisions
*/

with location_pickup_stats as (
    
    select
        pickup_location_key as location_id,
        pickup_borough as borough,
        pickup_zone_name as zone_name,
        pickup_district_type as district_type,
        pickup_zone_classification as zone_classification,
        
        -- Pickup metrics
        count(*) as pickup_trips,
        sum(fare_amount) as pickup_revenue,
        sum(total_amount) as pickup_total_revenue,
        sum(passenger_count) as pickup_passengers,
        sum(trip_distance) as pickup_total_distance,
        
        -- Pickup averages
        round(avg(fare_amount), 2) as avg_pickup_fare,
        round(avg(total_amount), 2) as avg_pickup_total,
        round(avg(trip_distance), 2) as avg_pickup_distance,
        round(avg(trip_duration_minutes), 2) as avg_pickup_duration,
        round(avg(avg_speed_mph), 2) as avg_pickup_speed,
        round(avg(tip_percentage), 4) as avg_pickup_tip_pct,
        
        -- Time patterns for pickups
        sum(case when time_period = 'morning' then 1 else 0 end) as pickup_morning,
        sum(case when time_period = 'afternoon' then 1 else 0 end) as pickup_afternoon,
        sum(case when time_period = 'evening' then 1 else 0 end) as pickup_evening,
        sum(case when time_period = 'night' then 1 else 0 end) as pickup_night,
        sum(case when is_rush_hour then 1 else 0 end) as pickup_rush_hour,
        sum(case when is_weekend then 1 else 0 end) as pickup_weekend,
        
        -- Service quality at pickup
        sum(case when is_efficient_trip = 1 then 1 else 0 end) as pickup_efficient_trips,
        sum(case when trip_quality_score = 'high_quality' then 1 else 0 end) as pickup_quality_trips,
        
        -- Taxi type distribution
        sum(case when taxi_type_key = 'yellow' then 1 else 0 end) as pickup_yellow,
        sum(case when taxi_type_key = 'green' then 1 else 0 end) as pickup_green,
        sum(case when taxi_type_key = 'fhv' then 1 else 0 end) as pickup_fhv
        
    from {{ ref('fact_taxi_trips') }}
    group by 
        pickup_location_key, pickup_borough, pickup_zone_name, 
        pickup_district_type, pickup_zone_classification
        
),

location_dropoff_stats as (
    
    select
        dropoff_location_key as location_id,
        dropoff_borough as borough,
        dropoff_zone_name as zone_name,
        dropoff_district_type as district_type,
        dropoff_zone_classification as zone_classification,
        
        -- Dropoff metrics
        count(*) as dropoff_trips,
        sum(fare_amount) as dropoff_revenue,
        sum(total_amount) as dropoff_total_revenue,
        sum(passenger_count) as dropoff_passengers,
        sum(trip_distance) as dropoff_total_distance,
        
        -- Dropoff averages
        round(avg(fare_amount), 2) as avg_dropoff_fare,
        round(avg(total_amount), 2) as avg_dropoff_total,
        round(avg(trip_distance), 2) as avg_dropoff_distance,
        round(avg(trip_duration_minutes), 2) as avg_dropoff_duration,
        round(avg(avg_speed_mph), 2) as avg_dropoff_speed,
        round(avg(tip_percentage), 4) as avg_dropoff_tip_pct,
        
        -- Time patterns for dropoffs
        sum(case when time_period = 'morning' then 1 else 0 end) as dropoff_morning,
        sum(case when time_period = 'afternoon' then 1 else 0 end) as dropoff_afternoon,
        sum(case when time_period = 'evening' then 1 else 0 end) as dropoff_evening,
        sum(case when time_period = 'night' then 1 else 0 end) as dropoff_night,
        sum(case when is_rush_hour then 1 else 0 end) as dropoff_rush_hour,
        sum(case when is_weekend then 1 else 0 end) as dropoff_weekend,
        
        -- Service quality at dropoff
        sum(case when is_efficient_trip = 1 then 1 else 0 end) as dropoff_efficient_trips,
        sum(case when trip_quality_score = 'high_quality' then 1 else 0 end) as dropoff_quality_trips,
        
        -- Taxi type distribution
        sum(case when taxi_type_key = 'yellow' then 1 else 0 end) as dropoff_yellow,
        sum(case when taxi_type_key = 'green' then 1 else 0 end) as dropoff_green,
        sum(case when taxi_type_key = 'fhv' then 1 else 0 end) as dropoff_fhv
        
    from {{ ref('fact_taxi_trips') }}
    group by 
        dropoff_location_key, dropoff_borough, dropoff_zone_name, 
        dropoff_district_type, dropoff_zone_classification
        
),

location_combined as (
    
    select
        coalesce(p.location_id, d.location_id) as location_id,
        coalesce(p.borough, d.borough) as borough,
        coalesce(p.zone_name, d.zone_name) as zone_name,
        coalesce(p.district_type, d.district_type) as district_type,
        coalesce(p.zone_classification, d.zone_classification) as zone_classification,
        
        -- Total activity
        coalesce(p.pickup_trips, 0) as pickup_trips,
        coalesce(d.dropoff_trips, 0) as dropoff_trips,
        coalesce(p.pickup_trips, 0) + coalesce(d.dropoff_trips, 0) as total_trips,
        
        -- Revenue metrics
        coalesce(p.pickup_revenue, 0) as pickup_revenue,
        coalesce(d.dropoff_revenue, 0) as dropoff_revenue,
        coalesce(p.pickup_revenue, 0) + coalesce(d.dropoff_revenue, 0) as total_revenue,
        
        coalesce(p.pickup_total_revenue, 0) as pickup_total_revenue,
        coalesce(d.dropoff_total_revenue, 0) as dropoff_total_revenue,
        coalesce(p.pickup_total_revenue, 0) + coalesce(d.dropoff_total_revenue, 0) as total_amount_revenue,
        
        -- Passenger and distance totals
        coalesce(p.pickup_passengers, 0) + coalesce(d.dropoff_passengers, 0) as total_passengers,
        coalesce(p.pickup_total_distance, 0) + coalesce(d.dropoff_total_distance, 0) as total_distance,
        
        -- Average metrics (weighted by trip volume)
        case 
            when (coalesce(p.pickup_trips, 0) + coalesce(d.dropoff_trips, 0)) > 0
            then round(
                (coalesce(p.avg_pickup_fare * p.pickup_trips, 0) + coalesce(d.avg_dropoff_fare * d.dropoff_trips, 0)) /
                (coalesce(p.pickup_trips, 0) + coalesce(d.dropoff_trips, 0)), 2
            )
            else 0
        end as avg_fare_combined,
        
        -- Activity patterns
        case 
            when coalesce(p.pickup_trips, 0) > coalesce(d.dropoff_trips, 0) * 1.5 then 'Origin Heavy'
            when coalesce(d.dropoff_trips, 0) > coalesce(p.pickup_trips, 0) * 1.5 then 'Destination Heavy'
            else 'Balanced'
        end as activity_pattern,
        
        -- Peak time analysis
        (coalesce(p.pickup_rush_hour, 0) + coalesce(d.dropoff_rush_hour, 0)) as rush_hour_trips,
        (coalesce(p.pickup_weekend, 0) + coalesce(d.dropoff_weekend, 0)) as weekend_trips,
        
        -- Time distribution
        (coalesce(p.pickup_morning, 0) + coalesce(d.dropoff_morning, 0)) as morning_trips,
        (coalesce(p.pickup_afternoon, 0) + coalesce(d.dropoff_afternoon, 0)) as afternoon_trips,
        (coalesce(p.pickup_evening, 0) + coalesce(d.dropoff_evening, 0)) as evening_trips,
        (coalesce(p.pickup_night, 0) + coalesce(d.dropoff_night, 0)) as night_trips,
        
        -- Service quality
        (coalesce(p.pickup_efficient_trips, 0) + coalesce(d.dropoff_efficient_trips, 0)) as efficient_trips,
        (coalesce(p.pickup_quality_trips, 0) + coalesce(d.dropoff_quality_trips, 0)) as quality_trips,
        
        -- Taxi type totals
        (coalesce(p.pickup_yellow, 0) + coalesce(d.dropoff_yellow, 0)) as yellow_trips,
        (coalesce(p.pickup_green, 0) + coalesce(d.dropoff_green, 0)) as green_trips,
        (coalesce(p.pickup_fhv, 0) + coalesce(d.dropoff_fhv, 0)) as fhv_trips,
        
        -- Individual pickup/dropoff metrics for detailed analysis
        p.avg_pickup_fare,
        p.avg_pickup_distance,
        p.avg_pickup_duration,
        p.avg_pickup_tip_pct,
        d.avg_dropoff_fare,
        d.avg_dropoff_distance,
        d.avg_dropoff_duration,
        d.avg_dropoff_tip_pct
        
    from location_pickup_stats p
    full outer join location_dropoff_stats d
        on p.location_id = d.location_id
        
),

location_with_rankings as (
    
    select
        *,
        
        -- Calculate percentages
        case 
            when total_trips > 0 
            then round((rush_hour_trips * 100.0 / total_trips), 2)
            else 0
        end as rush_hour_pct,
        
        case 
            when total_trips > 0 
            then round((weekend_trips * 100.0 / total_trips), 2)
            else 0
        end as weekend_pct,
        
        case 
            when total_trips > 0 
            then round((efficient_trips * 100.0 / total_trips), 2)
            else 0
        end as efficiency_rate_pct,
        
        case 
            when total_trips > 0 
            then round((quality_trips * 100.0 / total_trips), 2)
            else 0
        end as quality_rate_pct,
        
        -- Taxi type distribution percentages
        case 
            when total_trips > 0 
            then round((yellow_trips * 100.0 / total_trips), 2)
            else 0
        end as yellow_pct,
        
        case 
            when total_trips > 0 
            then round((green_trips * 100.0 / total_trips), 2)
            else 0
        end as green_pct,
        
        case 
            when total_trips > 0 
            then round((fhv_trips * 100.0 / total_trips), 2)
            else 0
        end as fhv_pct,
        
        -- Rankings
        row_number() over (order by total_trips desc) as trip_volume_rank,
        row_number() over (order by total_revenue desc) as revenue_rank,
        row_number() over (order by avg_fare_combined desc) as avg_fare_rank,
        row_number() over (partition by borough order by total_trips desc) as borough_rank,
        
        -- Revenue efficiency
        case 
            when total_distance > 0 
            then round(total_revenue / total_distance, 2)
            else 0
        end as revenue_per_mile,
        
        case 
            when total_passengers > 0 
            then round(total_revenue / total_passengers, 2)
            else 0
        end as revenue_per_passenger
        
    from location_combined
    
),

final as (
    
    select
        location_id,
        borough,
        zone_name,
        district_type,
        zone_classification,
        
        -- Volume metrics
        pickup_trips,
        dropoff_trips,
        total_trips,
        activity_pattern,
        
        -- Revenue metrics
        pickup_revenue,
        dropoff_revenue,
        total_revenue,
        avg_fare_combined,
        revenue_per_mile,
        revenue_per_passenger,
        
        -- Time patterns
        rush_hour_trips,
        rush_hour_pct,
        weekend_trips,
        weekend_pct,
        morning_trips,
        afternoon_trips,
        evening_trips,
        night_trips,
        
        -- Service quality
        efficient_trips,
        efficiency_rate_pct,
        quality_trips,
        quality_rate_pct,
        
        -- Taxi type distribution
        yellow_trips,
        yellow_pct,
        green_trips,
        green_pct,
        fhv_trips,
        fhv_pct,
        
        -- Rankings and performance
        trip_volume_rank,
        revenue_rank,
        avg_fare_rank,
        borough_rank,
        
        -- Performance categories
        case 
            when trip_volume_rank <= 10 then 'Top 10'
            when trip_volume_rank <= 25 then 'Top 25'
            when trip_volume_rank <= 50 then 'Top 50'
            else 'Other'
        end as volume_tier,
        
        case 
            when revenue_rank <= 10 then 'Top 10'
            when revenue_rank <= 25 then 'Top 25'
            when revenue_rank <= 50 then 'Top 50'
            else 'Other'
        end as revenue_tier,
        
        case 
            when efficiency_rate_pct >= 80 then 'High Efficiency'
            when efficiency_rate_pct >= 60 then 'Medium Efficiency'
            else 'Low Efficiency'
        end as efficiency_category,
        
        -- Peak time classification
        case 
            when morning_trips = (greatest(morning_trips, afternoon_trips, evening_trips, night_trips)) then 'Morning Peak'
            when afternoon_trips = (greatest(morning_trips, afternoon_trips, evening_trips, night_trips)) then 'Afternoon Peak'
            when evening_trips = (greatest(morning_trips, afternoon_trips, evening_trips, night_trips)) then 'Evening Peak'
            else 'Night Peak'
        end as peak_time_period,
        
        -- Detailed averages for analysis
        avg_pickup_fare,
        avg_dropoff_fare,
        avg_pickup_distance,
        avg_dropoff_distance,
        avg_pickup_tip_pct,
        avg_dropoff_tip_pct,
        
        current_timestamp() as analysis_timestamp
        
    from location_with_rankings
    where total_trips > 0  -- Only include locations with actual activity
    
)

select * from final
order by total_trips desc