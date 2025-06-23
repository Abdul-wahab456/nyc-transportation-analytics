{{
  config(
    materialized='table',
    docs={'node_color': 'green'}
  )
}}

/*
Location dimension table for NYC Transportation Data Warehouse
- Based on NYC Taxi & Limousine Commission zone definitions
- Provides geographic context for trip analysis
- Includes business district classifications and activity levels
*/

with location_enhanced as (
    
    select
        -- Primary key
        location_id,
        
        -- Geographic attributes
        borough,
        zone_name,
        district_type,
        economic_tier,
        
        -- Activity indicators
        is_tourism_area,
        is_high_traffic,
        volume_category,
        
        -- Statistics from trips
        pickup_count,
        dropoff_count,
        total_trip_volume,
        avg_pickup_fare,
        avg_dropoff_fare,
        
        -- Rankings
        popularity_rank,
        borough_rank,
        
        -- Additional business context
        case 
            when is_tourism_area and is_high_traffic then 'Premium Tourist Zone'
            when is_tourism_area then 'Tourist Area'
            when is_high_traffic then 'High Traffic Zone'
            when district_type = 'Business District' then 'Business Hub'
            when district_type = 'Airport/Transport Hub' then 'Transportation Center'
            else 'Residential/Mixed'
        end as zone_classification,
        
        -- Service level expectations
        case 
            when volume_category in ('very_high', 'high') then 'High Service Demand'
            when volume_category = 'medium' then 'Moderate Service Demand'
            else 'Low Service Demand'
        end as service_level,
        
        -- Economic indicators
        case 
            when avg_pickup_fare > 20 then 'High Value'
            when avg_pickup_fare > 12 then 'Medium Value'
            else 'Standard Value'
        end as fare_tier,
        
        -- Operational characteristics
        case 
            when pickup_count > dropoff_count * 1.2 then 'Origin Heavy'
            when dropoff_count > pickup_count * 1.2 then 'Destination Heavy'
            else 'Balanced'
        end as traffic_pattern,
        
        -- Distance from city center (approximated by location_id)
        case 
            when location_id <= 50 then 'Urban Core'
            when location_id <= 150 then 'Inner Suburbs'
            else 'Outer Areas'
        end as urban_classification,
        
        last_updated
        
    from {{ ref('int_location_mapping') }}
    
),

location_with_neighbors as (
    
    select
        le.*,
        
        -- Identify neighboring zones (simplified - within ±5 location IDs)
        listagg(
            case 
                when ln.location_id != le.location_id 
                    and abs(ln.location_id - le.location_id) <= 5 
                then ln.zone_name 
            end, 
            ', '
        ) as nearby_zones,
        
        -- Count of nearby high-traffic zones
        count(
            case 
                when ln.location_id != le.location_id 
                    and abs(ln.location_id - le.location_id) <= 5 
                    and ln.is_high_traffic 
                then 1 
            end
        ) as nearby_high_traffic_count
        
    from location_enhanced le
    left join location_enhanced ln 
        on abs(ln.location_id - le.location_id) <= 5
    group by 
        le.location_id, le.borough, le.zone_name, le.district_type, 
        le.economic_tier, le.is_tourism_area, le.is_high_traffic, 
        le.volume_category, le.pickup_count, le.dropoff_count, 
        le.total_trip_volume, le.avg_pickup_fare, le.avg_dropoff_fare, 
        le.popularity_rank, le.borough_rank, le.zone_classification, 
        le.service_level, le.fare_tier, le.traffic_pattern, 
        le.urban_classification, le.last_updated
        
),

final as (
    
    select
        location_id,
        borough,
        zone_name,
        district_type,
        economic_tier,
        zone_classification,
        urban_classification,
        
        -- Activity metrics
        is_tourism_area,
        is_high_traffic,
        volume_category,
        service_level,
        traffic_pattern,
        
        -- Trip statistics
        coalesce(pickup_count, 0) as pickup_count,
        coalesce(dropoff_count, 0) as dropoff_count,
        coalesce(total_trip_volume, 0) as total_trip_volume,
        round(coalesce(avg_pickup_fare, 0), 2) as avg_pickup_fare,
        round(coalesce(avg_dropoff_fare, 0), 2) as avg_dropoff_fare,
        
        -- Value metrics
        fare_tier,
        case 
            when avg_pickup_fare > 0 and avg_dropoff_fare > 0 
            then round((avg_pickup_fare + avg_dropoff_fare) / 2, 2)
            else round(coalesce(avg_pickup_fare, avg_dropoff_fare, 0), 2)
        end as avg_fare_overall,
        
        -- Rankings and comparisons
        popularity_rank,
        borough_rank,
        case 
            when popularity_rank <= 10 then 'Top 10'
            when popularity_rank <= 25 then 'Top 25'
            when popularity_rank <= 50 then 'Top 50'
            else 'Other'
        end as popularity_tier,
        
        -- Contextual information
        nearby_zones,
        nearby_high_traffic_count,
        
        -- Data quality indicators
        case 
            when total_trip_volume >= 100 then 'Sufficient Data'
            when total_trip_volume >= 10 then 'Limited Data'
            else 'Minimal Data'
        end as data_quality,
        
        -- Special designations
        case 
            when zone_name like '%Airport%' then true
            when location_id in (1, 2, 132, 138) then true  -- Known airport zones
            else false 
        end as is_airport,
        
        case 
            when zone_name like '%Bridge%' or zone_name like '%Tunnel%' then true
            else false 
        end as is_bridge_tunnel,
        
        -- Operational insights
        case 
            when total_trip_volume > 0 
            then round(pickup_count::float / total_trip_volume * 100, 1)
            else 0
        end as pickup_percentage,
        
        case 
            when total_trip_volume > 0 
            then round(dropoff_count::float / total_trip_volume * 100, 1)
            else 0
        end as dropoff_percentage,
        
        last_updated,
        current_timestamp() as created_at
        
    from location_with_neighbors
    
)

select * from final
order by popularity_rank, location_id