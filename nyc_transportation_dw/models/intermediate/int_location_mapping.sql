{{
  config(
    materialized='view',
    docs={'node_color': 'orange'}
  )
}}

/*
Intermediate model for NYC Taxi Zone location mapping
- Creates lookup table for location IDs to zone names and boroughs
- Handles common location patterns and groupings
- Provides geographic context for trip analysis
*/

with location_base as (
    
    select distinct
        pickup_location_id as location_id
    from {{ ref('int_taxi_trips_cleaned') }}
    where pickup_location_id is not null
    
    union
    
    select distinct
        dropoff_location_id as location_id
    from {{ ref('int_taxi_trips_cleaned') }}
    where dropoff_location_id is not null
    
),

-- Since we don't have the actual taxi zone lookup table,
-- we'll create a simplified mapping based on common NYC patterns
location_mapping as (
    
    select
        location_id,
        
        -- Simplified borough assignment based on location ID ranges
        -- This is approximate - in production you'd use the official TLC zone lookup
        case 
            when location_id between 1 and 50 then 'Manhattan'
            when location_id between 51 and 100 then 'Brooklyn'
            when location_id between 101 and 150 then 'Queens'
            when location_id between 151 and 200 then 'Bronx'
            when location_id between 201 and 265 then 'Staten Island'
            else 'Unknown'
        end as borough,
        
        -- Simplified zone naming
        case 
            when location_id between 1 and 25 then 'Lower Manhattan'
            when location_id between 26 and 50 then 'Midtown Manhattan'
            when location_id between 51 and 75 then 'North Brooklyn'
            when location_id between 76 and 100 then 'South Brooklyn'
            when location_id between 101 and 125 then 'West Queens'
            when location_id between 126 and 150 then 'East Queens'
            when location_id between 151 and 175 then 'South Bronx'
            when location_id between 176 and 200 then 'North Bronx'
            when location_id between 201 and 265 then 'Staten Island'
            else 'Unknown Zone'
        end as zone_name,
        
        -- Business district classification
        case 
            when location_id in (4, 13, 24, 41, 42, 43, 50, 68, 79, 87, 88, 90, 
                                 100, 103, 104, 105, 113, 114, 116, 120, 125, 
                                 127, 128, 137, 140, 141, 142, 143, 144, 148, 
                                 151, 152, 153, 158, 161, 162, 163, 164, 166, 
                                 170, 186, 194, 202, 209, 211, 224, 229, 230) 
            then 'Business District'
            when location_id in (1, 2, 3, 12, 25, 48, 74, 75, 89, 95, 112, 130, 
                                 131, 132, 145, 179, 180, 181, 182, 183, 184, 
                                 185, 208, 254, 255, 256, 257, 258, 259, 260, 
                                 261, 262, 263) 
            then 'Airport/Transport Hub'
            when location_id in (6, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 26, 
                                 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38, 
                                 39, 40, 44, 45, 46, 47, 49, 51, 52, 53, 54, 55, 
                                 56, 57, 58, 59, 60, 61, 62, 63, 64, 65, 66, 67, 
                                 69, 70, 71, 72, 73, 76, 77, 78, 80, 81, 82, 83, 
                                 84, 85, 86, 91, 92, 93, 94, 96, 97, 98, 99, 101, 
                                 102, 106, 107, 108, 109, 110, 111, 115, 117, 118, 
                                 119, 121, 122, 123, 124, 126, 129, 133, 134, 135, 
                                 136, 138, 139, 146, 147, 149, 150, 154, 155, 156, 
                                 157, 159, 160, 165, 167, 168, 169, 171, 172, 173, 
                                 174, 175, 176, 177, 178, 187, 188, 189, 190, 191, 
                                 192, 193, 195, 196, 197, 198, 199, 200, 203, 204, 
                                 205, 206, 207, 210, 212, 213, 214, 215, 216, 217, 
                                 218, 219, 220, 221, 222, 223, 225, 226, 227, 228, 
                                 231, 232, 233, 234, 235, 236, 237, 238, 239, 240, 
                                 241, 242, 243, 244, 245, 246, 247, 248, 249, 250, 
                                 251, 252, 253, 264, 265) 
            then 'Residential'
            else 'Mixed Use'
        end as district_type,
        
        -- Tourism/entertainment areas
        case 
            when location_id in (13, 41, 42, 43, 50, 68, 79, 87, 88, 90, 100, 
                                 103, 104, 105, 113, 114, 116, 120, 125, 127, 
                                 128, 137, 140, 141, 142, 143, 144, 148, 151, 
                                 152, 153, 158, 161, 162, 163, 164, 166, 170) 
            then true 
            else false 
        end as is_tourism_area,
        
        -- High traffic zones
        case 
            when location_id in (1, 2, 3, 4, 13, 24, 41, 42, 43, 50, 68, 79, 
                                 87, 88, 90, 100, 103, 104, 105, 113, 114, 116, 
                                 120, 125, 127, 128, 137, 140, 141, 142, 143, 
                                 144, 148, 151, 152, 153, 158, 161, 162, 163, 
                                 164, 166, 170, 186, 194, 202, 209, 211, 224, 
                                 229, 230) 
            then true 
            else false 
        end as is_high_traffic,
        
        -- Economic tier (simplified)
        case 
            when location_id between 1 and 50 then 'high'
            when location_id between 51 and 150 then 'medium'
            else 'low'
        end as economic_tier
        
    from location_base
    
),

location_statistics as (
    
    select
        lm.*,
        
        -- Trip volume statistics (from our cleaned trips)
        pickup_stats.pickup_count,
        pickup_stats.avg_pickup_fare,
        dropoff_stats.dropoff_count,
        dropoff_stats.avg_dropoff_fare,
        
        coalesce(pickup_stats.pickup_count, 0) + 
        coalesce(dropoff_stats.dropoff_count, 0) as total_trip_volume,
        
        case 
            when coalesce(pickup_stats.pickup_count, 0) + 
                 coalesce(dropoff_stats.dropoff_count, 0) > 1000 then 'very_high'
            when coalesce(pickup_stats.pickup_count, 0) + 
                 coalesce(dropoff_stats.dropoff_count, 0) > 500 then 'high'
            when coalesce(pickup_stats.pickup_count, 0) + 
                 coalesce(dropoff_stats.dropoff_count, 0) > 100 then 'medium'
            when coalesce(pickup_stats.pickup_count, 0) + 
                 coalesce(dropoff_stats.dropoff_count, 0) > 10 then 'low'
            else 'very_low'
        end as volume_category
        
    from location_mapping lm
    
    left join (
        select 
            pickup_location_id,
            count(*) as pickup_count,
            round(avg(fare_amount), 2) as avg_pickup_fare
        from {{ ref('int_taxi_trips_cleaned') }}
        group by pickup_location_id
    ) pickup_stats on lm.location_id = pickup_stats.pickup_location_id
    
    left join (
        select 
            dropoff_location_id,
            count(*) as dropoff_count,
            round(avg(fare_amount), 2) as avg_dropoff_fare
        from {{ ref('int_taxi_trips_cleaned') }}
        group by dropoff_location_id
    ) dropoff_stats on lm.location_id = dropoff_stats.dropoff_location_id
    
),

final as (
    
    select
        location_id,
        borough,
        zone_name,
        district_type,
        is_tourism_area,
        is_high_traffic,
        economic_tier,
        pickup_count,
        dropoff_count,
        total_trip_volume,
        volume_category,
        avg_pickup_fare,
        avg_dropoff_fare,
        
        -- Location ranking
        row_number() over (order by total_trip_volume desc) as popularity_rank,
        
        -- Borough-level ranking
        row_number() over (
            partition by borough 
            order by total_trip_volume desc
        ) as borough_rank,
        
        current_timestamp() as last_updated
        
    from location_statistics
    
)

select * from final
order by total_trip_volume desc