{% snapshot snap_taxi_zones %}

    {{
        config(
          target_schema='snapshots',
          unique_key='location_id',
          strategy='timestamp',
          updated_at='last_updated',
        )
    }}
    
    select 
        location_id,
        borough,
        zone_name,
        district_type,
        zone_classification,
        urban_classification,
        is_tourism_area,
        is_high_traffic,
        is_airport,
        economic_tier,
        service_level,
        traffic_pattern,
        fare_tier,
        popularity_rank,
        borough_rank,
        total_trip_volume,
        avg_fare_overall,
        last_updated
    from {{ ref('dim_location') }}

{% endsnapshot %}