# NYC Transportation Analytics Platform

A comprehensive data engineering and analytics project analyzing NYC taxi and ride-sharing data.

## 🎯 Project Overview

This project builds a modern data warehouse analyzing 23+ million transportation records from NYC's official Taxi and Limousine Commission (TLC) data.

## 📊 Datasets
- **Yellow Taxi**: Traditional Manhattan taxi service (~3M records)
- **Green Taxi**: Outer borough taxi service (~1.2M records)  
- **For-Hire Vehicle**: Uber/Lyft ride-sharing data (~19.6M records)

## 🏗️ Architecture
- **Data Source**: NYC TLC official repository
- **Data Warehouse**: Snowflake Cloud
- **Transformation**: dbt (Data Build Tool)
- **Orchestration**: Snowflake Tasks

## 🚀 Quick Start

1. **Data Acquisition**:
   ```bash
   python src/data_acquisition/data_scrap.py
# 🚀 NYC Transportation Data Warehouse

## 📊 Star Schema ERD

```mermaid
erDiagram
    FACT_TAXI_TRIPS {
        string trip_key PK
        date pickup_date_key FK
        int pickup_location_key FK
        int dropoff_location_key FK
        string taxi_type_key FK
        float fare_amount
        float tip_amount
        float total_amount
        float trip_distance
        int trip_duration_minutes
        float fare_per_mile
        float avg_speed_mph
        boolean is_efficient_trip
        string trip_quality_score
    }

    DIM_DATE {
        date date_day PK
        int year
        int month
        int day
        string day_name
        string season
        boolean is_weekend
        boolean is_federal_holiday
        string federal_holiday
    }

    DIM_LOCATION {
        int location_id PK
        string borough
        string zone_name
        string district_type
        boolean is_tourism_area
        boolean is_high_traffic
        string economic_tier
        int popularity_rank
        float avg_fare_overall
    }

    DIM_TAXI_TYPE {
        string taxi_type_id PK
        string taxi_type_name
        string description
        boolean is_traditional_taxi
        boolean is_ride_share
        string service_positioning
        float market_share_trips_pct
    }

    FACT_TAXI_TRIPS ||--|| DIM_DATE : "pickup_date_key"
    FACT_TAXI_TRIPS ||--|| DIM_LOCATION : "pickup_location_key"
    FACT_TAXI_TRIPS ||--|| DIM_LOCATION : "dropoff_location_key"  
    FACT_TAXI_TRIPS ||--|| DIM_TAXI_TYPE : "taxi_type_key"
```

## 🏗️ Architecture Overview

- **Fact Table**: `FACT_TAXI_TRIPS` (~50K records)
- **Dimensions**: Date, Location (NYC zones), Taxi Type
- **Technology**: dbt + Snowflake
- **Update Frequency**: Daily

## 📈 Key Metrics

- Trip volume and revenue analysis
- Geographic demand patterns  
- Service type performance
- Data quality monitoring (A/B grade)
