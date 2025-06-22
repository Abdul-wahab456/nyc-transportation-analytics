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
