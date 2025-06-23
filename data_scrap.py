import pandas as pd
import requests
import os
from datetime import datetime, timedelta
import urllib.parse

class NYCTaxiDataDownloader:
    """
    Download NYC Taxi & Limousine Commission data
    Official source: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
    """
    
    def __init__(self, download_dir="data/raw"):
        self.download_dir = download_dir
        self.base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
        os.makedirs(download_dir, exist_ok=True)
    
    def get_available_datasets(self):
        """List available dataset types"""
        datasets = {
            "yellow_taxi": "Yellow Taxi trips (Manhattan primarily)",
            "green_taxi": "Green Taxi trips (Outer boroughs)",
            "fhv": "For-Hire Vehicle trips (Uber, Lyft, etc.)",
            "fhvhv": "High-Volume For-Hire Vehicle trips"
        }
        return datasets
    
    def download_monthly_data(self, dataset_type="yellow_taxi", year=2024, month=1):
        """
        Download specific month data - FIXED URL FORMATS
        
        Args:
            dataset_type: yellow_taxi, green_taxi, fhv, fhvhv  
            year: 2024, 2023, etc.
            month: 1-12
        """
        # Format month with leading zero
        month_str = f"{month:02d}"
        
        # CORRECTED filename formats based on successful FHV download
        if dataset_type == "yellow_taxi":
            filename = f"yellow_tripdata_{year}-{month_str}.parquet"
        elif dataset_type == "green_taxi":
            filename = f"green_tripdata_{year}-{month_str}.parquet"
        elif dataset_type == "fhv":
            filename = f"fhv_tripdata_{year}-{month_str}.parquet"
        elif dataset_type == "fhvhv":
            filename = f"fhvhv_tripdata_{year}-{month_str}.parquet"
        else:
            raise ValueError(f"Unknown dataset type: {dataset_type}")
        
        # Try multiple base URLs since some may be restricted
        base_urls_to_try = [
            "https://d37ci6vzurychx.cloudfront.net/trip-data",
            "https://nyc-tlc.s3.amazonaws.com/trip+data", 
            "https://s3.amazonaws.com/nyc-tlc/trip+data"
        ]
        
        
        # Try each base URL until one works
        for base_url in base_urls_to_try:
            url = f"{base_url}/{filename}"
            local_path = os.path.join(self.download_dir, filename)
            
            print(f"Trying: {url}")
            
            try:
                # First check if URL exists with HEAD request
                head_response = requests.head(url, timeout=10)
                if head_response.status_code != 200:
                    print(f"❌ Head check failed: Status {head_response.status_code}")
                    continue
                    
                # If HEAD successful, proceed with download
                response = requests.get(url, stream=True, timeout=30)
                response.raise_for_status()
                
                # Download with progress
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                
                with open(local_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            if total_size > 0:
                                percent = (downloaded / total_size) * 100
                                print(f"\rProgress: {percent:.1f}%", end="")
                
                print(f"\nDownloaded: {local_path}")
                return local_path
                
            except requests.exceptions.RequestException as e:
                print(f"Failed with {base_url}: {e}")
                continue
            except Exception as e:
                print(f"Unexpected error with {base_url}: {e}")
                continue
        
        print(f"All URLs failed for {filename}")
        return None
    
    def download_sample_datasets(self):
        """Download a good sample for the project - TRY MULTIPLE TIME PERIODS"""
        print("🎯 Attempting to download all three taxi types...")
        
        # Try multiple time periods in case some months aren't available
        time_periods_to_try = [
            (2024, 1),
            (2023, 12),
            (2023, 11),
            (2023, 10),
            (2024, 2),
        ]
        
        datasets_needed = ["yellow_taxi", "green_taxi", "fhvhv"]
        downloaded_files = []
        successful_downloads = {}
        
        # Try to get each dataset type
        for dataset_type in datasets_needed:
            print(f"\n🔍 Searching for {dataset_type} data...")
            
            success = False
            for year, month in time_periods_to_try:
                print(f"\n   Trying {dataset_type} for {year}-{month:02d}...")
                
                file_path = self.download_monthly_data(dataset_type, year, month)
                if file_path:
                    downloaded_files.append(file_path)
                    successful_downloads[dataset_type] = (year, month, file_path)
                    print(f"   SUCCESS: Got {dataset_type} data!")
                    success = True
                    break
                else:
                    print(f"   Failed for {year}-{month:02d}")
            
            if not success:
                print(f"   Could not find {dataset_type} data for any time period")
        
        # Summary of what we got
        print(f"\nDOWNLOAD SUMMARY:")
        print("=" * 50)
        
        for dataset_type in datasets_needed:
            if dataset_type in successful_downloads:
                year, month, filepath = successful_downloads[dataset_type]
                print(f"{dataset_type:12} -> {year}-{month:02d} ({filepath})")
            else:
                print(f"{dataset_type:12} -> Failed to download")
        
        print(f"\nTotal files downloaded: {len(downloaded_files)}")
        
        if len(downloaded_files) == 0:
            print("No files downloaded successfully")
        elif len(downloaded_files) < 3:
            print("Partial success - some taxi types missing")
        else:
            print("Perfect! All three taxi types downloaded successfully!")
        
        return downloaded_files
    
    def validate_downloaded_files(self, downloaded_files):
        """Validate that downloaded files are correct taxi data"""
        print(f"\n VALIDATING DOWNLOADED FILES")
        print("=" * 50)
        
        validation_results = []
        
        for filepath in downloaded_files:
            filename = os.path.basename(filepath)
            print(f"\n📋 Validating: {filename}")
            
            try:
                # Try to load the file and check structure - FIXED: parquet doesn't have nrows
                df = pd.read_parquet(filepath)
                df = df.head(100)  # Take first 100 rows for validation
                
                # Determine taxi type from filename
                if "yellow" in filename:
                    taxi_type = "Yellow Taxi"
                    expected_cols = ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_distance']
                elif "green" in filename:
                    taxi_type = "Green Taxi"  
                    expected_cols = ['VendorID', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'trip_distance']
                elif "fhvhv" in filename:
                    taxi_type = "FHV (Uber/Lyft)"
                    expected_cols = ['hvfhs_license_num', 'pickup_datetime', 'dropoff_datetime', 'trip_miles']
                else:
                    taxi_type = "Unknown"
                    expected_cols = []
                
                # Check if key columns exist
                missing_cols = [col for col in expected_cols if col not in df.columns]
                
                validation_info = {
                    'filename': filename,
                    'taxi_type': taxi_type,
                    'total_rows': len(df) if len(df) < 1000 else "1M+ (large file)",  # Avoid reloading
                    'columns': len(df.columns),
                    'sample_columns': list(df.columns)[:8],  # First 8 columns
                    'missing_expected': missing_cols,
                    'status': '✅ Valid' if len(missing_cols) == 0 else '⚠️ Check columns'
                }
                
                print(f"   Type: {taxi_type}")
                print(f"   Rows: {validation_info['total_rows']:,}")
                print(f"   Columns: {validation_info['columns']}")
                print(f"   Sample columns: {', '.join(validation_info['sample_columns'])}")
                print(f"   Status: {validation_info['status']}")
                
                if missing_cols:
                    print(f"   Missing expected: {missing_cols}")
                
                validation_results.append(validation_info)
                
            except Exception as e:
                print(f"   Validation failed: {e}")
                validation_results.append({
                    'filename': filename,
                    'status': 'Invalid',
                    'error': str(e)
                })
        
        return validation_results
    
    def convert_to_csv(self, parquet_file, sample_size=50000):
        """Convert parquet file to CSV with sampling - RESTORED METHOD"""
        try:
            print(f"Converting {os.path.basename(parquet_file)} to CSV...")
            
            # Load the parquet file
            df = pd.read_parquet(parquet_file)
            original_rows = len(df)
            
            print(f"Original dataset shape: {df.shape}")
            
            # Sample if dataset is too large
            if len(df) > sample_size:
                df = df.sample(n=sample_size, random_state=42)
                print(f"Sampled {sample_size} rows")
            
            # Create CSV filename
            parquet_name = os.path.basename(parquet_file)
            csv_name = parquet_name.replace('.parquet', f'_sample_{len(df)}.csv')
            csv_path = os.path.join(self.download_dir, csv_name)
            
            # Save to CSV
            df.to_csv(csv_path, index=False)
            print(f"CSV saved: {csv_path}")
            
            # Show basic info
            print(f"\n Dataset Info:")
            print(f"Columns: {list(df.columns)}")
            print(f"Date range: {df.iloc[0, 0]} to {df.iloc[0, 1]}" if len(df.columns) >= 2 else "N/A")
            print(f"Total records: {original_rows:,}")
            
            return csv_path
            
        except Exception as e:
            print(f"Error converting {parquet_file}: {e}")
            return None

def explore_taxi_data(csv_file, sample_size=1000):
    """Quick exploration of taxi data - RESTORED FUNCTION"""
    print(f"\n🔍 Exploring {csv_file}")
    
    try:
        # Load sample of the data
        df = pd.read_csv(csv_file, nrows=sample_size)
        
        print(f"Dataset shape: {df.shape}")
        print(f"\nColumns:")
        for i, col in enumerate(df.columns, 1):
            print(f"{i}. {col}")
        
        print(f"\nFirst few rows:")
        print(df.head())
        
        print(f"\nBasic statistics:")
        print(df.describe())
        
        return df
        
    except Exception as e:
        print(f" Error exploring data: {e}")
        return None
        """
        Convert parquet to CSV and create a sample
        
        Args:
            parquet_file: Path to parquet file
            sample_size: Number of rows to sample (None for full dataset)
        """
        try:
            print(f"Converting {parquet_file} to CSV...")
            
            # Read parquet file
            df = pd.read_parquet(parquet_file)
            print(f"Original dataset shape: {df.shape}")
            
            # Create sample if requested
            if sample_size and len(df) > sample_size:
                df_sample = df.sample(n=sample_size, random_state=42)
                print(f"Sampled {sample_size} rows")
            else:
                df_sample = df
            
            # Generate CSV filename
            csv_filename = parquet_file.replace('.parquet', '.csv')
            if sample_size:
                csv_filename = csv_filename.replace('.csv', f'_sample_{sample_size}.csv')
            
            # Save to CSV
            df_sample.to_csv(csv_filename, index=False)
            print(f"✅ CSV saved: {csv_filename}")
            
            # Display info about the dataset
            print(f"\n📊 Dataset Info:")
            print(f"Columns: {list(df.columns)}")
            print(f"Date range: {df.iloc[:,1].min()} to {df.iloc[:,1].max()}")
            print(f"Total records: {len(df):,}")
            
            return csv_filename
            
        except Exception as e:
            print(f" Error converting to CSV: {e}")
            return None

def explore_taxi_data(csv_file):
    """Quick data exploration"""
    print(f"\n🔍 Exploring {csv_file}")
    
    df = pd.read_csv(csv_file, nrows=1000)  # Load first 1000 rows for quick exploration
    
    print(f"Dataset shape: {df.shape}")
    print(f"\nColumns:")
    for i, col in enumerate(df.columns):
        print(f"{i+1}. {col}")
    
    print(f"\nFirst few rows:")
    print(df.head())
    
    print(f"\nBasic statistics:")
    print(df.describe())
    
    return df

# Main execution - IMPROVED VERSION
if __name__ == "__main__":
    print(" NYC TAXI DATA DOWNLOADER - FIXED VERSION")
    print("=" * 60)
    
    # Initialize downloader
    downloader = NYCTaxiDataDownloader()
    
    # Show available datasets
    print("📋 Target Datasets:")
    datasets_info = {
        "yellow_taxi": " Traditional NYC Yellow Taxis (Manhattan focus)",
        "green_taxi": " Green Boro Taxis (Outer boroughs)", 
        "fhvhv": " For-Hire Vehicles (Uber, Lyft, Via)"
    }
    
    for key, description in datasets_info.items():
        print(f"   • {key}: {description}")
    
    print(f"\n Goal: Download all 3 taxi types for comprehensive analysis")
    print(f" Will try multiple time periods to find available data")
    
    # Download datasets with improved error handling
    print(f"\n⬇ Starting download process...")
    downloaded_files = downloader.download_sample_datasets()
    
    if downloaded_files:
        # Validate downloaded files
        validation_results = downloader.validate_downloaded_files(downloaded_files)
        
        # Convert to CSV with samples
        print(f"\n Converting to CSV format...")
        csv_files = []
        for parquet_file in downloaded_files:
            print(f"\nProcessing: {os.path.basename(parquet_file)}")
            csv_file = downloader.convert_to_csv(parquet_file, sample_size=50000)
            if csv_file:
                csv_files.append(csv_file)
        
        # Final summary
        print(f"\n🎉 DOWNLOAD COMPLETE!")
        print("=" * 50)
        print(f" Parquet files: {len(downloaded_files)}")
        print(f" CSV files: {len(csv_files)}")
        print(f" Location: {downloader.download_dir}")
        
        # Quick data exploration
        if csv_files:
            print(f"\n🔍 Quick Data Preview:")
            explore_taxi_data(csv_files[0])
        
        if len(csv_files) >= 2:
            print(f"\n SUCCESS: You have enough data for your capstone project!")
            print(f" Next steps:")
            print(f"   1. Review your CSV files")
            print(f"   2. Set up Snowflake database") 
            print(f"   3. Load data into Snowflake")
            print(f"   4. Build dbt models")
        else:
            print(f"\n Partial success - consider using synthetic data to supplement")
    
    else:
        print(f"\n No files downloaded successfully")
        
    print(f"\n📋 Check your 'data/raw' directory for downloaded files")