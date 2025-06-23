import snowflake.connector
import os
from pathlib import Path

def upload_csv_to_snowflake():
    """Upload CSV files to Snowflake stage"""
    
    # Connection parameters
    conn_params = {
        'user': 'ABDULWAHAB456',
        'password': 'Abdulwahab7890', 
        'account': 'IAUOIGR-MU26544',  # Your account
        'warehouse': 'COMPUTE_WH',
        'database': 'NYC_TRANSPORTATION_DW',
        'schema': 'RAW_DATA'
    }
    
    # Data directory (look in multiple possible locations)
    possible_data_dirs = [
        Path("C:/Users/Abdul Wahab/Downloads/8th semester/Data Warehouse-Theory/Project Guidelines.docx/Project/data/raw"),
        Path("data/raw"),  # If running from Project directory
        Path("."),        # Current directory
        Path("../data/raw")  # Parent directory
    ]
    
    # CSV files to upload (50K rows each)
    csv_files_to_upload = [
        "yellow_tripdata_2024-01_sample_50000.csv",
        "green_tripdata_2024-01_sample_50000.csv", 
        "fhvhv_tripdata_2024-01_sample_50000.csv"
    ]
    
    # Find the data directory
    data_dir = None
    for possible_dir in possible_data_dirs:
        if possible_dir.exists():
            # Check if any CSV files exist in this directory
            csv_found = any((possible_dir / csv_file).exists() for csv_file in csv_files_to_upload)
            if csv_found:
                data_dir = possible_dir
                break
    
    if not data_dir:
        print(" Could not find CSV data files!")
        print(f"\n Looking for these files:")
        for filename in csv_files_to_upload:
            print(f"   - {filename}")
        print(f"\n Searched in these locations:")
        for possible_dir in possible_data_dirs:
            print(f"   - {possible_dir.absolute()}")
        return
    
    try:
        # Connect to Snowflake
        conn = snowflake.connector.connect(**conn_params)
        cursor = conn.cursor()
        
        print("="*60)
        print(" NYC TAXI DATA - CSV UPLOADER TO SNOWFLAKE")
        print("="*60)
        print(" Connected to Snowflake successfully!")
        print(f" Using data directory: {data_dir.absolute()}")
        
        uploaded_files = []
        
        # Upload each CSV file
        for filename in csv_files_to_upload:
            file_path = data_dir / filename
            if file_path.exists():
                print(f"\n⬆  Uploading {filename}...")
                
                # Check file size
                file_size_mb = file_path.stat().st_size / (1024*1024)
                print(f"    File size: {file_size_mb:.1f} MB")
                
                # Count lines in CSV (approximate record count)
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        line_count = sum(1 for line in f) - 1  # Subtract header
                    print(f"    Estimated records: {line_count:,}")
                except:
                    print(f"    Could not count records")
                
                # Convert path and upload to Snowflake stage
                abs_path_str = str(file_path.absolute()).replace('\\', '/')
                
                upload_sql = f"PUT 'file://{abs_path_str}' @NYC_TLC_STAGE AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
                
                print(f"    Executing upload...")
                result = cursor.execute(upload_sql)
                
                # Get upload results
                upload_results = cursor.fetchall()
                if upload_results:
                    status = upload_results[0][6] if len(upload_results[0]) > 6 else "COMPLETED"
                    print(f"    Status: {status}")
                    if "UPLOADED" in str(status) or "SKIPPED" in str(status):
                        print(f"    {filename} uploaded successfully!")
                        uploaded_files.append(filename)
                    else:
                        print(f"     Upload result: {status}")
                else:
                    print(f"    {filename} upload completed!")
                    uploaded_files.append(filename)
                    
            else:
                print(f"\n    File not found: {file_path}")
                print(f"    Make sure the CSV file exists in: {data_dir}")
        
        print(f"\n" + "="*60)
        print(f" CHECKING FILES IN SNOWFLAKE STAGE")
        print(f"="*60)
        
        # List files in stage
        cursor.execute("LIST @NYC_TLC_STAGE")
        results = cursor.fetchall()
        
        if results:
            print(f"\n  Files currently in Snowflake stage:")
            csv_files_in_stage = []
            for i, result in enumerate(results, 1):
                filename = result[0]
                size_mb = result[1] / (1024 * 1024) if len(result) > 1 else 0
                print(f"   {i}. {filename} ({size_mb:.1f} MB)")
                if filename.endswith('.csv.gz'):
                    csv_files_in_stage.append(filename)
        else:
            print(f"\n    No files found in stage")
            
        print(f"\n" + "="*60)
        print(f" UPLOAD SUMMARY")
        print(f"="*60)
        print(f"    Files uploaded this session: {len(uploaded_files)}")
        print(f"    Total files in stage: {len(results) if results else 0}")
        print(f"    Ready for COPY INTO commands: {'YES' if uploaded_files else 'NO'}")
        print(f"\n🎉 CSV upload process completed!")
            
    except Exception as e:
        print(f"\n❌ Error occurred: {e}")
        
    finally:
        if 'conn' in locals():
            conn.close()
            print(f"\n🔌 Snowflake connection closed")

def create_sample_csv_info():
    """Display information about expected CSV file format"""
    print(f"\n📋 Expected CSV file format:")
    print(f"   - yellow_tripdata_2024-01.csv")
    print(f"   - green_tripdata_2024-01.csv") 
    print(f"   - fhvhv_tripdata_2024-01.csv")
    print(f"\n💡 CSV files should have:")
    print(f"   - Header row with column names")
    print(f"   - Comma-separated values")
    print(f"   - ~50,000 rows each (as you mentioned)")
    print(f"   - UTF-8 encoding")

if __name__ == "__main__":
    print("="*70)
    print("🚀 NYC TAXI DATA - CSV UPLOADER FOR SNOWFLAKE (50K ROWS)")
    print("="*70)
    
    # Show expected file info
    create_sample_csv_info()
    
    # Run the upload
    upload_csv_to_snowflake()
    
    input("\nPress Enter to exit...")