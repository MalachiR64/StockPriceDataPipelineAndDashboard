import os, uuid
from io import StringIO
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from .models import Base, Sector, Industry, Stock
import pyodbc
import pandas as pd


FILES_DIR = os.path.join(os.path.dirname(__file__), '..', 'files')

# Ensure the folder exists
os.makedirs(FILES_DIR, exist_ok=True)

def files_blob_upload(files_to_upload,container_client):
    for file in files_to_upload:
        try:
            # Upload file
            with open(os.path.join(FILES_DIR,file), "rb") as data:
                container_client.upload_blob(name=file, data=data, overwrite=True)
            print(f"Uploaded {file} successfully.")
        except Exception as e:
            print(f"Failed to upload {file}: {e}")

def blob_exists(container_client, blob_name):
    """Check if a blob exists in the container"""
    try:
        blob_client = container_client.get_blob_client(blob_name)
        blob_client.get_blob_properties()
        return True
    except Exception:
        return False
    
def main():
    #name of the files 
    industries_json ='industries.json'
    sectors_json =  'sectors.json'
    stocks_csv =  "stocks.csv"
    sectors_csv = "sectors.csv"
    industries_csv = "industries.csv"
    hist_stocks_csv =  "hist_stocks.csv"

    #connection string method
    ## Azure storage account details
    connect_str_blob = os.getenv("CONNECT_STR_BLOB")
    container_name = os.getenv("CONTAINER_NAME")


    connect_str_sql =  os.getenv("CONNECT_STR_SQL")
    conn = pyodbc.connect(connect_str_sql)
    cursor = conn.cursor()

    def load_csv_to_sql(csv_file, table_name, conn):
        df = pd.read_csv(os.path.join(FILES_DIR,csv_file), encoding='utf-8')
        if (csv_file == 'stocks.csv'):
            for _, row in df.iterrows():
                cursor.execute(f'''
                INSERT INTO {table_name} 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', tuple(row))
        elif (csv_file == 'industries.csv'):
            for _, row in df.iterrows():
                cursor.execute(f'''
                INSERT INTO {table_name} 
                VALUES (?, ?, ?, ?, ?)
                ''', tuple(row))
        elif (csv_file == 'sectors.csv'):
            for _, row in df.iterrows():
                cursor.execute(f'''
                INSERT INTO {table_name} 
                VALUES (?, ?, ?, ?)
                ''', tuple(row))
        else:
            print("file was not read")
        conn.commit()

    #connect to blob storage
    # Initialize BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string(connect_str_blob)
    container_client = blob_service_client.get_container_client(container_name)
    #list of blob files
    blob_list = container_client.list_blobs()
   

    if blob_exists(container_client, hist_stocks_csv):
        print(f"{hist_stocks_csv} exists in blob storage. Downloading and appending new data...")
        try:
            # Download existing historical data from blob
            blob_client = container_client.get_blob_client(hist_stocks_csv)
            existing_hist_df = pd.read_csv(StringIO(blob_client.download_blob().readall().decode('utf-8')))
            
            # Read current historical data from local file
            current_hist_df = pd.read_csv(os.path.join(FILES_DIR, hist_stocks_csv))
            
            # Append current data to existing data
            combined_hist_df = pd.concat([existing_hist_df, current_hist_df], ignore_index=True)

            # Remove any duplicate rows if needed (optional, based on your data)
            combined_hist_df = combined_hist_df.drop_duplicates()

            # Save the combined dataframe back to local CSV file
            combined_hist_df.to_csv(os.path.join(FILES_DIR, hist_stocks_csv), index=False)

            print(f"Successfully combined historical data. Total rows: {len(combined_hist_df)}")   
        except Exception as e:
            print(f"Failed to download and combine {hist_stocks_csv}: {e}")
            print("Using only current data...")
    else:
        print(f"{hist_stocks_csv} does not exist in blob storage. Creating new historical file...")
        # The file already exists locally from the main.py script, so we just use it as is
    # List of files to upload
    files_to_upload_to_blob = [ "industries.csv", "sectors.csv", "stocks.csv","hist_stocks.csv"]
    files_blob_upload(files_to_upload_to_blob,container_client)




    #dropt the tables if they exist 
    cursor.execute('DROP TABLE IF EXISTS [dbo].[Industries]')
    cursor.execute('DROP TABLE IF EXISTS [dbo].[Sectors]')
    cursor.execute('DROP TABLE IF EXISTS [dbo].[Stocks]')

    # to create the sql tables
    #create the tables the   
    cursor.execute('''
    CREATE TABLE Stocks (
        stock_id INT PRIMARY KEY,
        industry_id INT,
        sector_id INT,
        symbol NVARCHAR(50),
        name NVARCHAR(255),
        price FLOAT,
        market_cap BIGINT,
        last_update DATETIME
    )
    ''')

    cursor.execute('''
    CREATE TABLE Industries (
        industry_id INT PRIMARY KEY,
        sector_id INT,
        name NVARCHAR(255),
        total_market_cap BIGINT,
        last_update DATETIME
    )
    ''')

    cursor.execute('''
    CREATE TABLE Sectors (
        sector_id INT PRIMARY KEY,
        name NVARCHAR(255),
        total_market_cap BIGINT,
        last_update DATETIME
    )
    ''')
#
    conn.commit()

    load_csv_to_sql(stocks_csv, "Stocks", conn)
    load_csv_to_sql(industries_csv, "Industries", conn)
    load_csv_to_sql(sectors_csv, "Sectors", conn)

    cursor.close()
    conn.close()
    print("Data loaded successfully!")

    #remove the files 
    removed_files = [stocks_csv,industries_csv,sectors_csv,industries_json,sectors_json,hist_stocks_csv]
    #for file in removed_files:
    #    try:
    #        os.remove(os.path.join(FILES_DIR, file))
    #    except Exception as e:
    #        print(f"Error deleting {file}: {e}")
if __name__ == "__main__":
    main()