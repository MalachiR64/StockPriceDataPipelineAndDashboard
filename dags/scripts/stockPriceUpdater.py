import os, uuid
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from .models import Base, Sector, Industry, Stock
import yfinance as yf
import pyodbc
import pandas as pd
import time
from .uploadToAzureBlobAndSQL import files_blob_upload
import logging


# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

FILES_DIR = os.path.join(os.path.dirname(__file__), '..', 'files')


# Ensure the folder exists
os.makedirs(FILES_DIR, exist_ok=True)

def file_remover(removed_files):
    """Remove files after processing"""
    for file in removed_files:
        file_path = os.path.join(FILES_DIR, file)
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"Removed file: {file_path}")
        else:
            logger.warning(f"File not found for removal: {file_path}")
            
def file_downloader(downloaded_files, connect_str_blob,container_name):
    blob_service_client = BlobServiceClient.from_connection_string(connect_str_blob)
    container_client = blob_service_client.get_container_client(container_name)
    for file in downloaded_files:
        try:
            blob_client = container_client.get_blob_client(file)
    
            with open(os.path.join(FILES_DIR,file), "wb") as file:
                file.write(blob_client.download_blob().readall())
    
            print(f"Downloaded: {file}")   
        except Exception as e:
            print(f"Failed to download {file}: {e}")
def updated_stock_data(ticker):
    try: 
        formatted_ticker = ticker.replace(".", "-")  # Replace "." with "-"
        stock = yf.Ticker(formatted_ticker)
        info = stock.info
        time.sleep(1)
        return {
        "stock_price" : info.get('currentPrice', "NULL"),
        "stock_market_cap" : info.get('marketCap', "NULL")
        }
    except Exception as e:
        print(f"Error fetching data for {formatted_ticker}: {e}")
        return None 

def main():
    #files names 

    #input your connection strings
    connect_str_blob = os.getenv("CONNECT_STR_BLOB")
    container_name = os.getenv("CONTAINER_NAME")
    blob_service_client = BlobServiceClient.from_connection_string(connect_str_blob)    
    container_client = blob_service_client.get_container_client(container_name)
    # Open a database session
    connect_str_sql = os.getenv("CONNECT_STR_SQL")
    conn = pyodbc.connect(connect_str_sql)
    cursor = conn.cursor()

    blob_files = ["stocks.csv","industries.csv","sectors.csv", "hist_stocks.csv"]
    #download the files
    file_downloader(blob_files,connect_str_blob,container_name)

    #interate through the stocks
    df_stocks = pd.read_csv(os.path.join(FILES_DIR,blob_files[0]))
    df_industries = pd.read_csv(os.path.join(FILES_DIR,blob_files[1]))
    df_sectors = pd.read_csv(os.path.join(FILES_DIR,blob_files[2]))
        
    # Load historical stock data if it exists
    hist_file_path = os.path.join(FILES_DIR, blob_files[3])
    hist_exists = os.path.exists(hist_file_path)
    
    if hist_exists:
        try:
            df_hist_stocks = pd.read_csv(hist_file_path)
            logger.info(f"Loaded historical stock data with {len(df_hist_stocks)} records")
        except Exception as e:
            logger.error(f"Error loading historical stock data: {e}")
            hist_exists = False
            df_hist_stocks = pd.DataFrame()
    else:
        logger.info("No existing historical stock data found")
        df_hist_stocks = pd.DataFrame()

    #update the stock data
    update_stock_data = []
    cols_to_append = ["stock_symbol", "stock_name", "stock_price", "market_cap", "last_updated"]
    new_hist_records = []
    for index, row in df_stocks.iterrows():
        new_stock_info = updated_stock_data(row.stock_symbol)
        new_stock_time = datetime.today().replace(microsecond=0)
        df_stocks.at[index, "stock_price"] = new_stock_info["stock_price"]
        df_stocks.at[index, "market_cap"] = new_stock_info["stock_market_cap"]
        df_stocks.at[index, "last_updated"] = new_stock_time
        #put it into the sql

        # Add SQL update query to batch
        update_stock_data.append(
        (
            new_stock_info["stock_price"],
            new_stock_info["stock_market_cap"],
            df_stocks.at[index, "last_updated"],
            row["stock_id"]
        )
        )
        # Add to historical records
        new_hist_records.append({
            'stock_symbol': row["stock_symbol"],
            'stock_name': row["stock_name"],
            'stock_price': new_stock_info["stock_price"],
            'market_cap': new_stock_info["stock_market_cap"],
            'last_updated': new_stock_time
        })        
    #update the stock sql and csv file
    update_stock_query = """
        UPDATE stocks 
        SET price = ?, market_cap = ?, last_update = ?
        WHERE stock_id = ?
    """
    cursor.executemany(update_stock_query,update_stock_data)
    conn.commit()

    #write to stocks csv
    df_stocks.to_csv(os.path.join(FILES_DIR, blob_files[0]), index=False)
    
    # Convert new historical records to DataFrame and append to existing data
    df_new_hist = pd.DataFrame(new_hist_records)
    
    if hist_exists and not df_hist_stocks.empty:
        df_hist_stocks = pd.concat([df_hist_stocks, df_new_hist], ignore_index=True)
        logger.info(f"Appended {len(df_new_hist)} new records to historical data")
    else:
        df_hist_stocks = df_new_hist
        logger.info(f"Created new historical data with {len(df_new_hist)} records")
    
    # Save the combined historical data
    df_hist_stocks.to_csv(hist_file_path, index=False)    


    #update industries
    update_industry_data = []
    for index, row in df_industries.iterrows():
        new_industry_market_cap =int( df_stocks.loc[df_stocks['industry_id'] == row.industry_id, 'market_cap'].sum())
        new_industry_time = datetime.today().replace(microsecond=0)
        df_industries.at[index, "market_cap"] = new_industry_market_cap
        df_industries.at[index, "last_updated"] = new_industry_time
        update_industry_data.append((new_industry_market_cap,new_industry_time, row["industry_id"]))

    #update the industry sql and csv files
    update_industry_query = """
        UPDATE industries
        SET total_market_cap = ?, last_update = ?
        WHERE industry_id = ?
    """
    cursor.executemany(update_industry_query,update_industry_data)
    conn.commit()
    #write to industries csv
    df_industries.to_csv(os.path.join(FILES_DIR, blob_files[1]), index=False)
    
    #update sector
    update_sector_data = []
    for index, row in df_sectors.iterrows():
        new_sector_market_cap =int( df_industries.loc[df_industries['sector_id'] == row.sector_id, 'market_cap'].sum())
        new_sector_time = datetime.today().replace(microsecond=0)
        df_sectors.at[index, "market_cap"] = new_sector_market_cap
        df_sectors.at[index, "last_updated"] = new_sector_time
        update_sector_data.append((new_sector_market_cap,new_sector_time, row["sector_id"]))

    update_sector_query = """
        UPDATE sectors
        SET total_market_cap = ?, last_update = ?
        WHERE sector_id = ?
    """
    cursor.executemany(update_sector_query,update_sector_data)
    conn.commit()
    #write to sectors csv
    df_sectors.to_csv(os.path.join(FILES_DIR, blob_files[2]), index=False)
   
    # Close SQL connection
    cursor.close()
    conn.close()

    files_blob_upload(blob_files,container_client)
    print("Stock data updated in Azure SQL and CSV successfully!")
    #remove files
    #file_remover(blob_files)
if __name__ == "__main__":
    main()