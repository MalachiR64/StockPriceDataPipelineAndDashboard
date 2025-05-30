import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import os
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from io import StringIO
import time

# Page configuration
st.set_page_config(
    page_title="S&P 500 Stocks Dashboard",
    page_icon="📈",
    layout="wide"
)

# UI Header
st.title("📊 S&P 500 Stocks Dashboard")
st.markdown("### End to End ETL Data Pipeline for all the stocks in the s&P 500")

# Connect to Azure Blob Storage
@st.cache_resource
def get_blob_service_client():
    try:
        # Try using connection string if available
        connect_str = os.getenv("CONNECT_STR_BLOB")
        if connect_str:
            blob_service_client = BlobServiceClient.from_connection_string(connect_str)
            return blob_service_client
    except Exception as e:
        st.error(f"Failed to connect to Azure Blob Storage: {e}")
        return None

# Load data from Azure Blob
@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_data_from_azure(container_name, blob_name):
    try:
        blob_service_client = get_blob_service_client()
        if not blob_service_client:
            return None
            
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        
        # Download blob content
        blob_data = blob_client.download_blob()
        
        # Read CSV directly from the downloaded stream
        return pd.read_csv(StringIO(blob_data.content_as_text()))
    except Exception as e:
        st.warning(f"Could not load {blob_name} from Azure: {e}")
        return None

# Add error handling for container name
def get_container_name():
    container_name = os.getenv("CONTAINER_NAME", "stockmarket")  # Default to "stockmarket" if not set
    return container_name

# Functions to load data (with improved error handling)
@st.cache_data(ttl=300)
def load_stocks_data():
    """Load stocks data with fallback to sample data"""
    container_name = get_container_name()
    df = load_data_from_azure(container_name, "stocks.csv")
    if df is None:
        st.warning("Failed to load stocks data from Azure. Using sample data instead.")
        df = pd.DataFrame({
            'stock_id': [1, 2, 3, 4, 5],
            'industry_id': [1, 2, 1, 3, 2],
            'sector_id': [1, 1, 1, 2, 1],
            'stock_symbol': ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META'],
            'stock_name': ['Apple Inc.', 'Microsoft Corp', 'Alphabet Inc.', 'Amazon.com Inc.', 'Meta Platforms Inc.'],
            'stock_price': [150.25, 320.45, 2800.75, 3200.10, 325.60],
            'market_cap': [2500000000000, 2400000000000, 1800000000000, 1600000000000, 900000000000],
            'last_updated': ['2025-05-17'] * 5
        })
    return df

@st.cache_data(ttl=300)
def load_industries_data():
    """Load industries data with fallback to sample data"""
    container_name = get_container_name()
    df = load_data_from_azure(container_name, "industries.csv")
    if df is None:
        st.warning("Failed to load industries data from Azure. Using sample data instead.")
        df = pd.DataFrame({
            'industry_id': [1, 2, 3],
            'sector_id': [1, 1, 2],
            'industry_name': ['Technology Services', 'Software', 'E-commerce'],
            'market_cap': [5000000000000, 4000000000000, 2500000000000],
            'last_updated': ['2025-05-17'] * 3
        })
    return df

@st.cache_data(ttl=300)
def load_sectors_data():
    """Load sectors data with fallback to sample data"""
    container_name = get_container_name()
    df = load_data_from_azure(container_name, "sectors.csv")
    if df is None:
        st.warning("Failed to load sectors data from Azure. Using sample data instead.")
        df = pd.DataFrame({
            'sector_id': [1, 2, 3],
            'sector_name': ['Technology', 'Consumer Cyclical', 'Healthcare'],
            'market_cap': [10000000000000, 5000000000000, 4000000000000],
            'last_updated': ['2025-05-17'] * 3
        })
    return df

@st.cache_data(ttl=300)
def load_historical_stocks_data():
    """Load historical stocks data with fallback to sample data"""
    container_name = get_container_name()
    df = load_data_from_azure(container_name, "hist_stocks.csv")
    if df is None:
        st.warning("Failed to load historical data from Azure. Using sample data instead.")
        # Create sample historical data as fallback
        symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META']
        dates = pd.date_range(end=pd.Timestamp.now(), periods=30)
        data = []
        
        for symbol in symbols:
            base_price = {'AAPL': 150, 'MSFT': 320, 'GOOGL': 2800, 'AMZN': 3200, 'META': 325}[symbol]
            for date in dates:
                price = base_price * (1 + (0.1 * (pd.Timestamp.now() - date).days / 30)) * (1 + 0.02 * (np.random.random() - 0.5))
                market_cap = price * {'AAPL': 16.5, 'MSFT': 7.5, 'GOOGL': 0.65, 'AMZN': 0.5, 'META': 2.75}[symbol] * 1_000_000_000
                
                data.append({
                    'stock_symbol': symbol,
                    'stock_name': {'AAPL': 'Apple Inc.', 'MSFT': 'Microsoft Corp', 'GOOGL': 'Alphabet Inc.', 
                                'AMZN': 'Amazon.com Inc.', 'META': 'Meta Platforms Inc.'}[symbol],
                    'stock_price': round(price, 2),
                    'market_cap': int(market_cap),
                    'last_updated': date.strftime('%Y-%m-%d')
                })
        
        df = pd.DataFrame(data)
    
    # Ensure date column is datetime
    if 'last_updated' in df.columns:
        df['last_updated'] = pd.to_datetime(df['last_updated'])
    
    return df

# Load data
with st.spinner("Loading data from Azure..."):
    stocks_df = load_stocks_data()
    industries_df = load_industries_data()
    sectors_df = load_sectors_data()
    hist_stocks_df = load_historical_stocks_data()

# Format the market cap values for display
def format_market_cap(value):
    """Format market cap in trillions, billions, or millions"""
    if value >= 1_000_000_000_000:
        return f"${value/1_000_000_000_000:.2f}T"
    elif value >= 1_000_000_000:
        return f"${value/1_000_000_000:.2f}B"
    elif value >= 1_000_000:
        return f"${value/1_000_000:.2f}M"
    else:
        return f"${value:,.0f}"

# Apply formatting to market cap columns
stocks_display = stocks_df.copy()
industries_display = industries_df.copy()
sectors_display = sectors_df.copy()

if 'market_cap' in stocks_display.columns:
    stocks_display['market_cap'] = stocks_display['market_cap'].apply(format_market_cap)
if 'market_cap' in industries_display.columns:
    industries_display['market_cap'] = industries_display['market_cap'].apply(format_market_cap)
if 'market_cap' in sectors_display.columns:
    sectors_display['market_cap'] = sectors_display['market_cap'].apply(format_market_cap)

# Dashboard Layout
tab1, tab2, tab3, tab4 = st.tabs(["Market Overview", "Stocks", "Industries & Sectors", "Historical Performance"])

with tab1:
    st.header("Market Overview")
    
    # Summary metrics
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            label="Total Stocks", 
            value=len(stocks_df),
            delta=None
        )
    
    with col2:
        total_market_cap = stocks_df['market_cap'].sum() if 'market_cap' in stocks_df.columns else 0
        st.metric(
            label="Total Market Cap", 
            value=format_market_cap(total_market_cap),
            delta=None
        )
    
    with col3:
        avg_price = stocks_df['stock_price'].mean() if 'stock_price' in stocks_df.columns else 0
        st.metric(
            label="Average Stock Price", 
            value=f"${avg_price:.2f}",
            delta=None
        )
    
    # Stock distribution by sector
    st.subheader("Market Cap Distribution by Sector")
    
    # Create a merged dataframe for the pie chart
    if not stocks_df.empty and not sectors_df.empty and 'sector_id' in stocks_df.columns:
        sector_distribution = stocks_df.groupby('sector_id')['market_cap'].sum().reset_index()
        sector_distribution = sector_distribution.merge(
            sectors_df[['sector_id', 'sector_name']], 
            on='sector_id', 
            how='left'
        )
        
        fig = px.pie(
            sector_distribution, 
            values='market_cap', 
            names='sector_name',
            title='Market Cap Distribution by Sector',
            color_discrete_sequence=px.colors.qualitative.Bold
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Not enough data to display sector distribution")

with tab2:
    st.header("Stocks")
    
    # Search and filters
    search_col1, search_col2 = st.columns([1, 3])
    
    with search_col1:
        search_term = st.text_input("Search stocks", placeholder="Enter stock symbol or name")
    
    with search_col2:
        # Get unique sectors and industries for filtering
        if 'sector_id' in stocks_df.columns and not sectors_df.empty:
            sectors_map = dict(zip(sectors_df['sector_id'], sectors_df['sector_name']))
            sector_filter = st.multiselect(
                "Filter by Sector",
                options=list(sectors_map.values()),
                default=[]
            )
        else:
            sector_filter = []
    
    # Filter the dataframe
    filtered_stocks = stocks_df.copy()
    
    if search_term:
        filtered_stocks = filtered_stocks[
            filtered_stocks['stock_symbol'].str.contains(search_term, case=False) | 
            filtered_stocks['stock_name'].str.contains(search_term, case=False)
        ]
    
    if sector_filter and 'sector_id' in filtered_stocks.columns:
        sector_ids = [k for k, v in sectors_map.items() if v in sector_filter]
        filtered_stocks = filtered_stocks[filtered_stocks['sector_id'].isin(sector_ids)]
    
    # Display filtered stocks
    if not filtered_stocks.empty:
        # Join with sector and industry names for display
        display_df = filtered_stocks.copy()
        
        if 'sector_id' in display_df.columns and not sectors_df.empty:
            display_df = display_df.merge(
                sectors_df[['sector_id', 'sector_name']], 
                on='sector_id', 
                how='left'
            )
        
        if 'industry_id' in display_df.columns and not industries_df.empty:
            display_df = display_df.merge(
                industries_df[['industry_id', 'industry_name']], 
                on='industry_id', 
                how='left'
            )
        
        # Select and order columns for display
        display_columns = ['stock_symbol', 'stock_name', 'stock_price', 'market_cap']
        if 'sector_name' in display_df.columns:
            display_columns.append('sector_name')
        if 'industry_name' in display_df.columns:
            display_columns.append('industry_name')
        if 'last_updated' in display_df.columns:
            display_columns.append('last_updated')
        
        # Get only the columns that exist in the dataframe
        display_columns = [col for col in display_columns if col in display_df.columns]
        
        # Format the market cap for display
        if 'market_cap' in display_df.columns:
            display_df['market_cap'] = display_df['market_cap'].apply(format_market_cap)
        
        st.dataframe(
            display_df[display_columns],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No stocks match your search criteria.")
    


with tab3:
    st.header("Industries & Sectors Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Industries Data")
        if not industries_df.empty:
            # Merge industries with sectors to get sector names
            industries_display = industries_df.copy()
            if not sectors_df.empty:
                industries_display = industries_display.merge(
                    sectors_df[['sector_id', 'sector_name']], 
                    on='sector_id', 
                    how='left'
                )
                # Drop sector_id column and format market cap
                display_columns = [col for col in industries_display.columns if col != 'sector_id']
                industries_display = industries_display[display_columns]
            st.dataframe(industries_display, use_container_width=True, hide_index=True)
        else:
            st.info("No industries data available")
    
    with col2:
        st.subheader("Sectors Data")
        if not sectors_df.empty:
            st.dataframe(sectors_display, use_container_width=True, hide_index=True)
        else:
            st.info("No sectors data available")
    


with tab4:
    st.header("Historical Stock Performance")
    
    # Stock symbol selector
    available_symbols = sorted(hist_stocks_df['stock_symbol'].unique()) if not hist_stocks_df.empty else []
    
    if available_symbols:
        selected_symbols = st.multiselect(
            "Select Stock Symbols",
            options=available_symbols,
            default=[available_symbols[0]] if available_symbols else []
        )
        
        date_range = st.date_input(
            "Select Date Range",
            value=(
                hist_stocks_df['last_updated'].min().date(),
                hist_stocks_df['last_updated'].max().date()
            ) if not hist_stocks_df.empty and 'last_updated' in hist_stocks_df.columns else (None, None),
            key="date_range"
        )
        
        if selected_symbols:
            # Filter data by selected stocks and date range
            filtered_hist = hist_stocks_df[hist_stocks_df['stock_symbol'].isin(selected_symbols)]
            
            if len(date_range) == 2:
                start_date, end_date = date_range
                filtered_hist = filtered_hist[
                    (filtered_hist['last_updated'].dt.date >= start_date) &
                    (filtered_hist['last_updated'].dt.date <= end_date)
                ]
            
            # Create time series chart
            fig = px.line(
                filtered_hist,
                x='last_updated',
                y='stock_price',
                color='stock_symbol',
                title="Historical Stock Price Performance",
                labels={
                    'last_updated': 'Date',
                    'stock_price': 'Stock Price ($)',
                    'stock_symbol': 'Stock Symbol'
                }
            )
            
            # Improve the layout
            fig.update_layout(
                legend=dict(
                    yanchor="top",
                    y=0.99,
                    xanchor="left",
                    x=0.01
                ),
                hovermode="x unified"
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Show percentage change table
            if not filtered_hist.empty:
                st.subheader("Performance Metrics")
                
                # Calculate metrics for each stock
                metrics_data = []
                
                for symbol in selected_symbols:
                    symbol_data = filtered_hist[filtered_hist['stock_symbol'] == symbol]
                    if len(symbol_data) > 1:
                        first_price = symbol_data.iloc[0]['stock_price']
                        last_price = symbol_data.iloc[-1]['stock_price']
                        min_price = symbol_data['stock_price'].min()
                        max_price = symbol_data['stock_price'].max()
                        
                        pct_change = ((last_price - first_price) / first_price) * 100
                        
                        metrics_data.append({
                            'Stock Symbol': symbol,
                            'First Price': f"${first_price:.2f}",
                            'Last Price': f"${last_price:.2f}",
                            'Min Price': f"${min_price:.2f}",
                            'Max Price': f"${max_price:.2f}",
                            'Change %': f"{pct_change:.2f}%"
                        })
                
                if metrics_data:
                    metrics_df = pd.DataFrame(metrics_data)
                    st.dataframe(metrics_df, use_container_width=True, hide_index=True)
                
                # Show raw data option
                if st.checkbox("Show Raw Data"):
                    st.dataframe(
                        filtered_hist.sort_values(['stock_symbol', 'last_updated']),
                        use_container_width=True
                    )
        else:
            st.info("Please select at least one stock symbol to view historical performance.")
    else:
        st.info("No historical stock data available.")

# Footer with refresh button and last updated timestamp
st.divider()
col1, col2 = st.columns([3, 1])

with col1:
    st.caption(f"Last updated: {time.strftime('%Y-%m-%d %H:%M:%S')}")

with col2:
    if st.button("Refresh Data"):
        st.cache_data.clear()
        st.experimental_rerun()