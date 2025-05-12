import plotly.express as px
import pandas as pd
import os
from django.shortcuts import render
from django.conf import settings
from django.core.cache import cache
from datetime import datetime
import logging
import numpy as np

# Set up logging
logger = logging.getLogger(__name__)

# Constants
CACHE_TIMEOUT = 60 * 60 * 24  # 24 hours cache
MAX_SAMPLE_SIZE = 100000  # Maximum rows to process
TRANSACTION_FILE = os.path.join(settings.BASE_DIR, 'data', "bank_transactions.csv")

def get_optimized_data():
    """
    Load and process data with caching and memory optimization
    Returns: Processed DataFrame or None if error occurs
    """
    try:
        # Try to get cached data first
        cached_data = cache.get('optimized_transaction_data')
        if cached_data is not None:
            return cached_data

        # Define columns we actually use to reduce memory
        usecols = [
            'TransactionAmount (INR)', 'CustAccountBalance', 'TransactionDate',
            'TransactionTime', 'CustomerID', 'CustLocation', 'TransactionID',
            'CustAge'  # Optional age field
        ]

        # Load data with optimized parameters
        df = pd.read_csv(
            TRANSACTION_FILE,
            usecols=lambda col: col in usecols,
            parse_dates=['TransactionDate'],
            infer_datetime_format=True
        )

        # Rename columns for consistency
        df.rename(columns={
            'TransactionAmount (INR)': 'TransactionAmount',
            'CustAccountBalance': 'AccountBalance'
        }, inplace=True)

        # Downcast numeric columns to reduce memory
        numeric_cols = ['TransactionAmount', 'AccountBalance']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, downcast='float')

        # Create derived columns
        df['TransactionMonth'] = df['TransactionDate'].dt.to_period('M').astype(str)
        df['TransactionDayOfWeek'] = df['TransactionDate'].dt.dayofweek.astype('int8')
        df['TransactionHour'] = df['TransactionTime'].str.split(':').str[0].astype('int8')
        df['AccountBalanceChange'] = df.groupby('CustomerID')['AccountBalance'].diff().fillna(0)

        # Sample data if too large
        if len(df) > MAX_SAMPLE_SIZE:
            df = df.sample(MAX_SAMPLE_SIZE, random_state=42)
            logger.info(f"Sampled data to {MAX_SAMPLE_SIZE} rows")

        # Cache the processed data
        cache.set('optimized_transaction_data', df, timeout=CACHE_TIMEOUT)
        return df

    except Exception as e:
        logger.error(f"Error loading transaction data: {str(e)}")
        return None

def create_figure(fig_func, df, *args, **kwargs):
    """
    Helper function to create and optimize plotly figures
    """
    try:
        fig = fig_func(df, *args, **kwargs)
        fig.update_layout(
            plot_bgcolor='white',
            margin=dict(l=20, r=20, t=40, b=20),
            height=400
        )
        return fig.to_html(full_html=False, include_plotlyjs=False)
    except Exception as e:
        logger.error(f"Error creating figure: {str(e)}")
        return "<div class='error'>Could not generate visualization</div>"

# Visualization creation functions
def create_top_locations_fig(df):
    top_locations = df.groupby('CustLocation')['TransactionAmount'].sum().nlargest(20).reset_index()
    return px.bar(
        top_locations,
        x='TransactionAmount',
        y='CustLocation',
        orientation='h',
        title='Top 20 Locations by Transaction Amount'
    )

def create_top_customers_fig(df):
    top_customers = df.groupby('CustomerID')['TransactionAmount'].sum().nlargest(20).reset_index()
    fig = px.bar(
        top_customers,
        x='CustomerID',
        y='TransactionAmount',
        title='Top 20 Customers by Transaction Amount'
    )
    fig.update_layout(xaxis_tickangle=-45)
    return fig

def create_monthly_trend_fig(df):
    filtered_df = df[df['CustLocation'].isin(['MUMBAI', 'NEW DELHI', 'DELHI', 'BANGALORE', 'GURGAON'])]
    month_counts = filtered_df['TransactionMonth'].value_counts().sort_index().reset_index()
    month_counts.columns = ['Month', 'TotalTransactions']
    return px.bar(
        month_counts,
        x='Month',
        y='TotalTransactions',
        title='Monthly Transaction Count (Top Locations)'
    )

def create_daily_pattern_fig(df):
    filtered_df = df[df['CustLocation'].isin(['MUMBAI', 'NEW DELHI', 'DELHI', 'BANGALORE', 'GURGAON'])]
    day_map = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'}
    filtered_df['DayOfWeek'] = filtered_df['TransactionDayOfWeek'].map(day_map)
    day_counts = filtered_df['DayOfWeek'].value_counts().loc[list(day_map.values())].reset_index()
    day_counts.columns = ['DayOfWeek', 'TotalTransactions']
    return px.bar(
        day_counts,
        x='DayOfWeek',
        y='TotalTransactions',
        title='Daily Transaction Pattern (Top Locations)'
    )

def create_hourly_pattern_fig(df):
    filtered_df = df[df['CustLocation'].isin(['MUMBAI', 'NEW DELHI', 'DELHI', 'BANGALORE', 'GURGAON'])]
    hour_counts = filtered_df['TransactionHour'].value_counts().sort_index().reset_index()
    hour_counts.columns = ['Hour', 'TotalTransactions']
    return px.bar(
        hour_counts,
        x='Hour',
        y='TotalTransactions',
        title='Hourly Transaction Pattern (Top Locations)'
    )

def create_hourly_pattern_all_fig(df):
    hour_counts = df['TransactionHour'].value_counts().sort_index().reset_index()
    hour_counts.columns = ['Hour', 'TotalTransactions']
    return px.bar(
        hour_counts,
        x='Hour',
        y='TotalTransactions',
        title='Hourly Transaction Pattern (All Locations)'
    )

def create_peak_hours_fig(df):
    peak_hours = list(range(9, 21))
    peak_hour_transactions = df[df['TransactionHour'].isin(peak_hours)]
    peak_hour_balance_change = peak_hour_transactions.groupby('TransactionHour')['AccountBalanceChange'].mean().reset_index()
    return px.bar(
        peak_hour_balance_change,
        x='TransactionHour',
        y='AccountBalanceChange',
        title='Average Account Balance Change During Peak Hours'
    )

def create_peak_days_fig(df):
    day_map = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'}
    peak_day_balance_change = df.groupby('TransactionDayOfWeek')['AccountBalanceChange'].mean().reset_index()
    peak_day_balance_change['DayOfWeek'] = peak_day_balance_change['TransactionDayOfWeek'].map(day_map)
    return px.bar(
        peak_day_balance_change,
        x='DayOfWeek',
        y='AccountBalanceChange',
        title='Average Account Balance Change by Day of Week'
    )

def create_heatmap_fig(df):
    day_map = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'}
    peak_hour_day_balance_change = df.groupby(['TransactionHour', 'TransactionDayOfWeek'])['AccountBalanceChange'].mean().reset_index()
    peak_hour_day_balance_change['DayOfWeek'] = peak_hour_day_balance_change['TransactionDayOfWeek'].map(day_map)
    return px.density_heatmap(
        peak_hour_day_balance_change,
        x='TransactionHour',
        y='DayOfWeek',
        z='AccountBalanceChange',
        title='Account Balance Change Heatmap by Hour and Day'
    )

def create_clv_fig(df):
    customer_stats = df.groupby('CustomerID').agg(
        TotalTransactions=('TransactionID', 'count'),
        TotalAmount=('TransactionAmount', 'sum'),
        AverageTransactionAmount=('TransactionAmount', 'mean'),
        LastTransactionDate=('TransactionDate', 'max')
    ).reset_index()
    customer_stats['CLV'] = customer_stats['TotalAmount']
    high_value_threshold = customer_stats['TotalAmount'].quantile(0.8)
    customer_stats['HighValueCustomer'] = (customer_stats['TotalAmount'] >= high_value_threshold).astype(int)
    return px.bar(
        customer_stats.nlargest(20, 'CLV'),
        x='CustomerID',
        y='CLV',
        title='Top 20 Customers by Lifetime Value',
        color='HighValueCustomer'
    )

def create_high_value_trend_fig(df):
    df['HighValueTransaction'] = (df['TransactionAmount'] >= df['TransactionAmount'].quantile(0.9)).astype(int)
    high_value_monthly = df[df['HighValueTransaction'] == 1].groupby('TransactionMonth')['HighValueTransaction'].count().reset_index()
    high_value_monthly.columns = ['Month', 'HighValueTransactions']
    return px.line(
        high_value_monthly,
        x='Month',
        y='HighValueTransactions',
        title='High-Value Transactions Over Time',
        markers=True
    )

def create_customer_segmentation_fig(df):
    customer_stats = df.groupby('CustomerID').agg(
        TotalTransactions=('TransactionID', 'count'),
        TotalAmount=('TransactionAmount', 'sum'),
        AverageTransactionAmount=('TransactionAmount', 'mean')
    ).reset_index()
    customer_stats['CLV'] = customer_stats['TotalAmount']
    high_value_threshold = customer_stats['TotalAmount'].quantile(0.8)
    customer_stats['HighValueCustomer'] = (customer_stats['TotalAmount'] >= high_value_threshold).astype(int)
    return px.scatter(
        customer_stats,
        x='TotalTransactions',
        y='AverageTransactionAmount',
        size='CLV',
        color='HighValueCustomer',
        title='Customer Segmentation',
        hover_data=['CustomerID']
    )

def create_age_analysis_fig(df):
    if 'CustAge' not in df.columns:
        return None
    df['AgeGroup'] = pd.cut(df['CustAge'], bins=[0, 25, 40, 55, 100],
                           labels=['<25', '25-40', '40-55', '55+'])
    age_data = df.groupby('AgeGroup').agg(
        AvgTransactionAmount=('TransactionAmount', 'mean'),
        TotalCustomers=('CustomerID', 'nunique())
    ).reset_index()
    return px.bar(
        age_data,
        x='AgeGroup',
        y='AvgTransactionAmount',
        title='Average Transaction Amount by Age Group'
    )

def dashboard(request):
    try:
        # Get optimized data
        df = get_optimized_data()
        
        if df is None or df.empty:
            return render(request, 'error.html', {
                'error': 'Could not load transaction data. Please try again later.'
            })

        # Create all visualizations
        context = {
            # Location and Customer Analysis
            'top_locations': create_figure(create_top_locations_fig, df),
            'top_customers': create_figure(create_top_customers_fig, df),
            
            # Temporal Patterns
            'monthly_trend': create_figure(create_monthly_trend_fig, df),
            'daily_pattern': create_figure(create_daily_pattern_fig, df),
            'hourly_pattern': create_figure(create_hourly_pattern_fig, df),
            'hourly_pattern_all': create_figure(create_hourly_pattern_all_fig, df),
            
            # Peak Analysis
            'peak_hours': create_figure(create_peak_hours_fig, df),
            'peak_days': create_figure(create_peak_days_fig, df),
            'heatmap': create_figure(create_heatmap_fig, df),
            
            # Customer Value Analysis
            'clv': create_figure(create_clv_fig, df),
            'high_value_trend': create_figure(create_high_value_trend_fig, df),
            'customer_segmentation': create_figure(create_customer_segmentation_fig, df),
            
            # Summary Metrics
            'total_customers': df['CustomerID'].nunique(),
            'total_transactions': len(df),
            'period_start': df['TransactionDate'].min().strftime('%Y-%m-%d'),
            'period_end': df['TransactionDate'].max().strftime('%Y-%m-%d'),
        }

        # Add age analysis if available
        if 'CustAge' in df.columns:
            context['age_analysis'] = create_figure(create_age_analysis_fig, df)

        return render(request, 'visualization/dashboard.html', context)

    except Exception as e:
        logger.error(f"Dashboard view error: {str(e)}")
        return render(request, 'error.html', {
            'error': 'An unexpected error occurred. Our team has been notified.'
        })