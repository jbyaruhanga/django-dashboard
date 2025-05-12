import plotly.express as px
import pandas as pd
import os
from django.shortcuts import render
from django.conf import settings
from django.core.cache import cache
from datetime import datetime
import logging
import numpy as np

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

        # Verify file exists
        if not os.path.exists(TRANSACTION_FILE):
            raise FileNotFoundError(f"Transaction file not found at {TRANSACTION_FILE}")

        # Load with column verification
        df = pd.read_csv(TRANSACTION_FILE)
        
        # Verify required columns exist
        required_columns = {
            'TransactionAmount': 'TransactionAmount',
            'CustAccountBalance': 'AccountBalance',
            'TransactionDate': 'TransactionDate',
            'TransactionTime': 'TransactionTime',
            'CustomerID': 'CustomerID',
            'CustLocation': 'CustLocation',
            'TransactionID': 'TransactionID'
        }
        
        # Check for missing columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")

        # Rename columns
        df = df.rename(columns=required_columns)

        # Convert and optimize data types
        df['TransactionDate'] = pd.to_datetime(df['TransactionDate'])
        df['TransactionMonth'] = df['TransactionDate'].dt.to_period('M').astype(str)
        df['TransactionDayOfWeek'] = df['TransactionDate'].dt.dayofweek.astype('int8')
        df['TransactionHour'] = df['TransactionTime'].str.split(':').str[0].astype('int8')
        df['AccountBalanceChange'] = df.groupby('CustomerID')['AccountBalance'].diff().fillna(0)

        # Downcast numeric columns
        numeric_cols = ['TransactionAmount', 'AccountBalance']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, downcast='float')

        # Sample if too large
        if len(df) > MAX_SAMPLE_SIZE:
            df = df.sample(MAX_SAMPLE_SIZE, random_state=42)

        cache.set('optimized_transaction_data', df, timeout=CACHE_TIMEOUT)
        return df

    except Exception as e:
        logger.error(f"Error loading transaction data: {str(e)}")
        raise

def create_figure(fig_func, df, *args, **kwargs):
    """Helper function to create and optimize plotly figures"""
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
        color='TransactionAmount',
        color_continuous_scale='Blues',
        title='Top 20 Customer Locations by Total Transaction Amount',
        labels={'TransactionAmount': 'Total Transaction Amount (INR)', 'CustLocation': 'Customer Location'},
        height=600
    )

def create_top_customers_fig(df):
    top_customers = df.groupby('CustomerID')['TransactionAmount'].sum().nlargest(20).reset_index()
    fig = px.bar(
        top_customers,
        x='CustomerID',
        y='TransactionAmount',
        title='Top 20 Customers by Total Transaction Amount',
        labels={'TransactionAmount': 'Total Transaction Amount (INR)', 'CustomerID': 'Customer ID'},
        color='TransactionAmount',
        color_continuous_scale='Blues'
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
        title='Monthly Transaction Count (Top Locations)',
        color='TotalTransactions',
        color_continuous_scale='Blues'
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
        title='Daily Transaction Pattern (Top Locations)',
        color='TotalTransactions',
        color_continuous_scale='Blues'
    )

def create_hourly_pattern_fig(df):
    filtered_df = df[df['CustLocation'].isin(['MUMBAI', 'NEW DELHI', 'DELHI', 'BANGALORE', 'GURGAON'])]
    hour_counts = filtered_df['TransactionHour'].value_counts().sort_index().reset_index()
    hour_counts.columns = ['Hour', 'TotalTransactions']
    return px.bar(
        hour_counts,
        x='Hour',
        y='TotalTransactions',
        title='Hourly Transaction Pattern (Top Locations)',
        color='TotalTransactions',
        color_continuous_scale='Blues'
    )

def create_hourly_pattern_all_fig(df):
    hour_counts = df['TransactionHour'].value_counts().sort_index().reset_index()
    hour_counts.columns = ['Hour', 'TotalTransactions']
    return px.bar(
        hour_counts,
        x='Hour',
        y='TotalTransactions',
        title='Hourly Transaction Pattern (All Locations)',
        color='TotalTransactions',
        color_continuous_scale='Blues'
    )

def create_peak_hours_fig(df):
    peak_hours = list(range(9, 21))
    peak_hour_transactions = df[df['TransactionHour'].isin(peak_hours)]
    peak_hour_balance_change = peak_hour_transactions.groupby('TransactionHour')['AccountBalanceChange'].mean().reset_index()
    return px.bar(
        peak_hour_balance_change,
        x='TransactionHour',
        y='AccountBalanceChange',
        title='Average Account Balance change During Peak Hours (9AM-9PM)',
        color='AccountBalanceChange',
        color_continuous_scale='Blues'
    )

def create_peak_days_fig(df):
    day_map = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday', 3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'}
    peak_day_balance_change = df.groupby('TransactionDayOfWeek')['AccountBalanceChange'].mean().reset_index()
    peak_day_balance_change['DayOfWeek'] = peak_day_balance_change['TransactionDayOfWeek'].map(day_map)
    return px.bar(
        peak_day_balance_change,
        x='DayOfWeek',
        y='AccountBalanceChange',
        title='Average Account Balance change by Day of Week',
        color='AccountBalanceChange',
        color_continuous_scale='Blues'
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
        title='Account Balance change Heatmap by Hour and Day',
        color_continuous_scale='Blues'
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
        color='HighValueCustomer',
        color_continuous_scale='Blues'
    )

def create_high_value_trend_fig(df):
    df['HighValueTransaction'] = (df['TransactionAmount'] >= df['TransactionAmount'].quantile(0.9)).astype(int)
    high_value_monthly = df[df['HighValueTransaction'] == 1].groupby('TransactionMonth')['HighValueTransaction'].count().reset_index()
    high_value_monthly.columns = ['Month', 'HighValueTransactions']
    high_value_monthly['Month'] = high_value_monthly['Month'].astype(str)
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
        TotalCustomers=('CustomerID', 'nunique')
    ).reset_index()
    return px.bar(
        age_data,
        x='AgeGroup',
        y='AvgTransactionAmount',
        title='Average Transaction Amount by Age Group'
    )

def dashboard(request):
    try:
        df = get_optimized_data()
        
        context = {
            'fig': create_figure(create_top_locations_fig, df),
            'fig1': create_figure(create_top_customers_fig, df),
            'fig_month': create_figure(create_monthly_trend_fig, df),
            'fig_day': create_figure(create_daily_pattern_fig, df),
            'fig_hour': create_figure(create_hourly_pattern_fig, df),
            'fig_hour_e': create_figure(create_hourly_pattern_all_fig, df),
            'fig_peak_hours': create_figure(create_peak_hours_fig, df),
            'fig_peak_days': create_figure(create_peak_days_fig, df),
            'fig_peak_hour_day': create_figure(create_heatmap_fig, df),
            'fig_clv': create_figure(create_clv_fig, df),
            'fig_high_value': create_figure(create_high_value_trend_fig, df),
            'fig_segmentation': create_figure(create_customer_segmentation_fig, df),
            'total_customers': df['CustomerID'].nunique(),
            'total_transactions': len(df),
            'period_start': df['TransactionDate'].min().strftime('%Y-%m-%d'),
            'period_end': df['TransactionDate'].max().strftime('%Y-%m-%d'),
        }

        if 'CustAge' in df.columns:
            context['fig_age'] = create_figure(create_age_analysis_fig, df)

        return render(request, 'visualization/plotly_chart.html', context)

    except FileNotFoundError as e:
        logger.error(f"Data file not found: {str(e)}")
        return render(request, 'error.html', {
            'error': 'Transaction data file not found.',
            'details': 'Please ensure the data file exists in the correct location.'
        })

    except ValueError as e:
        logger.error(f"Data validation error: {str(e)}")
        return render(request, 'error.html', {
            'error': 'Invalid data format.',
            'details': str(e)
        })

    except Exception as e:
        logger.error(f"Unexpected error in dashboard: {str(e)}")
        return render(request, 'error.html', {
            'error': 'An unexpected error occurred while loading the dashboard.',
            'details': 'Our team has been notified. Please try again later.'
        })