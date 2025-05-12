import plotly.express as px
import pandas as pd
import os
from django.shortcuts import render
from django.conf import settings
from datetime import datetime


Transactions = os.path.join(settings.BASE_DIR, 'data', "bank_transactions.csv")

def dashboard(request):
    
    df = pd.read_csv(Transactions)

    
    df.rename(columns={
        'TransactionAmount (INR)': 'TransactionAmount',
        'CustAccountBalance': 'AccountBalance'
    }, inplace=True)

   
    df['TransactionDate'] = pd.to_datetime(df['TransactionDate'])
    df['TransactionMonth'] = df['TransactionDate'].dt.to_period('M').astype(str) 
    df['TransactionDayOfWeek'] = df['TransactionDate'].dt.dayofweek
    df['TransactionHour'] = df['TransactionTime'].str.split(':').str[0].astype(int)
    df['AccountBalanceChange'] = df.groupby('CustomerID')['AccountBalance'].diff().fillna(0)

    
   
    # Top Locations by Transaction Amount
    transaction_amount_per_location = df.groupby('CustLocation')['TransactionAmount'].sum().reset_index()
    top_20_transaction_amounts = transaction_amount_per_location.nlargest(20, 'TransactionAmount')
    fig = px.bar(top_20_transaction_amounts,
                 x='TransactionAmount',
                 y='CustLocation',
                 orientation='h',
                 color='TransactionAmount',
                 color_continuous_scale='Blues',
                 title='Top 20 Customer Locations by Total Transaction Amount',
                 labels={'TransactionAmount': 'Total Transaction Amount (INR)', 'CustLocation': 'Customer Location'},
                 height=600)
    fig.update_layout(plot_bgcolor='white')

    # Top Customers by Transaction Amount
    top_customers = df.groupby('CustomerID')['TransactionAmount'].sum().nlargest(20).reset_index()
    fig1 = px.bar(top_customers,
                 x='CustomerID',
                 y='TransactionAmount',
                 title='Top 20 Customers by Total Transaction Amount',
                 labels={'TransactionAmount': 'Total Transaction Amount (INR)', 'CustomerID': 'Customer ID'},
                 color='TransactionAmount',
                 color_continuous_scale='Blues')
    fig1.update_layout(plot_bgcolor='white', xaxis_tickangle=-45)

    # Temporal Patterns (Filtered for top locations)
    filtered_df = df[df['CustLocation'].isin(['MUMBAI', 'NEW DELHI', 'DELHI', 'BANGALORE', 'GURGAON'])]
   
    # Monthly Transactions
    month_counts = filtered_df['TransactionMonth'].value_counts().sort_index().reset_index()
    month_counts.columns = ['Month', 'TotalTransactions']
    fig_month = px.bar(month_counts, x='Month', y='TotalTransactions',
                      title='Monthly Transaction Count (Top Locations)',
                      color='TotalTransactions',
                      color_continuous_scale='Blues')
    fig_month.update_layout(plot_bgcolor='white')

    # Daily Transactions
    day_map = {0: 'Monday', 1: 'Tuesday', 2: 'Wednesday',
              3: 'Thursday', 4: 'Friday', 5: 'Saturday', 6: 'Sunday'}
    filtered_df['DayOfWeek'] = filtered_df['TransactionDayOfWeek'].map(day_map)
    day_counts = filtered_df['DayOfWeek'].value_counts().loc[list(day_map.values())].reset_index()
    day_counts.columns = ['DayOfWeek', 'TotalTransactions']
    fig_day = px.bar(day_counts, x='DayOfWeek', y='TotalTransactions',
                    title='Daily Transaction Pattern (Top Locations)',
                    color='TotalTransactions',
                    color_continuous_scale='Blues')
    fig_day.update_layout(plot_bgcolor='white')

    # Hourly Transactions for top locations
    hour_counts = filtered_df['TransactionHour'].value_counts().sort_index().reset_index()
    hour_counts.columns = ['Hour', 'TotalTransactions']
    fig_hour = px.bar(hour_counts, x='Hour', y='TotalTransactions',
                     title='Hourly Transaction Pattern (Top Locations)',
                     color='TotalTransactions',
                     color_continuous_scale='Blues')
    fig_hour.update_layout(plot_bgcolor='white')

    # Hourly Transactions (Entire Bank)
    hour_counts_e = df['TransactionHour'].value_counts().sort_index().reset_index()
    hour_counts_e.columns = ['Hour', 'TotalTransactions']
    fig_hour_e = px.bar(hour_counts_e, x='Hour', y='TotalTransactions',
                       title='Hourly Transaction Pattern (All Locations)',
                       color='TotalTransactions',
                       color_continuous_scale='Blues')
    fig_hour_e.update_layout(plot_bgcolor='white')

    # Peak Hours Analysis
    peak_hours = list(range(9, 21))  
    
    peak_hour_transactions = df[df['TransactionHour'].isin(peak_hours)]
    peak_hour_balance_change = peak_hour_transactions.groupby('TransactionHour')['AccountBalanceChange'].mean().reset_index()
    fig_peak_hours = px.bar(peak_hour_balance_change,
                           x='TransactionHour',
                           y='AccountBalanceChange',
                           title='Average Account Balance change During Peak Hours (9AM-9PM)',
                           color='AccountBalanceChange',
                           color_continuous_scale='Blues')
    fig_peak_hours.update_layout(plot_bgcolor='white')

    # Peak Days Analysis
    peak_day_balance_change = df.groupby('TransactionDayOfWeek')['AccountBalanceChange'].mean().reset_index()
    peak_day_balance_change['DayOfWeek'] = peak_day_balance_change['TransactionDayOfWeek'].map(day_map)
    fig_peak_days = px.bar(peak_day_balance_change,
                          x='DayOfWeek',
                          y='AccountBalanceChange',
                          title='Average Account Balance change by Day of Week',
                          color='AccountBalanceChange',
                          color_continuous_scale='Blues')
    fig_peak_days.update_layout(plot_bgcolor='white')

    # Heatmap by Hour and Day
    peak_hour_day_balance_change = df.groupby(['TransactionHour', 'TransactionDayOfWeek'])['AccountBalanceChange'].mean().reset_index()
    peak_hour_day_balance_change['DayOfWeek'] = peak_hour_day_balance_change['TransactionDayOfWeek'].map(day_map)
    fig_peak_hour_day = px.density_heatmap(peak_hour_day_balance_change,
                                         x='TransactionHour',
                                         y='DayOfWeek',
                                         z='AccountBalanceChange',
                                         title='Account Balance change Heatmap by Hour and Day',
                                         color_continuous_scale='Blues')
    fig_peak_hour_day.update_layout(plot_bgcolor='white')

    

    # Calculate metrics for new visualizations
    customer_stats = df.groupby('CustomerID').agg(
        TotalTransactions=('TransactionID', 'count'),
        TotalAmount=('TransactionAmount', 'sum'),
        AverageTransactionAmount=('TransactionAmount', 'mean'),
        LastTransactionDate=('TransactionDate', 'max')
    ).reset_index()

    # Calculate CLV 
    customer_stats['CLV'] = customer_stats['TotalAmount']
   
    # High Value Transactions flag (top 20%)
    high_value_threshold = customer_stats['TotalAmount'].quantile(0.8)
    customer_stats['HighValueCustomer'] = (customer_stats['TotalAmount'] >= high_value_threshold).astype(int)

    # Customer Lifetime Value
    fig_clv = px.bar(customer_stats.nlargest(20, 'CLV'),
                    x='CustomerID',
                    y='CLV',
                    title='Top 20 Customers by Lifetime Value',
                    color='HighValueCustomer',
                    color_continuous_scale='Blues')
    fig_clv.update_layout(plot_bgcolor='white')

    # High Value Transactions Over Time
    df['HighValueTransaction'] = (df['TransactionAmount'] >= df['TransactionAmount'].quantile(0.9)).astype(int)
    high_value_monthly = df[df['HighValueTransaction'] == 1].groupby('TransactionMonth')['HighValueTransaction'].count().reset_index()
    high_value_monthly.columns = ['Month', 'HighValueTransactions']
    high_value_monthly['Month'] = high_value_monthly['Month'].astype(str)  
    fig_high_value = px.line(high_value_monthly,
                            x='Month',
                            y='HighValueTransactions',
                            title='High-Value Transactions Over Time',
                            markers=True)
    fig_high_value.update_layout(plot_bgcolor='white')

    # High Value Customer Segmentation
    fig_pie = px.pie(customer_stats,
                    names='HighValueCustomer',
                    title='High-Value Customer Segmentation',
                    color='HighValueCustomer',
                    color_discrete_map={0: 'lightgray', 1: 'gold'})
    fig_pie.update_layout(plot_bgcolor='white')

    # Revenue vs Transactions
    monthly_data = df.groupby('TransactionMonth').agg(
        TotalRevenue=('TransactionAmount', 'sum'),
        TotalTransactions=('TransactionID', 'count')
    ).reset_index()
    monthly_data['TransactionMonth'] = monthly_data['TransactionMonth'].astype(str)  
    fig_revenue_transactions = px.line(monthly_data,
                                     x='TransactionMonth',
                                     y=['TotalRevenue', 'TotalTransactions'],
                                     title='Revenue vs Transactions Over Time',
                                     markers=True)
    fig_revenue_transactions.update_layout(plot_bgcolor='white')

    # Customer Tenure Analysis
    df['CustomerTenure'] = (df['TransactionDate'] - df.groupby('CustomerID')['TransactionDate'].transform('min')).dt.days
    tenure_data = df.groupby('CustomerTenure')['TransactionAmount'].sum().reset_index()
    fig_tenure_trans = px.line(tenure_data,
                              x='CustomerTenure',
                              y='TransactionAmount',
                              title='Transaction Volume by Customer Tenure')
    fig_tenure_trans.update_layout(plot_bgcolor='white')

    # Customer Segmentation
    fig_segmentation = px.scatter(customer_stats,
                                x='TotalTransactions',
                                y='AverageTransactionAmount',
                                size='CLV',
                                color='HighValueCustomer',
                                title='Customer Segmentation',
                                hover_data=['CustomerID'])
    fig_segmentation.update_layout(plot_bgcolor='white')

    # Age Group Analysis 
    if 'CustAge' in df.columns:
        df['AgeGroup'] = pd.cut(df['CustAge'], bins=[0, 25, 40, 55, 100],
                               labels=['<25', '25-40', '40-55', '55+'])
        age_data = df.groupby('AgeGroup').agg(
            AvgTransactionAmount=('TransactionAmount', 'mean'),
            TotalCustomers=('CustomerID', 'nunique')
        ).reset_index()
        fig_age = px.bar(age_data,
                        x='AgeGroup',
                        y='AvgTransactionAmount',
                        title='Average Transaction Amount by Age Group')
        fig_age.update_layout(plot_bgcolor='white')

    # Convert all figures to HTML
    context = {
        'fig': fig.to_html(full_html=False),
        'fig1': fig1.to_html(full_html=False),
        'fig_month': fig_month.to_html(full_html=False),
        'fig_day': fig_day.to_html(full_html=False),
        'fig_hour': fig_hour.to_html(full_html=False),
        'fig_hour_e': fig_hour_e.to_html(full_html=False),
        'fig_peak_hours': fig_peak_hours.to_html(full_html=False),
        'fig_peak_days': fig_peak_days.to_html(full_html=False),
        'fig_peak_hour_day': fig_peak_hour_day.to_html(full_html=False),
        'fig_clv': fig_clv.to_html(full_html=False),
        'fig_high_value': fig_high_value.to_html(full_html=False),
        'fig_pie': fig_pie.to_html(full_html=False),
        'fig_revenue_transactions': fig_revenue_transactions.to_html(full_html=False),
        'fig_tenure_trans': fig_tenure_trans.to_html(full_html=False),
        'fig_segmentation': fig_segmentation.to_html(full_html=False),
    }

   
    if 'CustAge' in df.columns:
        context['fig_age'] = fig_age.to_html(full_html=False)

    return render(request, 'visualization/plotly_chart.html', context)