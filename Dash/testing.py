import plotly.express as px
import pandas as pd
import os
from django.shortcuts import render
from django.conf import settings

# File paths
Transactions = os.path.join(settings.BASE_DIR, 'data', "bank_transactions.csv")

def combined_dashboard(request):
    # Load data
    df = pd.read_csv(Transactions)

    # Clean column name if necessary
    df.rename(columns={'TransactionAmount (INR)': 'TransactionAmount'}, inplace=True)

    # Aggregate transaction amounts per location
    transaction_amount_per_location = df.groupby('CustLocation')['TransactionAmount'].sum().reset_index()

    # Get top 20 locations by total transaction amount
    top_20_transaction_amounts = transaction_amount_per_location.nlargest(20, 'TransactionAmount')

    # Create an interactive bar chart for top 20 locations
    fig = px.bar(top_20_transaction_amounts,
                 x='TransactionAmount',
                 y='CustLocation',
                 orientation='h',
                 color='TransactionAmount',
                 color_continuous_scale='Blues',
                 title='Top 20 Customer Locations by Total Transaction Amount',
                 labels={'TransactionAmount': 'Total Transaction Amount (INR)', 'CustLocation': 'Customer Location'},
                 height=600)

    # Customize layout for the location bar chart
    fig.update_layout(
        title_x=0.5,  # Center title
        title_font_size=18,  # Set title font size
        xaxis_title_font_size=14,
        yaxis_title_font_size=14,
        showlegend=False,  # Hide legend
        plot_bgcolor='white',  # Set background to white
        bargap=0.3  # Increase space between bars
    )

    # Aggregate top 20 customers by total transaction amount
    top_customers = (
        df.groupby('CustomerID')['TransactionAmount']
        .sum()
        .sort_values(ascending=False)
        .head(20)
        .reset_index()
    )

    # Create an interactive bar chart for top 20 customers
    fig1 = px.bar(
        top_customers,
        x='CustomerID',
        y='TransactionAmount',
        title='Top 20 Customers by Total Transaction Amount',
        labels={'TransactionAmount': 'Total Transaction Amount (INR)', 'CustomerID': 'Customer ID'},
        color='TransactionAmount',
        color_continuous_scale='Blues',
        text='TransactionAmount',
    )

    # Customize layout for the customer bar chart
    fig1.update_layout(
        title_font_size=20,
        xaxis_tickangle=-45,
        xaxis_title_font=dict(size=14),
        yaxis_title_font=dict(size=14),
        plot_bgcolor='white',
        margin=dict(t=60, b=100),
        width=1100,
        height=500,
        bargap=0.1,
    )
    fig1.update_traces(texttemplate='%{text:.2s}', textposition='outside')

    # Convert figures to HTML for embedding in the template
    fig_html = fig.to_html(full_html=False)
    fig1_html = fig1.to_html(full_html=False)

    # Render the dashboard template with the figures
    return render(request, 'visualization/plotly_chart.html', {
        'fig': fig_html,
        'fig1': fig1_html,
    })
