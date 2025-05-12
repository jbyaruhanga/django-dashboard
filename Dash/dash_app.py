import os
import pandas as pd
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
from django_plotly_dash import DjangoDash
from django.conf import settings
import plotly.express as px

# File path for the data (Ensure this path is correct)
file_path = os.path.join(settings.BASE_DIR, "data", "StaffJoinedbanklastmonth.xlsx")
df = pd.read_excel(file_path)

# Initialize the Dash app with external Plotly stylesheet
app = DjangoDash('StaffDashboard', external_stylesheets=['https://cdnjs.cloudflare.com/ajax/libs/plotly.js/2.11.1/plotly.min.css'])

# Function to create graphs
def create_graphs(df):
    graphs = []
    # Debugging the dataframe to ensure it has data and columns
    print(f"Dataframe columns: {df.columns}")
    print(f"Dataframe head: {df.head()}")

    # Graph 1: Staff Rank Distribution by Gender
    count_data = df.groupby(['RANK', 'Gender']).size().reset_index(name='Count')
    fig1 = px.bar(count_data, x='RANK', y='Count', color='Gender', barmode='group',
                  title="Staff Rank Distribution by Gender")
    graphs.append(dcc.Graph(figure=fig1, style={'height': '400px'}))

    # Graph 2: Staff Location Distribution by Gender
    location_data = df.groupby(['Location', 'Gender']).size().reset_index(name='Count')
    fig2 = px.bar(location_data, x='Location', y='Count', color='Gender', barmode='group',
                  title="Staff Location Distribution by Gender")
    graphs.append(dcc.Graph(figure=fig2, style={'height': '400px'}))

    # Graph 3: Staff Designation Distribution by Gender
    designation_data = df.groupby(['Designation', 'Gender']).size().reset_index(name='Count')
    fig3 = px.bar(designation_data, x='Designation', y='Count', color='Gender', barmode='group',
                  title="Staff Designation Distribution by Gender")
    graphs.append(dcc.Graph(figure=fig3, style={'height': '400px'}))

    # Graph 4: Staff Region Distribution by Gender
    region_data = df.groupby(['LOC_NAME', 'Gender']).size().reset_index(name='Count')
    fig4 = px.bar(region_data, x='LOC_NAME', y='Count', color='Gender', barmode='group',
                  title="Staff Region Distribution by Gender")
    graphs.append(dcc.Graph(figure=fig4, style={'height': '400px'}))

    # Return the list of graphs
    print(f"Created {len(graphs)} graphs")  # Debugging line
    return graphs

# Layout of the app
app.layout = html.Div([
    html.H1("Staff Analytics Dashboard", style={'text-align': 'center'}),
    # Placeholder for the graphs, dynamically updated via callback
    html.Div(id='graphs-container', style={'display': 'grid', 'grid-template-columns': '1fr 1fr', 'gap': '20px', 'padding': '20px'})
])

# Callback to dynamically update the graphs
@app.callback(
    Output('graphs-container', 'children'),
    Input('dummy-input', 'value')  # This is a dummy input to trigger the callback
)
def update_graphs(dummy_input):
    return create_graphs(df)  # Generate the graphs dynamically

# Ensure the app is running in Django
if __name__ == '__main__':
    app.run_server(debug=True)
