def staff_retirement_age_analysis(df):
    # Filter the dataset to include only 'Supervisor' and below ranks
    supervisor_and_below = ['Supervisor', 'Assistant Supervisor', 'Team Lead']  # Adjust as per your rank hierarchy
    filtered_df = df[df['RANK'].isin(supervisor_and_below)].copy()

    # Handle missing data
    filtered_df['DATE_JOINED_GROUP'] = filtered_df['DATE_JOINED_GROUP'].fillna(filtered_df['Date_Joined'])
    filtered_df['EmploymentType'] = filtered_df['EmploymentType'].fillna('UNKNOWN')
    filtered_df['Sections'] = filtered_df['Sections'].fillna('UNKNOWN')

    # Calculate 'Years to Retirement'
    filtered_df['Years_to_Retirement'] = 60 - filtered_df['Age']

    # Group by Rank and calculate required metrics
    result = filtered_df.groupby('RANK').agg(
        No_of_Employees=('EMP_NUMBER', 'count'),
        Avg_Age=('Age', 'mean'),
        Avg_Years_to_Retirement=('Years_to_Retirement', 'mean')
    ).reset_index()

    # Format the results
    result['Avg_Age'] = result['Avg_Age'].round(2)  # Round to 2 decimal places
    result['Avg_Years_to_Retirement'] = result['Avg_Years_to_Retirement'].round(2)

    # Add totals row
    totals = pd.DataFrame({
        'RANK': ['TOTAL'],
        'No_of_Employees': [result['No_of_Employees'].sum()],
        'Avg_Age': [result['Avg_Age'].mean()],  # Average of all averages
        'Avg_Years_to_Retirement': [result['Avg_Years_to_Retirement'].mean()]  # Average of all averages
    })

    # Concatenate with the original result
    result_table = pd.concat([result, totals], ignore_index=True)

    return result_table

# Usage:
result_table_for_supervisor_and_below_retirement = staff_retirement_age_analysis(Age)

# Create a table visualization
header = ['Rank', 'No_of_Employees', 'Avg_Age', 'Avg_Years_to_Retirement']
column_names = ['RANK', 'No_of_Employees', 'Avg_Age', 'Avg_Years_to_Retirement']
plot_html_table_for_retirement = create_table_age_group(result_table_for_supervisor_and_below_retirement, header, column_names)
