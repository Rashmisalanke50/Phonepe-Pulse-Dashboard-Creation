import os
import json
import pandas as pd
import mysql.connector
from sqlalchemy import create_engine
import streamlit as st
import plotly.express as px
import geopandas as gpd
import plotly.graph_objects as go

# Database Connection
db_connection = mysql.connector.connect(
    host="localhost",
    user="root",
    passwd="EMPATH.6",
    database="pulse")

# Create a MySQL Engine using SQLAlchemy
engine = create_engine('mysql+mysqlconnector://root:EMPATH.6@localhost:3306/pulse', echo=False)

#aggregate transaction

path0 ="C:/Users/rashm/OneDrive/Desktop/streamlit/rep/pulse/data/aggregated/transaction/country/india/state/"
translist= os.listdir(path0)
columns1={"States":[], "Years":[], "Transaction_type":[], "Transaction_count":[], "Transaction_amount": [], "Quarter": []}

for state in translist:
    cur_states=path0+state+"/"
    year_list=os.listdir(cur_states)
    
    for year in year_list:
        cur_year=cur_states+year+"/"
        file_list=os.listdir(cur_year)
        
        for file in file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")
            
            A=json.load(data)
            
            for i in A["data"]["transactionData"]:
                name=i["name"]
                count=i["paymentInstruments"] [0]["count"]
                amount=i["paymentInstruments"][0]["amount"]
                columns1["Transaction_type"].append(name)
                columns1["Transaction_count"].append(count)
                columns1["Transaction_amount"].append(amount)
                columns1["States"].append(state)
                columns1["Years"].append(year)
                columns1["Quarter"].append(int(file.strip(".json")))

agg_transaction=pd.DataFrame(columns1)

#aggregate user
path1="C:/Users/rashm/OneDrive/Desktop/streamlit/rep/pulse/data/aggregated/user/country/india/state/"
userlist= os.listdir(path1)
columns2={"States":[], "Years":[], "Quarter": [], "Brands":[], "Transaction_count":[], "Percentage": []}

for state in userlist:
    cur_states=path1+state+"/"
    year_list=os.listdir(cur_states)
    
    for year in year_list:
        cur_year=cur_states+year+"/"
        file_list=os.listdir(cur_year)
        
        for file in file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")
            
            B=json.load(data)
            
            try:
                for i in B["data"]["usersByDevice"]:
                    brand=i["brand"]
                    count=i["count"]
                    percentage=i["percentage"]
                    columns2["Brands"].append(brand)
                    columns2["Transaction_count"].append(count)
                    columns2["Percentage"].append(percentage)
                    columns2["States"].append(state)
                    columns2["Years"].append(year)
                    columns2["Quarter"].append(int(file.strip(".json")))
            except:
                pass
agg_user=pd.DataFrame(columns2)

#map transaction
path2="C:/Users/rashm/OneDrive/Desktop/streamlit/rep/pulse/data/map/transaction/hover/country/india/state/"
map_translist=os.listdir(path2)
columns3={"States":[], "Years":[], "Quarter": [], "Districts":[], "Transaction_count":[], "Transaction_amount": []}

for state in map_translist:
    cur_states=path2+state+"/"
    year_list=os.listdir(cur_states)
    
    for year in year_list:
        cur_year=cur_states+year+"/"
        file_list=os.listdir(cur_year)
        
        for file in file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")
            
            C=json.load(data)
            
            for i in C["data"]["hoverDataList"]:
                name=i["name"]
                count=i["metric"][0]["count"]
                amount=i["metric"][0]["amount"]
                columns3["Districts"].append(name)
                columns3["Transaction_count"].append(count)
                columns3["Transaction_amount"].append(amount)
                columns3["States"].append(state)
                columns3["Years"].append(year)
                columns3["Quarter"].append(int(file.strip(".json")))
        
map_trans=pd.DataFrame(columns3)

#map user
path3="C:/Users/rashm/OneDrive/Desktop/streamlit/rep/pulse/data/map/user/hover/country/india/state/"
map_userlist=os.listdir(path3)
columns4={"States":[], "Years":[], "Quarter": [], "Districts":[], "RegisteredUsers":[], "AppOpens": []}

for state in map_userlist:
    cur_states=path3+state+"/"
    year_list=os.listdir(cur_states)
    
    for year in year_list:
        cur_year=cur_states+year+"/"
        file_list=os.listdir(cur_year)
        
        for file in file_list:
            cur_file=cur_year+file
            data=open(cur_file,"r")
            
            D=json.load(data)
            
            for i in D["data"]["hoverData"].items():
                district=i[0]
                registeredUsers=i[1]["registeredUsers"]
                appOpens=i[1]["appOpens"]
                columns4["Districts"].append(district)
                columns4["RegisteredUsers"].append(registeredUsers)
                columns4["AppOpens"].append(appOpens)
                columns4["States"].append(state)
                columns4["Years"].append(year)
                columns4["Quarter"].append(int(file.strip(".json")))

map_users=pd.DataFrame(columns4)

#table creation in sql
agg_transaction.to_sql('agg_transaction_table', con=engine, if_exists='replace', index=False)
agg_user.to_sql('agg_user_table', con=engine, if_exists='replace', index=False)
map_trans.to_sql('map_trans_table', con=engine, if_exists='replace', index=False)
map_users.to_sql('map_users_table', con=engine, if_exists='replace', index=False)

# Function to check for null values in a DataFrame and drop columns with null values
def check_and_drop_nulls(df, table_name, engine):
    null_columns = df.columns[df.isnull().any()]
    
    if null_columns.any():
        print(f"Table: {table_name}")
        print("Columns with Null Values:")
        print(null_columns)
        df = df.dropna(axis=1)
        
        # Save the DataFrame back to the table without null columns
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        print("Null columns dropped and updated table in the database.")
    else:
        print(f"No null values found in the table: {table_name}")

# List of table names to check for null values and drop columns
table_names = ['agg_transaction_table', 'agg_user_table', 'map_trans_table', 'map_users_table']

# Iterate over tables
for table_name in table_names:
    df = pd.read_sql_table(table_name, con=engine)
    check_and_drop_nulls(df, table_name, engine)

# Define a function to fetch data from MySQL Database
def fetch_data(query):
    df = pd.read_sql(query, con=engine)
    return df

# Query to get the data for choropleth map
choropleth_query = "SELECT States, SUM(Transaction_amount) AS TotalAmount FROM agg_transaction_table GROUP BY States"
choropleth_df = fetch_data(choropleth_query)

# Query to get the total transaction amount, count, and average amount
total_transaction_query = """
    SELECT 
        SUM(Transaction_count) AS TotalCount, 
        SUM(Transaction_amount) AS TotalAmount, 
        AVG(Transaction_amount) AS AvgAmount 
    FROM agg_transaction_table
"""
total_transaction_df = fetch_data(total_transaction_query)
total_count = total_transaction_df['TotalCount'].iloc[0]
total_amount = total_transaction_df['TotalAmount'].iloc[0]
avg_amount = total_transaction_df['AvgAmount'].iloc[0]

# Query to get the top 10 states with the highest transaction amount
top_states_query = "SELECT States, SUM(Transaction_amount) AS TotalAmount FROM agg_transaction_table GROUP BY States ORDER BY TotalAmount DESC LIMIT 10"
top_states_df = fetch_data(top_states_query)


# Streamlit App
# Streamlit App Title
st.markdown(
    "<h1 style='color: #002060; font-family: Times New Roman, Times, serif; text-align: center;'>Phonepe Pulse Data Visualization</h1>",
    unsafe_allow_html=True
)
st.sidebar.markdown("## Data Overview")

st.sidebar.markdown(
    f"<div style='background-color: #8FAADC; padding: 10px; border-radius: 10px;'>"
    f"<p style='color: black; font-size: 16px;'>Total Transaction Count: {total_count:,.0f}</p>"
    f"<p style='color: black; font-size: 16px;'>Total Transaction Amount: Rs.{(total_amount)}</p>"
    f"<p style='color: black; font-size: 16px;'>Average Transaction Amount: Rs.{(avg_amount)}</p>"
    "</div>",
    unsafe_allow_html=True)

st.sidebar.markdown("### Top 10 States by Transaction Amount")

# Apply formatting to the DataFrame
formatted_top_states_df = top_states_df.copy()
formatted_top_states_df["TotalAmount"] = formatted_top_states_df["TotalAmount"]

# Center align the table contents in the sidebar
st.sidebar.table(formatted_top_states_df.style.set_properties(**{'background-color': 'white'}))

# Dropdown to select the state
selected_state = st.selectbox("Select a State:", choropleth_df['States'].unique())
# Display stats for the selected state
if selected_state:
    st.markdown(f"### Statistics for {selected_state.capitalize()}")
    
    # Query to get total transaction amount for the selected state
    state_transaction_query = f"SELECT SUM(Transaction_count) AS TotalCount, SUM(Transaction_amount) AS TotalAmount, AVG(Transaction_amount) AS AvgAmount FROM agg_transaction_table WHERE States = '{selected_state}'"
    state_transaction_df = fetch_data(state_transaction_query)
    state_total_count = state_transaction_df['TotalCount'].iloc[0]
    state_total_amount = state_transaction_df['TotalAmount'].iloc[0]
    state_avg_amount = state_transaction_df['AvgAmount'].iloc[0]
    st.markdown(
        f"<div style='display: flex; justify-content: space-between; background-color: #BFBFBF; padding: 4px; border-radius: 10px;'>"
        f"    <div style='background-color: #BFBFBF; padding: 4px; border-radius: 2px; margin-right: 2px;'>"
        f"        <p style='color: black; font-size: 14px;'>Total Transaction Count: {state_total_count:,.0f}</p>"
        f"    </div>"
        f"    <div style='background-color: #BFBFBF; padding: 4px; border-radius: 2px; margin-right: 2px;'>"
        f"        <p style='color: black; font-size: 14px;'>Total Transaction Amount: Rs.{state_total_amount}</p>"
        f"    </div>"
        f"    <div style='background-color: #BFBFBF; padding: 4px; border-radius: 2px;'>"
        f"        <p style='color: black; font-size: 14px;'>Average Transaction Amount: Rs.{state_avg_amount}</p>"
        f"    </div>"
        f"</div>",
        unsafe_allow_html=True
    )
else:
    st.warning("Please select a state from the dropdown.")


# India Map
# Query to get the data for India map
choropleth_query = "SELECT States, SUM(Transaction_amount) AS TotalAmount FROM agg_transaction_table GROUP BY States"
choropleth_df = fetch_data(choropleth_query)

# Read the GeoJSON file and load it into a GeoDataFrame
geojson_path = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
geojson_states = gpd.read_file(geojson_path)

# Convert the state names in the GeoDataFrame to lowercase for consistent comparison
geojson_states['ST_NM'] = geojson_states['ST_NM'].str.lower()

# Mapping dictionary for state names
state_mapping = {
    'andaman-&-nicobar-islands': 'andaman & nicobar',
    'andhra-pradesh': 'andhra pradesh',
    'arunachal-pradesh': 'arunachal pradesh',
    'assam': 'assam',
    'bihar': 'bihar',
    'chandigarh': 'chandigarh',
    'chhattisgarh': 'chhattisgarh',
    'dadra-&-nagar-haveli-&-daman-&-diu': 'dadra and nagar haveli and daman and diu',
    'delhi': 'delhi',
    'goa': 'goa',
    'gujarat': 'gujarat',
    'haryana': 'haryana',
    'himachal-pradesh': 'himachal pradesh',
    'jammu-&-kashmir': 'jammu & kashmir',
    'jharkhand': 'jharkhand',
    'karnataka': 'karnataka',
    'kerala': 'kerala',
    'ladakh': 'ladakh',
    'lakshadweep': 'lakshadweep',
    'madhya-pradesh': 'madhya pradesh',
    'maharashtra': 'maharashtra',
    'manipur': 'manipur',
    'meghalaya': 'meghalaya',
    'mizoram': 'mizoram',
    'nagaland': 'nagaland',
    'odisha': 'odisha',
    'puducherry': 'puducherry',
    'punjab': 'punjab',
    'rajasthan': 'rajasthan',
    'sikkim': 'sikkim',
    'tamil-nadu': 'tamil nadu',
    'telangana': 'telangana',
    'tripura': 'tripura',
    'uttar-pradesh': 'uttar pradesh',
    'uttarakhand': 'uttarakhand',
    'west-bengal': 'west bengal'
}

# Apply the mapping to your data
choropleth_df['States'] = choropleth_df['States'].map(state_mapping)

# Merge the DataFrame with the GeoJSON data based on state names
merged_df = pd.merge(choropleth_df, geojson_states, left_on='States', right_on='ST_NM')


# India map figure
fig = px.choropleth(
    data_frame=merged_df,
    geojson=geojson_states,
    locations='ST_NM',
    color='TotalAmount',
    featureidkey='properties.ST_NM',
    color_continuous_scale='mint'
)
# Adjust the height and width of the figure
fig.update_layout(
    height=800,  
    width=1200   
)
fig.update_geos(fitbounds="locations", visible=False)

# Display the choropleth map in the Streamlit app
st.plotly_chart(fig)

# Check if a state is selected
if selected_state:
    # Query to get top districts for the selected state
    top_districts_query = f"SELECT Districts, SUM(Transaction_amount) AS TotalAmount FROM map_trans_table WHERE States = '{selected_state}' GROUP BY Districts ORDER BY TotalAmount DESC LIMIT 10"
    top_districts_df = fetch_data(top_districts_query)

    # Create a bar chart for top districts
    bar_chart = go.Figure(go.Bar(
        x=top_districts_df['Districts'],
        y=top_districts_df['TotalAmount'],
        marker_color='#203864'
    ))

    # Update the layout of the bar chart
    bar_chart.update_layout(
        title=f'Top 10 Districts in {selected_state} by Transaction Amount',
        xaxis_title='Districts',
        yaxis_title='Total Transaction Amount (Rs.)',
        showlegend=False
    )

    # Display the bar chart in the Streamlit app
    st.plotly_chart(bar_chart)