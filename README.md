Phonepe Pulse Data Visualization

Overview:
This repository contains a Streamlit web application for visualizing and analyzing Phonepe Pulse data. The data is extracted and aggregated from various sources, including transaction records, user data, and geographical mapping information.

Data Sources:
Aggregated Transaction Data:
Transaction data is collected at the country, India, and state levels.
The data includes information about transaction types, counts, and amounts.
Aggregation is performed based on states, years, and quarters.
Aggregated User Data:
User data is collected at the country, India, and state levels.
Information about user device brands, transaction counts, and percentages is included.
Aggregation is performed based on states, years, and quarters.
Map Data - Transaction and User:
Geographical data is collected for transaction and user information at the state and district levels in India.
The data includes details such as transaction counts, transaction amounts, registered users, and app opens.
Aggregation is performed based on states, years, quarters, and districts.
Database Integration:
MySQL database is used for storing aggregated transaction and user data.
SQLAlchemy is employed to create a database engine for seamless interaction with the MySQL database.
Streamlit Web Application:
The Streamlit web application provides an intuitive interface for data exploration and visualization.
The main dashboard includes an overview of total transaction count, total transaction amount, and average transaction amount.
A choropleth map of India displays total transaction amounts at the state level.
State-level Statistics:
Users can select a specific state from a dropdown menu to view detailed statistics.
Statistics include total transaction count, total transaction amount, and average transaction amount for the selected state.
Top 10 States:
The sidebar displays a table of the top 10 states based on transaction amounts.
Choropleth Map:
A choropleth map of India is generated using Plotly Express, showcasing transaction amounts in different states.
The map provides an interactive visualization of transaction data at the state level.
Top 10 Districts (if a state is selected):
When a state is selected, the application dynamically generates a bar chart of the top 10 districts within that state based on transaction amounts.
Dependencies:
Python libraries including Pandas, NumPy, SQLAlchemy, Streamlit, Plotly Express, Geopandas, and MySQL Connector are used for data processing, visualization, and database interactions.
Instructions for Use:
Ensure that the required dependencies are installed (pip install -r requirements.txt).
Configure the MySQL database connection details in the code.
Run the Streamlit application locally using streamlit run app.py.
Explore and analyze Phonepe Pulse data interactively through the web interface.
Future Work:
Potential enhancements include additional visualizations, interactivity improvements, and feature additions based on user feedback.
Feel free to contribute, provide feedback, or use this repository as a reference for your data visualization projects. Happy exploring!
