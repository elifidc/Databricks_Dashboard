import streamlit as st
import pandas as pd
import plotly.express as px
df = pd.read_csv("~/Desktop/Databricks Dashboard/cleaned_customers.csv")
#print(df.columns)
#df = df.sort_values('ideal_count', ascending=False).head(20)

# Load your cleaned dataset from DBFS using Spark
#df_spark = spark.read.csv("/FileStore/cleaned_customers.csv", header=True, inferSchema=True)
#df = df_spark.toPandas()

# Title
st.title("Find Your Ideal Customers by ZIP Code")

# Sidebar filters
st.sidebar.header("Define Your Ideal Customer")
traits = {
    'Has Children': 'PresenceOfChildrenInd',
    'Food & Wine Interest': 'FoodWines',
    'Upscale Lifestyle': 'UpscaleLiving',
    'Online Purchaser': 'OnlinePurchasingIndicator',
    'Likes Jewelry': 'Jewelry'
}

selected_traits = [v for k, v in traits.items() if st.sidebar.checkbox(k)]

# Only run the rest if user selected at least one checkbox
if selected_traits:
    # Ensure selected columns are numeric
    for col in selected_traits:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Filter based on number of matching traits
    filter_condition = df[selected_traits].sum(axis=1) >= 1

    # filter_condition = df[selected_traits].sum(axis=1) >= len(selected_traits)
    filtered_df = df[filter_condition]

    # Group by ZIP
    zip_summary = (
    filtered_df.groupby("ZIPCode")
    .agg(
        ideal_count=('CustomerID', 'count'),
        Latitude=('Latitude', 'first'),
        Longitude=('Longitude', 'first')
    )
    .reset_index()
    .dropna(subset=['Latitude', 'Longitude'])
    .sort_values("ideal_count", ascending=False)
    .head(20)  # ⬅️ ADD THIS to show only top ZIPs
)

    # Convert to numeric if needed
    zip_summary['Latitude'] = pd.to_numeric(zip_summary['Latitude'], errors='coerce')
    zip_summary['Longitude'] = pd.to_numeric(zip_summary['Longitude'], errors='coerce')

    # Map
    st.subheader("Matching ZIP Codes")
    fig = px.scatter_mapbox(
        zip_summary,
        lat="Latitude",
        lon="Longitude",
        size="ideal_count",
        color="ideal_count",
        hover_name="ZIPCode",
        mapbox_style="carto-positron",
        zoom=5
    )
    st.plotly_chart(fig)

    # Table
    st.subheader("Summary Table")
    st.dataframe(zip_summary.sort_values("ideal_count", ascending=False))

else:
    st.warning("Please select at least one trait to search.")
