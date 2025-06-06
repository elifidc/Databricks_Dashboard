import streamlit as st
import pandas as pd
import plotly.express as px
import requests
import time
import databricks.sql as dbsql

# Load Databricks credentials from Streamlit secrets
host = st.secrets["databricks"]["host"].rstrip("/")
token = st.secrets["databricks"]["token"]


@st.cache_data(ttl=3600)
def load_data_from_databricks(query):
    from databricks import sql

    conn = sql.connect(
        server_hostname=st.secrets["databricks"]["host"].replace("https://", "").rstrip("/"),
        http_path=st.secrets["databricks"]["http_path"],
        access_token=st.secrets["databricks"]["token"]
    )

    cursor = conn.cursor()
    cursor.execute(query)
    result = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(result, columns=columns)
    cursor.close()
    conn.close()

    return df


# Replace the CSV load with this:
query = "SELECT * FROM ds25_wp1.cleaned_customers LIMIT 10000"
df = load_data_from_databricks(query)







binary_cols = [
    'PresenceOfChildrenInd', 'FoodWines', 'Jewelry',
    'UpscaleLiving', 'OnlinePurchasingIndicator', 'AutomotiveBuff', 'BookReader',
    'CookingEnthusiast', 'ExerciseEnthusiast', 'Gardener', 'GolfEnthusiast',
    'HomeDecoratingEnthusiast', 'OutdoorEnthusiast', 'OutdoorSportsLover', 'Photography',
    'VeteranInHousehold', 'Smoker', 'HealthAndBeauty', 'Musicalinstruments', 'Arts', 
    'SewingKnittingNeedlework', 'Woodworking', 'HomeImprovement', 'GamingCasino', 'HomeSwimmingPoolIndicator'
]

# Convert to numeric and fill missing
df[binary_cols] = df[binary_cols].apply(pd.to_numeric, errors='coerce').fillna(0)

# top_occupations = df.groupby('ZIPCode')[binary_cols].sum().apply(
#     lambda row: row.nlargest(3).index.tolist(), axis=1
# ).reset_index()
# top_occupations.columns = ['ZIPCode', 'TopOccupations']
# zip_summary = zip_summary.merge(top_occupations, on='ZIPCode', how='left')
# zip_summary['Ideal Customer Count:'] = zip_summary['ideal_count']
# zip_summary.rename(columns={'TopOccupations': 'Most Common Occupations:'}, inplace=True)


# Title
st.title("Find Your Ideal Customers by ZIP Code")

# Sidebar filters
st.sidebar.header("Define Your Ideal Customer")
traits = {
    'Has Children': 'PresenceOfChildrenInd',
    'Food & Wine Interest': 'FoodWines',
    'Upscale Lifestyle': 'UpscaleLiving',
    'Online Purchaser': 'OnlinePurchasingIndicator',
    'Likes Jewelry': 'Jewelry',
    'Automotive Buff': 'AutomotiveBuff',
    'Book Reader': 'BookReader',
    'Cooking Enthusiast': 'CookingEnthusiast',
    'Exercise Enthusiast': 'ExerciseEnthusiast',
    'Gardener': 'Gardener',
    'Photography': 'Photography',
    'Veteran in Household': 'VeteranInHousehold',
    'Smoker': 'Smoker',
    'Health and Beauty': 'HealthAndBeauty',
    'Musical Instruments': 'Musicalinstruments',
    'Arts': 'Arts',
    'Sewing/Knitting': 'SewingKnittingNeedlework',
    'Woodworking': 'Woodworking',
    'Home Improvement': 'HomeImprovement',
    'Gambling': 'GamingCasino',
    'Has Swimming Pool': 'HomeSwimmingPoolIndicator'
}


selected_traits = [v for k, v in traits.items() if st.sidebar.checkbox(k)]

# Only run the rest if user selected at least one checkbox
# Only run the rest if user selected at least one checkbox
if selected_traits:
    # Ensure selected columns are numeric
    for col in selected_traits:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Filter based on number of matching traits
    filter_condition = df[selected_traits].sum(axis=1) >= 1
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
        .head(20)
    )

    # 🔁 Recalculate top occupations for filtered_df
    top_occupations = filtered_df.groupby('ZIPCode')[binary_cols].sum().apply(
        lambda row: row.nlargest(3).index.tolist(), axis=1
    ).reset_index()
    top_occupations.columns = ['ZIPCode', 'TopOccupations']

    # Merge into zip_summary and rename columns
    zip_summary = zip_summary.merge(top_occupations, on='ZIPCode', how='left')
    zip_summary['Ideal Customer Count:'] = zip_summary['ideal_count']
    zip_summary.rename(columns={'TopOccupations': 'Most Common Occupations:'}, inplace=True)

    # Ensure coordinates are numeric
    zip_summary['Latitude'] = pd.to_numeric(zip_summary['Latitude'], errors='coerce')
    zip_summary['Longitude'] = pd.to_numeric(zip_summary['Longitude'], errors='coerce')

    # Map
    fig = px.scatter_mapbox(
        zip_summary,
        lat="Latitude",
        lon="Longitude",
        size="ideal_count",
        color="ideal_count",
        hover_name="ZIPCode",
        hover_data={
            'Latitude': False, 
            'Longitude': False, 
            'Ideal Customer Count:': True, 
            'Most Common Occupations:': True,
            'ideal_count': False
        },
        mapbox_style="carto-positron",
        zoom=6,
        center={"lat": 27.6648, "lon": -81.5158},
        color_continuous_scale="Viridis"
    )

    fig.update_traces(
        hovertemplate="<b>ZIP Code:</b> %{customdata[0]}<br>" +
                      "<b>Ideal Customer Count:</b> %{customdata[1]}<br>" +
                      "<b>Most Common Occupations:</b> %{customdata[2]}<extra></extra>",
        customdata=zip_summary[['ZIPCode', 'Ideal Customer Count:', 'Most Common Occupations:']]
    )

    st.plotly_chart(fig)

    # Table
    st.subheader("Summary Table")
    st.dataframe(zip_summary.sort_values("ideal_count", ascending=False))

else:
    st.warning("Please select at least one trait to search.")
