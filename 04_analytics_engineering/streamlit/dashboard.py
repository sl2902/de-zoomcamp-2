import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import plotly.express as px
from google.oauth2 import service_account
from google.cloud import bigquery
import os, sys
import geopandas as gpd
import json
current_dir = os.path.dirname(os.path.abspath(__file__))

# Append the 'project_root' directory to the Python path
project_root = os.path.abspath(os.path.join(current_dir, ".."))
sys.path.append(project_root)
# from dags.config_parameters import *
PROJECT_NUMBER = "hive-413217"
DATASET = "dbt_taxi_trips"
TABLE = "fact_trips"
FHV_TABLE = "fact_fhv_trips"
url = "https://raw.githubusercontent.com/sl2902/de-zoomcamp-2/main/04_analytics_engineering/geo_data/taxi_zones.geojson"


page_title = "NYC Taxi Trips dashboard"
# alt.themes.enable("dark")

st.set_page_config(
    page_title=page_title,
    page_icon="🏂",
    layout="wide",
    initial_sidebar_state="expanded")

dow = {
    "Monday": 0,
    "Tuesday": 1,
    "Wednesday": 2,
    "Thursday": 3,
    "Friday": 4,
    "Saturday": 5,
    "Sunday": 6
}

mm_name = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12
}

#credentials = service_account.Credentials.from_service_account_info(
#    json.load(open('/Users/home/Documents/secrets/personal-gcp.json'))
#)
credentials = service_account.Credentials.from_service_account_info(
    st.secrets["gcp_service_account"]
)
client = bigquery.Client(credentials=credentials)

# with open('geo_data/taxi_zones.geojson', 'r') as json_data:
#     geo_data = json.load(json_data)


@st.cache_data(ttl=1800)
def prepare_txn_query(query):
    print(query)
    query_job = client.query(query).to_dataframe()
   #  rows = [dict(row) for row in recs]
    return query_job

df = prepare_txn_query("""
                  SELECT
                       trip_id,
                       vendor_id,
                       service_type,
                       pickup_locationid,
                       pickup_borough,
                       pickup_zone,
                       dropoff_borough,
                       dropoff_zone,
                       pickup_datetime,
                       dropoff_datetime,
                       passenger_count,
                       trip_distance,
                       total_amount,
                       payment_type_description,
                       extract(year from pickup_datetime) as year,
                       format_date('%B', pickup_datetime) as month
                  FROM
                     `{project_number}.{bq_dataset}.{table}`
                  WHERE
                       extract(year from pickup_datetime) >= 2019
                  AND
                       total_amount > 0
                  AND
                       total_amount < 1000
                  LIMIT 100000
                  """.format(project_number=PROJECT_NUMBER, bq_dataset=DATASET, table=TABLE)
)

df2 = prepare_txn_query("""
                  SELECT
                       Affiliated_base_number,
                       'fhv' as service_type,
                       pickup_locationid,
                       pickup_borough,
                       pickup_zone,
                       dropoff_borough,
                       dropoff_zone,
                       pickup_datetime,
                       dropoff_datetime,
                       extract(year from pickup_datetime) as year,
                       format_date('%B', pickup_datetime) as month
                  FROM
                     `{project_number}.{bq_dataset}.{table}`
                  WHERE
                       Affiliated_base_number is not null
                  LIMIT 100000
                  """.format(project_number=PROJECT_NUMBER, bq_dataset=DATASET, table=FHV_TABLE)
)

with st.sidebar:
    st.title(f'🏂 {page_title}')
    year_list = df["year"].astype(np.int16).unique()[::-1]
    selected_year = st.selectbox('Select a year', year_list)
    service_list = df["service_type"].unique().tolist() + df2["service_type"].unique().tolist()
    selected_service_type = st.selectbox('Select a service type', service_list)
    color_theme_list = ['blues', 'cividis', 'greens', 'inferno', 'magma', 'plasma', 'reds', 'rainbow', 'turbo', 'viridis']
    selected_color_theme = st.selectbox('Select a color theme', color_theme_list)
    df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce")
    # geo_df = gpd.GeoDataFrame.from_features((geo_data))
    # geo_df = geo_df.to_crs('EPSG:4326')
    # geo_df = geo_df[geo_df['borough'] == 'Manhattan']
    # geo_df = geo_df[['location_id', 'zone', 'geometry']]
    # geo_df = geo_df.rename(columns={'location_id':'LocationID'})
    # geo_df["LocationID"] = pd.to_numeric(geo_df["LocationID"])


def make_heatmap(input_df, input_y, input_x, input_color, input_color_theme):
    # base = alt.Chart(input_df).transform_aggregate(
    # mean_revenue=f'mean({input_color})',
    # groupby=[input_x, input_y]
    #      ).encode(
    #         alt.Y(f"{input_y}:O", axis=alt.Axis(title="pickup month", titleFontSize=18, titlePadding=15, titleFontWeight=900, labelAngle=0), sort=mm_name[input_y]),
    #         alt.X(f"{input_x}:O", axis=alt.Axis(title="pickup borough", titleFontSize=18, titlePadding=15, titleFontWeight=900, labelAngle=0)),
    #         tooltip=[f'{input_y}', f'{input_x}', alt.Tooltip("mean_revenue:Q", format="$,.0f")],
    #      )
    subset = df.query("(year == @selected_year) & (service_type == @selected_service_type)").reset_index(drop=True)#.round({"total_amount": 0})
    if len(subset) == 0:
        return
    base = subset.groupby([input_x, input_y], as_index=False)\
            .agg({input_color: "mean"})\
            .rename(columns={"total_amount": "mean_amount"})\
            .sort_values(input_y, key=lambda x: x.map(mm_name))
    heatmap = alt.Chart(base).mark_rect().encode(
            alt.Y(f"{input_y}:O", axis=alt.Axis(title="pickup month", titleFontSize=18, titlePadding=15, titleFontWeight=900, labelAngle=0), sort=list(mm_name.keys())),
            alt.X(f"{input_x}:O", axis=alt.Axis(title="pickup borough", titleFontSize=18, titlePadding=15, titleFontWeight=900, labelAngle=0)),
            tooltip=[f'{input_y}', f'{input_x}', alt.Tooltip(f"mean_amount:Q", format="$,.0f")],
            color=alt.Color(f'mean_amount:Q',
                            #  legend="None",
                            title="Average amount($)",
                             scale=alt.Scale(scheme=input_color_theme)),
            stroke=alt.value('black'),
            strokeWidth=alt.value(0.25),
        ).properties(width=900)
    
    alt.layer(heatmap).configure_axis(
        labelFontSize=12,
        titleFontSize=12
        ) 
    
    return heatmap

def make_heatmap2(input_df, input_y, input_x, input_color, input_color_theme):
    subset = df.query("(year == @selected_year) & (service_type == @selected_service_type)").reset_index(drop=True)
    if len(subset) == 0:
        return
    base = subset.groupby([input_x, input_y], as_index=False)\
            .agg({input_color: "mean"})\
            .rename(columns={"total_amount": "mean_amount"})\
            .sort_values(input_y, key=lambda x: x.map(mm_name))
    heatmap = alt.Chart(base).mark_rect().encode(
            alt.Y(f"{input_y}:O", axis=alt.Axis(title="dropoff borough", titleFontSize=18, titlePadding=15, titleFontWeight=900, labelAngle=0)),
            alt.X(f"{input_x}:O", axis=alt.Axis(title="pickup borough", titleFontSize=18, titlePadding=15, titleFontWeight=900, labelAngle=0)),
            tooltip=[f'{input_y}', f'{input_x}', alt.Tooltip(f"mean_amount:Q", format="$,.0f")],
            color=alt.Color(f'mean_amount:Q',
                            title="Average amount($)",
                             scale=alt.Scale(scheme=input_color_theme)),
            stroke=alt.value('black'),
            strokeWidth=alt.value(0.25),
        ).properties(width=900)
    
    alt.layer(heatmap).configure_axis(
        labelFontSize=12,
        titleFontSize=12
        ) 
    
    return heatmap

@st.cache_data()
def load_geometry(url: str):
    return (
        alt.Data(
            url=url,
            format=alt.DataFormat(property='features')
        )
    )


# Choropleth map
def make_choropleth(input_df, url, input_id, input_column, input_color_theme):
    subset = input_df.query("(year == @selected_year) & (service_type == @selected_service_type)").reset_index(drop=True)
    aggregate = subset.groupby([input_id], as_index=False)[input_column].mean().round({input_column: 0})
    aggregate[input_id] = aggregate[input_id].astype(int)
    aggregate[input_column] = pd.to_numeric(aggregate[input_column])

    aggregate = aggregate.rename(columns={input_id: "location_id"})

    geometry = load_geometry(url)

    colours_obj = alt.Color(f'properties.{input_column}:Q',
                            scale=alt.Scale(scheme=input_color_theme),
                            legend=None,
                            title="Average pickup revenue")

    # sel_line_hover = alt.selection_single(on='mouseover', empty='none')
    # sel_line_col = alt.selection_single()
    # sel_line_size = alt.selection_single(empty='none')
    # choropleth = alt.Chart(geometry).mark_geoshape(
    # stroke='black',
    # strokeWidth=1
    # ).encode(
    #     color=alt.Color('properties.zone:N', scale=alt.Scale(scheme='viridis'), title="Zone"),
    #     tooltip=['properties.location_id:O']
    # )
    # https://github.com/streamlit/streamlit/issues/1002

    choropleth = alt.Chart(geometry).mark_geoshape(
        stroke='black',
        strokeWidth=1
    ).transform_lookup(
        lookup='properties.location_id',
        from_=alt.LookupData(
            data=aggregate,
            key='location_id',
            fields=['location_id', input_column])
    ).encode(
        color=alt.Color(f'{input_column}:Q', scale=alt.Scale(scheme=input_color_theme), title="Average amount ($)"),
        tooltip=[alt.Tooltip('properties.zone:O' ,title="zone"), alt.Tooltip(f'{input_column}:Q', format="$d")]
    ).properties(
        width=350,
        height=450
    )



    return choropleth

# Line chart
def make_linechart(input_df, input_x=None, input_y=None, input_color=None):
    # https://stackoverflow.com/questions/53287928/tooltips-in-altair-line-charts
    subset = input_df.query("(year == @selected_year) & (service_type == @selected_service_type)").reset_index(drop=True)
    if len(subset) == 0:
        st.write(f"No data for service {selected_service_type}")
        return
    else:
        subset["pickup_date"] = subset["pickup_datetime"].dt.date
        subset["month"] = subset["pickup_date"].to_numpy().astype("datetime64[M]")
        subset = subset.groupby(['month'], as_index=False).size()
        input_x, input_y = 'month', 'size'
        linechart = alt.Chart(subset).mark_line().encode(
            alt.X(f"month({input_x}):T", axis=alt.Axis(title="pickup month", titleFontSize=18, titlePadding=15, titleFontWeight=900, labelAngle=0), sort=list(mm_name.keys())),
            alt.Y(f"{input_y}:Q", axis=alt.Axis(title="number of rides", titleFontSize=18, titlePadding=15, titleFontWeight=900, labelAngle=0)),
            color=alt.value('gold'),
            tooltip=[alt.Tooltip(f"{input_y}:Q", title="num of rides", format=",d"), alt.Tooltip(f'month({input_x}):T', title="pickup month")]
        )
        tt = linechart.mark_line(strokeWidth=30, opacity=0.01)
        return linechart + tt

def _format_arrow(val):
    return f"{'↑' if val > 0 else '↓'} {abs(val):.2f}%" if val != 0 and val != 999 else '-'

def _color_arrow(val):
    return "color: green" if val > 0 and val != 999 else "color: red" if val < 0 else "color: white"

# Dashboard Main Panel
col = st.columns((1.5, 4.5, 2), gap='medium')

with col[0]:
    st.markdown('#### Monthly trip amount')

    subset = df.query("(year == @selected_year) & (service_type == @selected_service_type)").reset_index(drop=True)
    if len(subset) == 0:
        st.text(f"No data for service {selected_service_type}")
    else:
        subset["total_amount"] = pd.to_numeric(subset["total_amount"], errors="coerce")
        subset = subset.groupby(["month"], as_index=False)\
                .agg({"total_amount": "mean"})\
                .sort_values(["total_amount", "month"], ascending=[False, True])
        # custom sort key=lambda x: x.map(mm_name)
        st.dataframe(subset,
                    column_order=("month", "total_amount"),
                    hide_index=True,
                    width=None,
                    column_config={
                        "month": st.column_config.TextColumn(
                            "Month",
                        ),
                        "total_amount": st.column_config.ProgressColumn(
                            "Avg amount",
                            format="$%.0f",
                            min_value=0,
                            max_value=max(subset.total_amount),
                            )}
                    )


with col[1]:
   st.markdown('#### Average trip amount')
   choropleth = make_choropleth(df, url, 'pickup_locationid', 'total_amount', selected_color_theme)
   if choropleth:
       st.altair_chart(choropleth, use_container_width=True)
   else:
       st.text(f"No data for service {selected_service_type}")

   heatmap = make_heatmap(df, 'month', 'pickup_borough', 'total_amount', selected_color_theme)
   if heatmap:
       st.altair_chart(heatmap, use_container_width=True)
   else:
       st.text(f"No data for service {selected_service_type}")
   heatmap = make_heatmap2(df, 'dropoff_borough', 'pickup_borough', 'total_amount', selected_color_theme)
   if heatmap:
       st.altair_chart(heatmap, use_container_width=True)
   else:
       st.text(f"No data for service {selected_service_type}")
   
   st.markdown('#### Average number of rides')
   if selected_service_type in ["green", "yellow"]:
       linechart = make_linechart(df)
   elif selected_service_type == "fhv":
       linechart = make_linechart(df2)
   if linechart:
       st.altair_chart(linechart, use_container_width=True)

       

with col[2]:
    st.markdown('#### Popular payment modes')

    subset = df.query("(year == @selected_year) & (service_type == @selected_service_type)").reset_index(drop=True)
    if len(subset) == 0:
        st.text(f"No data for service {selected_service_type}")
    else:
        subset = subset.groupby(["payment_type_description"], as_index=False).size().sort_values("size", ascending=False)
        st.dataframe(subset,
                    column_order=("payment_type_description", "size"),
                    hide_index=True,
                    width=None,
                    column_config={
                        "payment_type_descriptionion": st.column_config.TextColumn(
                            "Payment type",
                        ),
                        "size": st.column_config.ProgressColumn(
                            "Count",
                            format="%.0f",
                            min_value=0,
                            max_value=max(subset["size"])
                        )}
                )

    with st.expander('About', expanded=True):
        st.write('''
            - Data: [NYC Taxi Trips](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
            - :red[Note]: Only a sample of the data is shown here. Streamlit has trouble caching huge volume
                    of data
            - :orange[FHV]:  Data is available for only 2019. Additionally, there is no data around trip distance
                    or the amount
            ''')