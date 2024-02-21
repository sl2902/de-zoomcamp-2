import streamlit as st
import pandas as pd
import numpy as np
import altair as alt
import plotly.express as px
from google.oauth2 import service_account
from google.cloud import bigquery
import os, sys
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


page_title = "NYC Taxi Trips dashboard"
alt.themes.enable("dark")

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

@st.cache_data(ttl=600)
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
                            title="Range",
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
                            title="Range",
                             scale=alt.Scale(scheme=input_color_theme)),
            stroke=alt.value('black'),
            strokeWidth=alt.value(0.25),
        ).properties(width=900)
    
    alt.layer(heatmap).configure_axis(
        labelFontSize=12,
        titleFontSize=12
        ) 
    
    return heatmap

# Choropleth map
def make_choropleth(input_df, input_id, input_column, input_color_theme):
    aggregate = input_df.groupby(["txn_date", 'state_code'], as_index=False).agg({'revenue': 'mean'})
    avg_revenue = aggregate['revenue']
    choropleth = px.choropleth(aggregate, locations=input_id, color=avg_revenue, locationmode="USA-states",
                               color_continuous_scale=input_color_theme,
                               color_continuous_midpoint=aggregate["revenue"].median(),
                               range_color=(aggregate["revenue"].min(), aggregate["revenue"].max()),
                               scope="usa",
                              #  labels={'revenue':'Revenue'},
                               hover_data={'revenue': ':$,.0f'}
                              )
   #  choropleth.update_traces(hovertemplate=f'revenue: {avg_revenue:,.0f}')
    choropleth.update_layout(
        template='plotly_dark',
        plot_bgcolor='rgba(0, 0, 0, 0)',
        paper_bgcolor='rgba(0, 0, 0, 0)',
        margin=dict(l=0, r=0, t=0, b=0),
        height=350
    )
    return choropleth

# Line chart
def make_linechart(input_df, input_x=None, input_y=None, input_color=None):
    subset = input_df.query("(year == @selected_year) & (service_type == @selected_service_type)").reset_index(drop=True)
    if len(subset) == 0:
        st.write(f"No data for service {selected_service_type}")
        return
    else:
        # subset["pickup_date"] = subset["pickup_datetime"].dt.date
        subset = subset.groupby(['month'], as_index=False).size()
        input_x, input_y = 'month', 'size'
        linechart = alt.Chart(subset).mark_line().encode(
            alt.X(f"{input_x}:O", axis=alt.Axis(title="pickup month", titleFontSize=18, titlePadding=15, titleFontWeight=900, labelAngle=0), sort=list(mm_name.keys())),
            alt.Y(f"{input_y}:Q", axis=alt.Axis(title="number of rides", titleFontSize=18, titlePadding=15, titleFontWeight=900, labelAngle=0)),
            color=alt.value('gold'),
            tooltip=[alt.Tooltip(f"{input_y}:Q", title="num of rides", format=",d"), f'{input_x}:O']
        )
        return linechart

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
#    choropleth = make_choropleth(df, 'state_code', 'revenue', selected_color_theme)
#    st.plotly_chart(choropleth, use_container_width=True)
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