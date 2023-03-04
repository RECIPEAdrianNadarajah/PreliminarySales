import streamlit as st
st.set_page_config(page_title="RECIPE Preliminary Sales",
                   layout = "wide")
import pandas as pd
import numpy as np
from google.oauth2 import service_account
from google.cloud import bigquery

hide_streamlit_style = """
                       <style>
                       #MainMenu {visibility:hidden;}
                       footer {visibility:hidden;}
                       </style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html = True)

st.title("RECIPE Preliminary Sales")

##Create API Client - to connect into Advanced Data Analytics project in BigQuery
st.cache_resource()
bqCredentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
client = bigquery.Client(credentials = bqCredentials)

##GCP Queries
dailyTotalsQuery = ('''
               SELECT *
               FROM `rcp-ada.looker_ready.recipe_sales_yoy`
               WHERE DATE(business_date) <= CURRENT_DATE()-1
               ORDER BY DATE(business_date) DESC, store_number DESC, rvcID ASC
''')

##Pipeline Functions##
######################

st.cache_data(ttl = 86400)
def runQuery(query):
    query_job = client.query(query)
    return query_job.to_dataframe()


dailyTotals = runQuery(dailyTotalsQuery)

st.dataframe(dailyTotals)