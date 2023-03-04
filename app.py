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