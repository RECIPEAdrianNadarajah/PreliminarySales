import streamlit as st
st.set_page_config(page_title="RECIPE Preliminary Sales",
                   layout = "wide")
import pandas as pd
import numpy as np
from google.oauth2 import service_account
from google.cloud import bigquery

hide_streamlit_style = """
                       <style>
                       footer {visibility:hidden;}
                       </style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html = True)

##Create API Client - to connect into Advanced Data Analytics project in BigQuery
st.cache_resource()
bqCredentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])
client = bigquery.Client(credentials = bqCredentials)

##GCP Queries
dailyTotalsQuery = ('''
SELECT CASE WHEN bum.BrandName IN ('Elephant & Castle (US)', 'Elephant & Castle') THEN 'Elephant & Castle'
            WHEN bum.BrandName IN ('State & Main (US)', 'State & Main') THEN 'State & Main'
            WHEN bum.BrandName IN ('The Keg Steakhouse + Bar', 'The Keg Steakhouse + Bar (US)') THEN 'The Keg Steakhouse + Bar'
            ELSE bum.BrandName END AS brandName,
       bum.NationalRegion as nationalRegion,
       bum.provinceName as provinceName,
       yoy.store_number as storeNumber,
       yoy.business_date as businessDate,
       CASE WHEN rvcID = 1 THEN 'Dine In'
            WHEN rvcID = 2 THEN 'Take Out + Pick Up'
            WHEN rvcID = 3 THEN 'Delivery'
            WHEN rvcID = 4 THEN 'Drive Thru'
            WHEN rvcID = 5 THEN 'Pick Up'
            ELSE NULL END AS channel,
       cal.isyesterday as isYesterday,
       cal.iscurrentweek as isWTD,
       cal.iscurrentperiod as isPTD,
       cal.iscurrentquarter as isQTD,
       cal.iscurrentyear as isYTD,
       System_net_sales as systemNetSales,
       SameRestaurant_TY_net_sales as sameRestaurantTYNetSales,
       SameRestaurant_LY_net_sales as sameRestaurantLYNetSales,
       System_guest_count as systemGuestCount,
       SameRestaurant_TY_guest_count as sameRestaurantTYGuestCount,
       SameRestaurant_TY_guest_count as sameRestaurantLYGuestCount,
       System_transaction_count as systemTransactionCount,
       SameRestaurant_TY_transaction_count as sameRestaurantTYTransactionCount,
       SameRestaurant_LY_transaction_count as sameRestaurantLYTransactionCount,
       System_atv as systemATV,
       SameRestaurant_TY_atv as sameRestaurantTYATV,
       SameRestaurant_LY_atv as sameRestaurantLYATV,
       System_agc as systemAGC,
       SameRestaurant_TY_agc as sameRestaurantTYAGC,
       SameRestaurant_LY_agc as sameRestaurantLYAGC
FROM `rcp-ada.looker_ready.recipe_sales_yoy` yoy
  join `rcp-ada.reference_data.bum` bum
    on yoy.store_number = SAFE_CAST(bum.SourceLocationID AS INT64)
  join `rcp-ada.reference_data.fiscal_calendar` cal
    ON yoy.business_date = cal.full_date
WHERE business_date BETWEEN '2021-12-27' AND CURRENT_DATE()-1
ORDER BY business_date DESC, store_number DESC, rvcID ASC
''')

##Pipeline Functions##
######################

st.cache_data(ttl = 86400)
##Instantiate queries from BQ to DataFrames.
def runQuery(query):
    query_job = client.query(query)
    return query_job.to_dataframe()

##Clean NaN values
st.cache_data(ttl = 86400)
def fillNA(dataframe):
    values = {tuple(["systemNetSales",
               "sameRestaurantTYNetSales",
               "sameRestaurantLYNetSales",
               "systemGuestCount",
               "sameRestaurantTYGuestCount",
               "sameRestaurantLYGuestCount",
               "systemTransactionCount",
               "sameRestaurantTYTransactionCount",
               "sameRestaurantLYTransactionCount",
               "systemAGC",
               "sameRestaurantTYAGC",
               "sameRestaurantLYAGC",
               "systemATV",
               "sameRestaurantTYATV",
               "sameRestaurantLYATV"]):0}
    df = dataframe.fillna(value = values)
    return df

##Create System Measure Dataframes -- For Net Sales/Guest Count/Transaction Count
st.cache_data(ttl = 86400)
def createSystemDataframe(inputDataframe, timeframe, measure):
        dataframe = inputDataframe[inputDataframe[timeframe]==True].groupby('brandName')[measure].sum()
        return dataframe

##Create TY Measure Dataframes -- For Net Sales/Guest Count/Transaction Count
st.cache_data(ttl = 86400)
def createTYDataframe(inputDataframe, timeframe, measure):
        dataframe = inputDataframe[inputDataframe[timeframe]==True].groupby('brandName')[measure].sum()
        return dataframe

##Create LY Measure Dataframes -- For Net Sales/Guest Count/Transaction Count
st.cache_data(ttl = 86400)
def createLYDataframe(inputDataframe, timeframe, measure):
        dataframe = inputDataframe[inputDataframe[timeframe]==True].groupby('brandName')[measure].sum()
        return dataframe

##Merge dataframes to calculate growth measures
st.cache_data(ttl = 86400)
def createSRSDataframe(inputDataframe, timeframe):
        '''
        Create the appropriate SRS dataframe ('srs%', 'system$')
        for the varying timeframes: yesterday, wtd, ptd, qtd, ytd
        '''
        tyDataframe = createTYDataframe(inputDataframe, timeframe, 'sameRestaurantTYNetSales')
        lyDataframe = createLYDataframe(inputDataframe, timeframe, 'sameRestaurantLYNetSales')
        systemDataframe = createSystemDataframe(inputDataframe, timeframe, 'systemNetSales')
        compingDataframe = pd.concat([tyDataframe, lyDataframe], axis = 1)
        compingDataframe['srs%'] = round((compingDataframe['sameRestaurantTYNetSales']/compingDataframe['sameRestaurantLYNetSales'])-1,3)
        mergedDataframe = pd.concat([compingDataframe, systemDataframe], axis = 1)
        mergedDataframe = mergedDataframe[['srs%', 'systemNetSales']]
        return mergedDataframe

st.cache_data(ttl = 86400)
def createSRGCDataframe(inputDataframe, timeframe):
        '''
        Create the appropriate SRGC dataframe ('srgc%', 'system')
        for the varying timeframes: yesterday, wtd, ptd, qtd, ytd
        '''
        tyDataframe = createTYDataframe(inputDataframe, timeframe, 'sameRestaurantTYGuestCount')
        lyDataframe = createLYDataframe(inputDataframe, timeframe, 'sameRestaurantLYGuestCount')
        systemDataframe = createSystemDataframe(inputDataframe, timeframe, 'systemGuestCount')
        compingDataframe = pd.concat([tyDataframe, lyDataframe], axis = 1)
        compingDataframe['srgc%'] = round((compingDataframe['sameRestaurantTYGuestCount']/compingDataframe['sameRestaurantLYGuestCount'])-1,3)
        mergedDataframe = pd.concat([compingDataframe, systemDataframe], axis = 1)
        mergedDataframe = mergedDataframe[['srgc%', 'systemGuestCount']]
        return mergedDataframe

st.cache_data(ttl = 86400)
def createSRTCDataframe(inputDataframe, timeframe):
        '''
        Create the appropriate SRTC dataframes ('srtc%', 'system'))
        for the varying timeframes: yesterday, wtd, ptd, qtd, ytd
        '''
        tyDataframe = createTYDataframe(inputDataframe, timeframe, 'sameRestaurantTYTransactionCount')
        lyDataframe = createLYDataframe(inputDataframe, timeframe, 'sameRestaurantLYTransactionCount')
        systemDataframe = createSystemDataframe(inputDataframe, timeframe, 'systemTransactionCount')
        compingDataframe = pd.concat([tyDataframe, lyDataframe], axis = 1)
        compingDataframe['srtc%'] = round((compingDataframe['sameRestaurantTYTransactionCount']/compingDataframe['sameRestaurantLYTransactionCount'])-1,3)
        mergedDataframe = pd.concat([compingDataframe, systemDataframe], axis =1)
        mergedDataframe = mergedDataframe[['srtc%', 'systemTransactionCount']]
        return mergedDataframe

##Pipeline Functions##
######################

dailyTotals = runQuery(dailyTotalsQuery)
fillNA(dailyTotals)

##Brand Preliminary Sales needs to have the following:
##SRS table -   yesterday,         WTD,         PTD,            QTD,            YTD
        #   - SRS%, SYSTEM$, SRS%, SYSTEM$ ,SRS%, SYSTEM$, SRS%, SYSTEM$,  SRS%, SYSTEM$
##SRGC table -   yesterday,         WTD,         PTD,            QTD,            YTD
        #   - SRS%, SYSTEM$, SRS%, SYSTEM$ ,SRS%, SYSTEM$, SRS%, SYSTEM$,  SRS%, SYSTEM$
##SRTC table -   yesterday,         WTD,         PTD,            QTD,            YTD
        #   - SRS%, SYSTEM$, SRS%, SYSTEM$ ,SRS%, SYSTEM$, SRS%, SYSTEM$,  SRS%, SYSTEM$
##ATV - National Table -   yesterday,         WTD,         PTD,            QTD,            YTD
                 #   - SRS%, SYSTEM$, SRS%, SYSTEM$ ,SRS%, SYSTEM$, SRS%, SYSTEM$,  SRS%, SYSTEM$
##ATV - Ontario Table -  yesterday,         WTD,         PTD,            QTD,            YTD
                #   - SRS%, SYSTEM$, SRS%, SYSTEM$ ,SRS%, SYSTEM$, SRS%, SYSTEM$,  SRS%, SYSTEM$
##AGC Table -   yesterday,         WTD,         PTD,            QTD,            YTD
        #   - SRS%, SYSTEM$, SRS%, SYSTEM$ ,SRS%, SYSTEM$, SRS%, SYSTEM$,  SRS%, SYSTEM$
##Channel Table -   yesterday,         WTD,         PTD,            QTD,            YTD
        #   - SRS%, SYSTEM$, SRS%, SYSTEM$ ,SRS%, SYSTEM$, SRS%, SYSTEM$,  SRS%, SYSTEM$
##Region Table -   yesterday,         WTD,         PTD,            QTD,            YTD
        #   - SRS%, SYSTEM$, SRS%, SYSTEM$ ,SRS%, SYSTEM$, SRS%, SYSTEM$,  SRS%, SYSTEM$
##Location Table

with st.sidebar:
    st.title("Prelim Sales Reports")
    reportSelection = st.radio("Please select a report: ", ('📊 Executive Preliminary Sales', '📈 Brand Preliminary Sales'))

if reportSelection == "📊 Executive Preliminary Sales": ##Executive Preliminary Sales
    st.title("Executive Preliminary Sales")
    dateMapping = {"Yesterday":"isYesterday",
                   "WTD":"isWTD",
                   "PTD":"isPTD",
                   "QTD":"isQTD",
                   "YTD":"isYTD"}
    srsdataframeDict = {"Yesterday":0,
                     "WTD":0,
                     "PTD":0,
                     "QTD":0,
                     "YTD":0}
    srgcdataframeDict = {"Yesterday":0,
                     "WTD":0,
                     "PTD":0,
                     "QTD":0,
                     "YTD":0}
    srtcdataframeDict = {"Yesterday":0,
                     "WTD":0,
                     "PTD":0,
                     "QTD":0,
                     "YTD":0}
    with st.expander("SRS% - Click to Expand: "):
        st.header("SRS% Dataframe")
        for x in dateMapping.keys():
                srsdataframeDict[x] = createSRSDataframe(dailyTotals, dateMapping[x])
                srsdataframeDict[x] = srsdataframeDict[x].rename(columns = {"srs%":f"srs%{x}", "systemNetSales":f"system${x}"})
        srsDataframe = pd.concat([srsdataframeDict['Yesterday'], srsdataframeDict['WTD'], srsdataframeDict['PTD'], srsdataframeDict['QTD'], srsdataframeDict['YTD']], axis = 1)
        st.dataframe(srsDataframe)
    with st.expander("SRGC% - Click to Expand: "):
        st.header("SRGC% Dataframe")
        for x in dateMapping.keys():
                srgcdataframeDict[x] = createSRGCDataframe(dailyTotals, dateMapping[x])
                srgcdataframeDict[x] = srgcdataframeDict[x].rename(columns = {'srgc%':f'srgc%{x}','systemGuestCount':f'system{x}'})
        srgcDataframe = pd.concat([srgcdataframeDict['Yesterday'], srgcdataframeDict['WTD'], srgcdataframeDict['PTD'], srgcdataframeDict['QTD'], srgcdataframeDict['YTD']], axis = 1)
        st.dataframe(srgcDataframe)
    with st.expander('SRTC% - Click to Expand: '):
        st.header('SRTC% Dataframe')
        for x in dateMapping.keys():
                srtcdataframeDict[x] = createSRTCDataframe(dailyTotals, dateMapping[x])
                srtcdataframeDict[x] = srtcdataframeDict[x].rename(columns = {'srtc%':f'srtc%{x}','systemTransactionCount':f'system{x}'})
        srtcDataframe = pd.concat([srtcdataframeDict["Yesterday"], srtcdataframeDict['WTD'], srtcdataframeDict['PTD'], srtcdataframeDict['QTD'], srtcdataframeDict['YTD']], axis = 1)
        st.dataframe(srtcDataframe)
elif reportSelection == "📈 Brand Preliminary Sales":
    st.title("Brand Preliminary Sales")
    dateMapping = {"Yesterday":"isYesterday",
                   "WTD":"isWTD",
                   "PTD":"isPTD",
                   "QTD":"isQTD",
                   "YTD":"isYTD"}
    srsdataframeDict = {"Yesterday":0,
                     "WTD":0,
                     "PTD":0,
                     "QTD":0,
                     "YTD":0}
    srgcdataframeDict = {"Yesterday":0,
                     "WTD":0,
                     "PTD":0,
                     "QTD":0,
                     "YTD":0}
    srtcdataframeDict = {"Yesterday":0,
                     "WTD":0,
                     "PTD":0,
                     "QTD":0,
                     "YTD":0}
    agcdataframeDict = {"Yesterday":0,
                        "WTD":0,
                        "PTD":0,
                        "QTD":0,
                        "YTD":0}
    atvdataframeDict = {"Yesterday":0,
                        "WTD":0,
                        "PTD":0,
                        "QTD":0,
                        "YTD":0}
    brandList = ('Swiss Chalet',
                 "East Side Mario's",
                 "Fresh Kitchen + Juice Bar",
                 "The Keg Steakhouse + Bar",
                 "Ultimate Kitchens",
                 "Landing Group",
                 "Pickle Barrel",
                 "Montana's",
                 "Bier Markt",
                 "Kelseys",
                 "Harvey's",
                 "The Burger's Priest",
                 "New York Fries",
                 "St. Hubert",
                 "Original Joe's",
                 "State & Main",
                 "Elephant & Castle",
                 "Anejo Restaurant",
                 "Blanco Cantina")
    brandSelect = st.selectbox('Select a Brand: ', brandList)
    with st.expander("Brand SRS% - Click to Expand: "):
        st.header("Brand SRS%")
        for x in dateMapping.keys():
                srsdataframeDict[x] = createSRSDataframe(dailyTotals, dateMapping[x])
                srsdataframeDict[x] = srsdataframeDict[x].rename(columns = {'srs%':f'srs%{x}', 'systemNetSales':f'system{x}'})
        brandSRSDataframe = pd.concat([srsdataframeDict['Yesterday'], srsdataframeDict['WTD'], srsdataframeDict['PTD'], srsdataframeDict['QTD'], srsdataframeDict['YTD']], axis = 1)
        #brandSRSDataframe = brandSRSDataframe.loc[brandSelect]
        st.dataframe(brandSRSDataframe)
