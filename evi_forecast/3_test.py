#!/usr/bin/env python
# coding: utf-8

# Copyright (c) Microsoft Corporation. All rights reserved.
# 
# Licensed under the MIT License.

# # Test EVI Forecast (local)

# In[ ]:


# Stanadard library imports
import json
import pickle
import os
import sys
import requests
from datetime import datetime,timedelta

# Disable unnecessary logs 
import logging
logging.disable(sys.maxsize)
import warnings
warnings.filterwarnings("ignore")

# Third party library imports
import numpy as np
import pandas as pd
import rasterio
import tensorflow as tf
from tensorflow import keras

# Local imports
from utils.config import farmbeats_config
from utils.weather_util import WeatherUtil
from utils.satellite_util import SatelliteUtil
from utils.constants import CONSTANTS
from utils.ard_util import ard_preprocess

# Azure imports
from azure.identity import ClientSecretCredential

# SDK imports
from azure.farmbeats import FarmBeatsClient


# ### Farmbeats Configuration

# In[ ]:


# FarmBeats Client definition
credential = ClientSecretCredential(
    tenant_id=farmbeats_config['tenant_id'],
    client_id=farmbeats_config['client_id'],
    client_secret=farmbeats_config['client_secret'],
    authority=farmbeats_config['authority']
)

credential_scopes = [farmbeats_config['default_scope']]

fb_client = FarmBeatsClient(
    base_url=farmbeats_config['instance_url'],
    credential=credential,
    credential_scopes=credential_scopes,
    logging_enable=True
)


# ### Forecast EVI for test Boundary

# #### Satellie Data

# In[ ]:


root_dir = CONSTANTS['root_dir']
farmer_id = "contoso_farmer" 
boundary_id = "boundary1" 

end_dt = datetime.strptime(datetime.now().strftime("%Y-%m-%d"), "%Y-%m-%d")
start_dt = end_dt - timedelta(days=60)

# get boundary object
boundary = fb_client.boundaries.get(
            farmer_id=farmer_id,
            boundary_id=boundary_id
        )


# In[ ]:


sat_links = SatelliteUtil(farmbeats_client = fb_client).download_and_get_sat_file_paths(farmer_id, [boundary], start_dt, end_dt, root_dir)

# get last available data of satellite data
end_dt_w = datetime.strptime(
    sat_links.sceneDateTime.sort_values(ascending=False).values[0][:10], "%Y-%m-%d"
)
# calculate 30 days from last satellite available date
start_dt_w = end_dt_w - timedelta(days=CONSTANTS["input_days"] - 1)


# #### Weather Data

# In[ ]:


# get weather data historical
weather_list = fb_client.weather.list(
            farmer_id=  boundary.farmer_id,
            boundary_id= boundary.id,
            start_date_time=start_dt_w,
            end_date_time=end_dt,
            extension_id=farmbeats_config['weather_provider_extension_id'],
            weather_data_type= "historical", 
            granularity="daily")
weather_data = []
for w_data in weather_list:
    weather_data.append(w_data)
w_df_hist = WeatherUtil.get_weather_data_df(weather_data)


# In[ ]:


# get weather data forecast
weather_list = fb_client.weather.list(
            farmer_id=  boundary.farmer_id,
            boundary_id= boundary.id,
            start_date_time=end_dt,
            end_date_time=end_dt + timedelta(10),
            extension_id=farmbeats_config['weather_provider_extension_id'], 
            weather_data_type= "forecast", 
            granularity="daily")

weather_data = []
for w_data in weather_list:
    weather_data.append(w_data)
w_df_forecast = WeatherUtil.get_weather_data_df(weather_data)


# In[ ]:


# merge weather data
weather_df = pd.concat([w_df_hist, w_df_forecast], axis=0, ignore_index=True)

with open(CONSTANTS["w_pkl"], "rb") as f:
    w_parms, weather_mean, weather_std = pickle.load(f)


# ### Prepare ARD for test boundary

# In[ ]:


ard = ard_preprocess(
        sat_file_links=sat_links,
        w_df=weather_df,
        sat_res_x=1,
        var_name=CONSTANTS["var_name"],
        interp_date_start=end_dt_w - timedelta(days=60),
        interp_date_end=end_dt_w,
        w_parms=w_parms,
        input_days=CONSTANTS["input_days"],
        output_days=CONSTANTS["output_days"],
        ref_tm=start_dt_w.strftime("%d-%m-%Y"),
        w_mn=weather_mean,
        w_sd=weather_std,
    )

frcst_st_dt  = end_dt_w


# In[ ]:


# raise exception if ARD is empty
if ard.shape[0] == 0:
    raise Exception("Analysis ready dataset is empty")
# raise exception if data spills into multiple rows
if ard.query("grp1_ > 0").shape[0] > 0:
    raise Exception(
        "More than one record has been found for more than one pixel"
    )
# warning if nans are in input data or data is out of bounds
if (
    ard.query("not nan_input_evi").shape[0] > 0
    or ard.query("not nan_input_w").shape[0] > 0
    or ard.query("not nan_output_w").shape[0] > 0
):
    print("Warning: NaNs found in the input data")
if (
    ard.query(
        "nan_input_evi and nan_input_w and nan_output_w and  not input_evi_le1"
    ).shape[0]
    > 0
):
    print("Warning: input data outside range of (-1,1) found")


# ### Load Model

# In[ ]:


# read model and weather normalization stats
model = tf.keras.models.load_model(CONSTANTS["modelh5"], compile=False)


# ### Model Predictions

# In[ ]:


# model prediction
label = model.predict(
    [
        np.array(ard.input_evi.to_list()),
        np.array(ard.input_weather.to_list()),
        np.array(ard.forecast_weather.to_list()),
    ]
)
label_names = [
    (frcst_st_dt + timedelta(days=i + 1)).strftime("%Y-%m-%d")
    for i in range(CONSTANTS["output_days"])
]


pred_df = pd.DataFrame(label[:, :, 0], columns=label_names).assign(
    lat=ard.lat_.values, long=ard.long_.values
)


# In[ ]:


pred_df.head()


# ### Write output to TIF files

# In[ ]:


get_ipython().run_line_magic('matplotlib', 'inline')
import time
from IPython import display
from rasterio.plot import show

output_dir = "results/"
ref_tif = sat_links.filePath.values[0]
with rasterio.open(ref_tif) as src:
    ras_meta = src.profile


# In[ ]:


for coln in pred_df.columns[:-2]: # Skip last 2 columns: lattiude, longitude
    data_array = np.array(pred_df[coln]).reshape(src.shape)
    with rasterio.open(os.path.join(output_dir, coln + '.tif'), 'w', **ras_meta) as dst:
        dst.write(data_array, indexes=1)


# ### Visualize EVI Forecast Maps

# In[ ]:


for coln in pred_df.columns[:-2]: # Skip last 2 columns: lattiude, longitude
    src = rasterio.open(os.path.join(output_dir, coln + '.tif'))
    show(src.read(), transform=src.transform, title=coln)
    #show_hist(src)
    display.clear_output(wait=True)
    time.sleep(1)  

