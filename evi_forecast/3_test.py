# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from IPython import get_ipython

# %% [markdown]
# # Test EVI Forecast (local)

# %%
# System Imports
import json
import pickle
import os
import requests
from datetime import datetime,timedelta

#3rd Party Imports
import numpy as np
import pandas as pd
import rasterio
import tensorflow as tf
from azure.identity import ClientSecretCredential
from tensorflow import keras


# Local Imports
from azure.farmbeats.models import Farmer, Boundary, Polygon, SatelliteIngestionJobRequest, WeatherIngestionJobRequest
from azure.farmbeats import FarmBeatsClient
from utils.config import farmbeats_config
from utils.weather_util import WeatherUtil
from utils.satellite_util import SatelliteUtil
from utils.constants import CONSTANTS
from utils.ard_util import ard_preprocess

# Disable unnecessary logs
import sys
import logging
logging.disable(sys.maxsize)

# %% [markdown]
# ### Farmbeats Configuration

# %%
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

# %% [markdown]
# ### Forecast EVI for test Boundary
# %% [markdown]
# #### Satellite Data

# %%
farmer_id = "annam_farmer"
boundary_id = "boundary055"

end_dt = datetime.strptime(datetime.now().strftime("%Y-%m-%d"), "%Y-%m-%d")
start_dt = end_dt - timedelta(days=60)

# get boundary object
boundary = fb_client.boundaries.get(
            farmer_id=farmer_id,
            boundary_id=boundary_id
        )

root_dir = "/home/temp/"


# %%
sat_links1 = SatelliteUtil(farmbeats_client = fb_client).download_and_get_sat_file_paths(farmer_id, [boundary], start_dt, end_dt, root_dir)

# get last available data of satellite data
end_dt_w = datetime.strptime(
    sat_links1.sceneDateTime.sort_values(ascending=False).values[0][:10], "%Y-%m-%d"
)
# calculate 30 days from last satellite available date
start_dt_w = end_dt_w - timedelta(days=CONSTANTS["input_days"] + 1)

# %% [markdown]
# #### Weather Data

# %%
# get weather data historical
weather_list = fb_client.weather.list(
            farmer_id=  boundary.farmer_id,
            boundary_id= boundary.id,
            start_date_time=start_dt_w,
            end_date_time=end_dt,
            extension_id="dtn.clearAg", 
            weather_data_type= "historical", 
            granularity="daily")
weather_data = []
for w_data in weather_list:
    weather_data.append(w_data)
w_df_hist = WeatherUtil.get_weather_data_df(weather_data)


# %%
# get weather data forecast
weather_list = fb_client.weather.list(
            farmer_id=  boundary.farmer_id,
            boundary_id= boundary.id,
            start_date_time=end_dt,
            end_date_time=end_dt + timedelta(10),
            extension_id="dtn.clearAg", 
            weather_data_type= "forecast", 
            granularity="daily")

weather_data = []
for w_data in weather_list:
    weather_data.append(w_data)
w_df_forecast = WeatherUtil.get_weather_data_df(weather_data)


# %%
# merge weather data
w_df = pd.concat([w_df_hist, w_df_forecast], axis=0)

with open(CONSTANTS["w_pkl"], "rb") as f:
    w_parms, w_mn, w_sd = pickle.load(f)


# %%
ard = ard_preprocess(
        sat_links1=sat_links1,
        w_df=w_df,
        sat_res_x=1,
        var_name=CONSTANTS["var_name"],
        interp_date_start=end_dt_w - timedelta(days=60),
        interp_date_end=end_dt_w,
        w_parms=w_parms,
        input_days=CONSTANTS["input_days"],
        output_days=CONSTANTS["output_days"],
        ref_tm=start_dt_w.strftime("%d-%m-%Y"),
        w_mn=w_mn,
        w_sd=w_sd,
    )

frcst_st_dt  = end_dt_w


# %%
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


# %%
# read model and weather normalization stats
model = tf.keras.models.load_model(CONSTANTS["modelh5"], compile=False)


# %%
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


tmp_df = pd.DataFrame(label[:, :, 0], columns=label_names).assign(
    lat=ard.lat_.values, long=ard.long_.values
)


# %%
tmp_df

# %% [markdown]
# ### Write output to TIF files

# %%
get_ipython().run_line_magic('matplotlib', 'inline')
import time
from IPython import display
from rasterio.plot import show

output_dir = "results//"
ref_tif = sat_links1.filePath.values[0]
with rasterio.open(ref_tif) as src:
    ras_meta = src.profile


# %%
for coln in tmp_df.columns[:-2]:
    data_array = np.array(tmp_df[coln]).reshape(src.shape)
    with rasterio.open(os.path.join(output_dir, coln + '.tif'), 'w', **ras_meta) as dst:
        dst.write(data_array, indexes=1)

# %% [markdown]
# ### Visualize EVI Forecast Maps

# %%
for coln in tmp_df.columns[:-2]:
    src = rasterio.open(os.path.join(output_dir, coln + '.tif'))
    show(src.read(), transform=src.transform, title=coln)
    #show_hist(src)
    display.clear_output(wait=True)
    time.sleep(1)  


