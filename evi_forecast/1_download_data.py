# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %% [markdown]
# # Data Download using Azure FarmBeats
# Download the required satellite and weather data using Azure FarmBeats PaaS.
# %% [markdown]
# ### Import 3rd party libraies

# %%
import json
import numpy as np
import os
import pandas as pd
import rasterio
from azure.identity import ClientSecretCredential
from datetime import datetime, timedelta

# Disable unnecessary logs 
import sys
import logging
logging.disable(sys.maxsize)

# %% [markdown]
# ### Import Farmbeats and Utilities

# %%
from azure.farmbeats.models import Farmer, Boundary, Polygon, SatelliteIngestionJobRequest, WeatherIngestionJobRequest
from azure.farmbeats import FarmBeatsClient

from utils.config import farmbeats_config
from utils.weather_util import WeatherUtil
from utils.satellite_util import SatelliteUtil
from utils.constants import CONSTANTS

# %% [markdown]
# ### Farmbeats Configuration

# %%
# FarmBeats Client definition
# Please make sure to change default values in config.py
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


# %%
RUN = 62
NO_BOUNDARIES = 2
root_dir = "/home/temp/" # Satellite data gets downloaded here

# %% [markdown]
# ### Create Farmer

# %%
farmer_id = "annam_farmer"
try:
    farmer = fb_client.farmers.get(farmer_id=farmer_id)
    if farmer is not None:
        print("Farmer Exists")
    else:
        print("Farmer doesn't exist...Creating one ", end="", flush=True)
        farmer = fb_client.farmers.create_or_update(
            farmer_id=farmer_id,
            farmer=Farmer()
        )
except Exception as e:
    print(e)

# %% [markdown]
# ### Create Boundaries

# %%
# Read 1000 farm geojsons from farms_1kmx1km.csv
locations_df = pd.read_csv("data//farms_sample_1kmx1km.csv")
locations_df["farms1"] = locations_df.farms.apply(json.loads)  # farm geojsons converted from string to list with numeric elements


# %%
boundaries = [] # List of boundaries

for i, boundary_polygon in enumerate(locations_df.farm_boundaries.values[:NO_BOUNDARIES]):
    boundary_id="boundary" + str(i) + str(RUN)
    try:
        boundary = fb_client.boundaries.get(
            farmer_id=farmer_id,
            boundary_id=boundary_id
        )
        if boundary is not None:
            print("Exist")
        else:
            print(f"Creating boundary with id {boundary_id}... ", end="")
            boundary = fb_client.boundaries.create_or_update(
                farmer_id=farmer_id,
                boundary_id=boundary_id,
                boundary=Boundary(
                    description="Created by SDK",
                    geometry=Polygon(
                        coordinates=[
                        item
                        ]
                    )
                )
            )
            print("Created")
    except Exception as e:
        print(e)
    boundaries.append(boundary)

# %% [markdown]
# ###  Submit Satellite Jobs

# %%
# Start and End data for Satellite and Weather data to be pulled
start_dt = datetime.strptime(CONSTANTS["interp_date_start"], "%d-%m-%Y")
end_dt = datetime.strptime(CONSTANTS["interp_date_end"], "%d-%m-%Y")


# %%
satellite_jobs = []
for i, boundary in enumerate(boundaries[:NO_BOUNDARIES]):
    job_id = "satellitejob"+ str(i) + str(RUN)
    
    # Submit Satellite Job
    try:
        print("Queuing satellite job... ", end="", flush=True)
        satellite_job = fb_client.scenes.begin_create_satellite_data_ingestion_job(
            job_id=job_id,
            job=SatelliteIngestionJobRequest(
                farmer_id=boundary.farmer_id,
                boundary_id=boundary.id,
                start_date_time=start_dt,
                end_date_time=end_dt,
            ),
            polling=True
        )
        print("Submitted Satellite Job")

    except HttpResponseError as e:
        print(e)
        raise
    satellite_jobs.append(satellite_job)

# %% [markdown]
# ### Check status of Satellite Jobs

# %%
for sat_job in satellite_jobs:
    print("Waiting")
    sat_job.result()

for sat_job in satellite_jobs:
    print(sat_job.result().as_dict()['id'])
    print(sat_job.status())
    
# TODO: Save job ids with Job request body to track failed jobs if any!

# %% [markdown]
# ### Submit Weather (Historical) Jobs

# %%
# Weather API inputs
extension_id = "dtn.clearAg"
extension_api_name = "dailyhistorical"
extension_data_provider_api_key = farmbeats_config["weather_provider_key"]
extension_data_provider_app_id = farmbeats_config["weather_provider_id"]


# %%
weather_jobs = []
for i, boundary in enumerate(boundaries[:NO_BOUNDARIES]):
    job_id = "weatherjob"+ str(i) + str(RUN)
    st_unix = int(start_dt.timestamp())
    ed_unix = int(end_dt.timestamp())
    
    try:
        print("Queuing weather job... ", end="", flush=True)
        weather_job = fb_client.weather.begin_create_data_ingestion_job(
            job_id=job_id,
            job=WeatherIngestionJobRequest(
                farmer_id=boundary.farmer_id,
                boundary_id=boundary.id,
                extension_id= extension_id, 
                extension_api_name= extension_api_name, 
                extension_api_input= {"start": st_unix, "end": ed_unix},
                extension_data_provider_api_key= extension_data_provider_api_key,
                extension_data_provider_app_id=extension_data_provider_app_id
            ),
            polling=True
        )
        print("Submitted Weather Job")
    except HttpResponseError as e:
        print(e)
        raise
    weather_jobs.append(weather_job)

# %% [markdown]
# ### Check status of Weather (Historical) Jobs

# %%
for wth_job in weather_jobs:
    print("Waiting")
    wth_job.result()

for wth_job in weather_jobs:
    print(wth_job.result().as_dict()['id'])
    print(wth_job.status())
    
# TODO: Save job ids with Job request body to track failed jobs if any!

# %% [markdown]
# ### Submit Weather (forecast) jobs

# %%
weather_forecast_jobs = []
START = 0
END = 10
extension_api_name = "dailyforecast"
for i, boundary in enumerate(boundaries[:NO_BOUNDARIES]):
    job_id = "weatherforecastjob"+ str(i) + str(RUN)
    
    try:
        print("Queuing weather job... ", end="", flush=True)
        weather_job = fb_client.weather.begin_create_data_ingestion_job(
            job_id=job_id,
            job=WeatherIngestionJobRequest(
                farmer_id=boundary.farmer_id,
                boundary_id=boundary.id,
                extension_id= extension_id, 
                extension_api_name= extension_api_name, 
                extension_api_input= {"start": START, "end": END},
                extension_data_provider_api_key= extension_data_provider_api_key,
                extension_data_provider_app_id=extension_data_provider_app_id
            ),
            polling=True
        )
        print("Submitted Weather Job")
    except HttpResponseError as e:
        print(e)
        raise
    weather_forecast_jobs.append(weather_job)

# %% [markdown]
# ### Check status of Weather (forecast) jobs

# %%
for wth_job in weather_forecast_jobs:
    print("Waiting")
    wth_job.result()

for wth_job in weather_forecast_jobs:
    print(wth_job.result().as_dict()['id'])
    print(wth_job.status())
    
# TODO: Save job ids with Job request body to track failed jobs if any!

# %% [markdown]
# ### Download Satellite Data to Local

# %%
df = SatelliteUtil(farmbeats_client = fb_client).download_and_get_sat_file_paths(farmer_id, boundaries, 
                                                                              start_dt, 
                                                                              end_dt, 
                                                                              root_dir)
df.to_csv("satellite_paths.csv", index=None)

# %% [markdown]
# ### Download Weather Data to Local

# %%
for boundary in boundaries:
    weather_list = fb_client.weather.list(
            farmer_id=  boundary.farmer_id,
            boundary_id= boundary.id,
            extension_id="dtn.clearAg", 
            weather_data_type= "historical", 
            granularity="daily")

    weather_data = []
    for w_data in weather_list:
        weather_data.append(w_data)

    w_df = WeatherUtil.get_weather_data_df(weather_data)
    w_df.to_csv(boundary.id + "_historical.csv", index=False)


# %%
for boundary in boundaries:
    weather_list = fb_client.weather.list(
            farmer_id=  boundary.farmer_id,
            boundary_id= boundary.id,
            extension_id="dtn.clearAg", 
            weather_data_type= "forecast", 
            granularity="daily")

    weather_data = []
    for w_data in weather_list:
        weather_data.append(w_data)

    w_df = WeatherUtil.get_weather_data_df(weather_data)
    w_df.to_csv(boundary.id + "_forecast.csv", index=False)
