# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from IPython import get_ipython

# %% [markdown]
# Copyright (c) Microsoft Corporation. All rights reserved.
# 
# Licensed under the MIT License.
# %% [markdown]
# # Azure FarmBeats: Satellite and Weather Data
# 
# In this notebook, the following things are demonstrated:
# 
# > * Create a Farmer
# > * Create boundaries
# > * How to submit satellite and weather (historical and forecast) jobs in FarmBeats PaaS for created boundaries
# > * Check the status of jobs in FarmBeats PaaS
# > * Download satellite data from FarmBeats PaaS to local compute
# > * Download weather data from FarmBeats PaaS to local compute
# 
# 
# In order to build EVI (Enhanced Vegetation Index) forecast model, you need satellite, historical weather and weather forecast data for the locations you want to train. This will be achieved easily using Azure FarmBeats python SDK. 
# %% [markdown]
# ### Import Libraries

# %%
get_ipython().system('pip install --quiet -r ../requirements-modelsamples.txt')


# %%
# Standard library imports
import json
import os
import sys
import uuid
from datetime import datetime
import time

# Disable unnecessary logs 
import logging
logging.disable(sys.maxsize)
import warnings
warnings.filterwarnings("ignore")

# Third party imports
import pandas as pd

# Local imports
from utils.config import farmbeats_config
from utils.constants import CONSTANTS
from utils.io_utils import IOUtil
from utils.satellite_util import SatelliteUtil
from utils.weather_util import WeatherUtil

# Azure imports
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.identity import ClientSecretCredential

# Azure FarmBeats SDK imports
from azure.agrifood.farming import FarmBeatsClient
from azure.agrifood.farming.models import (Farmer, Boundary, Polygon,
                                    SatelliteDataIngestionJob,
                                    WeatherDataIngestionJob, 
                                    SatelliteData)

# %% [markdown]
# ### FarmBeats Configuration
# Please follow the instructions here to create Azue Farmbeats resource and generate client id, client secrets, etc.. These values need to be added in config.py in utils folder accordingly
# 

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
    endpoint=farmbeats_config['instance_url'],
    credential=credential,
    credential_scopes=credential_scopes,
    logging_enable=True
)


# %%
RUN_ID = uuid.uuid1()  # This helps in creating unique job id everytime you run
NO_BOUNDARIES = 3  # Defaults 3;
root_dir = CONSTANTS['root_dir']  # Satellite data gets downloaded here

# %% [markdown]
# ### Create Farmer
# 
# Create a Farmer entity in FarmBeats system. You need to provide a farmer id as input

# %%
farmer_id = "contoso_farmer"
try:
    farmer = fb_client.farmers.get(farmer_id=farmer_id)
    if farmer is not None:
        print("Farmer {} Exists.".format(farmer_id))
    else:
        print("Farmer doesn't exist...Creating ... ", end="", flush=True)
        farmer = fb_client.farmers.create_or_update(
            farmer_id=farmer_id,
            farmer=Farmer()
        )
except Exception as e:
    print(e)

# %% [markdown]
# ### Create Boundaries
# 
# Reads boundary geojson objects from a csv file and create boundary entity in FarmBeats system per each geojson object. 
# 
# <b>Inputs:</b> Boundary geojson string, boundary id

# %%
# farms_sample_1kmx1km.csv file contains farm boundaries curated from Crop Data Layer [(CDL)] (https://www.nass.usda.gov/Research_and_Science/Cropland/SARS1a.php). The locations spread across continental USA.  
# You can plug-in your own locations in the same format
locations_df = pd.read_csv(os.path.join("data","farms_sample_1kmx1km.csv"))
locations_df["farm_boundaries"] = locations_df.farms.apply(json.loads)  # converted from string to list with numeric elements


# %%
boundaries = locations_df.farm_boundaries.values[:NO_BOUNDARIES]
boundary_objs = []  # List of boundaru objects

for i, boundary_polygon in enumerate(boundaries):
    boundary_id = "boundary" + str(i)
    try:
        boundary = fb_client.boundaries.get(
            farmer_id=farmer_id,
            boundary_id=boundary_id
        )
        print(f"Boundary with id {boundary.id} Exist", end="\n")
    except ResourceNotFoundError:        
        print(f"Creating boundary with id {boundary_id}... ", end="")
        boundary = fb_client.boundaries.create_or_update(
            farmer_id=farmer_id,
            boundary_id=boundary_id,
            boundary=Boundary(
                description="Created by SDK",
                geometry=Polygon(
                    coordinates=[
                        boundary_polygon
                    ]
                )
            )
        )
        print("Created")
    except Exception as e:
        print(e)
    boundary_objs.append(boundary)

#TODO: If Boundary ID + Different geometry given, needs force delete existing and create new one with same ID 

# %% [markdown]
# ###  Submit Satellite Jobs
# Create a satellite job for a given set of boundaries using Azure Farmbeats satellite_data_ingestion_job and SatellitDataIngestionJob() methods. 
# This returns a pollable object for each satellite job. We can query this object to know the status of each job until gets completed. Once the job succeeded, all satellite scenes will be downloaded in PaaS for given duration and location of intereset. 

# %%
# Start and End data for Satellite and Weather data to be pulled
start_dt = datetime.strptime(CONSTANTS["interp_date_start"], "%d-%m-%Y")
end_dt = datetime.strptime(CONSTANTS["interp_date_end"], "%d-%m-%Y")


# %%
satellite_jobs = []
for i, boundary_obj in enumerate(boundary_objs):
    job_id = "s-job"+ str(i) + str(RUN_ID)
    
    # Submit Satellite Job
    try:
        print("Queuing satellite job... ", end="", flush=True)
        satellite_job = fb_client.scenes.begin_create_satellite_data_ingestion_job(
            job_id=job_id,
            job=SatelliteDataIngestionJob(
                farmer_id=boundary_obj.farmer_id,
                boundary_id=boundary_obj.id,
                start_date_time=start_dt,
                end_date_time=end_dt,
                data=SatelliteData(
                    image_names=[
                        # "B01",
                        # "B02",
                        # "B03",
                        # "B04",
                        "NDVI"
                    ]
                )
            ),
            polling=True
        )
        print("Submitted Satellite Job")

    except HttpResponseError as e:
        print(e.response.body())
        raise
    satellite_jobs.append(satellite_job)

# %% [markdown]
# ### Check Status of Satellite Jobs
# Now, wait for the satellite jobs to be completed. We can check the status of each job which results in <i> succeeded </i> or <i> failed </i> or <i> waiting </i>. Needs further investigation for failed jobs and re-run the jobs if required!! 

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
# 
# Similar to satellite jobs, submit weather job for each boundary using azure farmbeats weather.begin_create_data_ingestion_job() and WeatherDataIngestionJob() methods. This returns the weather job objects for each boundary. 
# 
# This also require the details of weather data provider that you want to use. The details are specific to weather, but typically includes extension id, APP_KEY, APP_ID, etc. and these needs to be added to config.py acoordingly

# %%
# Weather API inputs
extension_id = farmbeats_config["weather_provider_extension_id"]
extension_data_provider_api_key = farmbeats_config["weather_provider_key"]
extension_data_provider_app_id = farmbeats_config["weather_provider_id"]
extension_api_name = "dailyhistorical"


# %%
weather_jobs = []
job_count = 0
for i, boundary_obj in enumerate(boundary_objs):
    job_id = "w-hist" + str(i) + str(RUN_ID)
    job_count += 1
    if job_count%100 == 0:
        print("job_count", job_count)
        time.sleep(60)
    st_unix = int(start_dt.timestamp())
    ed_unix = int(end_dt.timestamp())
    try:
        print("Queuing weather job... ", end="", flush=True)
        weather_job = fb_client.weather.begin_create_data_ingestion_job(
            job_id=job_id,
            job=WeatherDataIngestionJob(
                farmer_id=boundary_obj.farmer_id,
                boundary_id=boundary_obj.id,
                extension_id=extension_id, 
                extension_api_name=extension_api_name, 
                extension_api_input={"start": st_unix, "end": ed_unix},
                extension_data_provider_api_key=extension_data_provider_api_key,
                extension_data_provider_app_id=extension_data_provider_app_id
            ),
            polling=True
        )
        print("Submitted Weather Job")
    except HttpResponseError as e:
        print(e.response.body())
        raise
    weather_jobs.append(weather_job)

# %% [markdown]
# ### Check Status of Weather (Historical) Jobs
# Wait for weather jobs to get completed. Log the weather job ids which have failed and can be investigated further. The failed jobs can be submitted again the same weather.begin_create_data_ingestion_job() method. 

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
# Similar to historical weather data, we need weather forecast data for model training. Submit the jobs for each boundary using weather.begin_create_data_ingestion_job and provide extension_api_name as according to weather provider  (e.g., DTN ClearAg, the extension api name for forecast data is 'dailyforecast')

# %%
weather_forecast_jobs = []
job_count = 0
START = 0
END = 10
extension_api_name = "dailyforecast"
for i, boundary_obj in enumerate(boundary_objs):
    job_id = "w-fcast"+ str(i) + str(RUN_ID)
    job_count += 1
    if job_count % 100 == 0:
        print("job_count", job_count)
        time.sleep(60)
    try:
        print("Queuing weather job... ", end="", flush=True)
        weather_job = fb_client.weather.begin_create_data_ingestion_job(
            job_id=job_id,
            job=WeatherDataIngestionJob(
                farmer_id=boundary_obj.farmer_id,
                boundary_id=boundary_obj.id,
                extension_id=extension_id,
                extension_api_name=extension_api_name,
                extension_api_input={"start": START, "end": END},
                extension_data_provider_api_key=extension_data_provider_api_key,
                extension_data_provider_app_id=extension_data_provider_app_id
            ),
            polling=True
        )
        print("Submitted Weather Job")
    except HttpResponseError as e:
        print(e.response.body())
        raise
    weather_forecast_jobs.append(weather_job)

# %% [markdown]
# ### Check Status of Weather (forecast) jobs

# %%
for wth_job in weather_forecast_jobs:
    print("Waiting")
    wth_job.result()

for wth_job in weather_forecast_jobs:
    print(wth_job.result().as_dict()['id'])
    print(wth_job.status())
    
# TODO: Save job ids with Job request body to track failed jobs if any!

# %% [markdown]
# ### Download Satellite Data to Compute
# 
# Once the data has been ingested to Azure Farmbeats PaaS, it can be downloaded to your local machine or AML compute or Data Science VM.
# The data gets downloaded using scenes download method. This would be dependent on network bandwidth of your compute.

# %%
df = SatelliteUtil(farmbeats_client=fb_client).download_and_get_sat_file_paths(farmer_id, boundary_objs,
                                                                            start_dt,
                                                                            end_dt,
                                                                            root_dir)
# Write output to result directory
IOUtil.create_dir_safely(CONSTANTS["results_dir"])
df.to_csv(os.path.join(CONSTANTS["results_dir"], "satellite_paths.csv"), index=None)

# %% [markdown]
# ### Download Weather Data (Historical) to Compute
# 
# We query the weather data from Azure Farmbeats and the resposne is list of json object. This gets conveted into pandas dataframe (The typical data format for ML model inputs) and saved to your compute.

# %%
for boundary_obj in boundary_objs:
    weather_list = fb_client.weather.list(
            farmer_id=boundary_obj.farmer_id,
            boundary_id=boundary_obj.id,
            extension_id=extension_id,
            weather_data_type="historical",
            granularity="daily")

    weather_data = []
    for w_data in weather_list:
        weather_data.append(w_data)

    w_hist_df = WeatherUtil.get_weather_data_df(weather_data)
    w_hist_df.to_csv(os.path.join(root_dir, boundary_obj.id + "_historical.csv"), index=False)

print('Downloaded weather (historical) data!!')

# %% [markdown]
# ### Download Weather Data (Forecast) to Compute
# Similar to historical weather data, we query forecast data and save it to csv files. 

# %%
for boundary_obj in boundary_objs:
    weather_list = fb_client.weather.list(
            farmer_id=boundary_obj.farmer_id,
            boundary_id=boundary_obj.id,
            extension_id=extension_id, 
            weather_data_type="forecast", 
            granularity="daily")

    weather_data = []
    for w_data in weather_list:
        weather_data.append(w_data)

    w_frcst_df = WeatherUtil.get_weather_data_df(weather_data)
    w_frcst_df.to_csv(os.path.join(root_dir, boundary_obj.id + "_forecast.csv"), index=False)

print('Downloaded weather (forecast) data!!')


# %%
farmbeats_config


# %%



