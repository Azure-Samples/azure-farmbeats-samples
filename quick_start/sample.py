#!/usr/bin/env python
# coding: utf-8

# # Quick Start
# 
# In this notebook, we demonstrate the capabilitis of Azure Farmbeats python SDK

# In[1]:


import sys
print(sys.executable)
print (sys.version)


# ### Import Libraries

# In[2]:


get_ipython().system('pip install --quiet -r ../requirements-modelsamples.txt')


# In[3]:


# Standard library imports
import json
import os
import sys
import uuid
from datetime import datetime

# Disable unnecessary logs 
import logging
logging.disable(sys.maxsize)
import warnings
warnings.filterwarnings("ignore")

# Local imports
from config import farmbeats_config

# Azure imports
from azure.core.exceptions import HttpResponseError
from azure.identity import ClientSecretCredential

# Azure FarmBeats SDK imports
from azure.farmbeats import FarmBeatsClient
from azure.farmbeats.models import (Farmer, Boundary, Polygon,
                                    SatelliteDataIngestionJob,
                                    WeatherDataIngestionJob, 
                                    SatelliteData)


# ### Farmbeats Configuration

# In[4]:


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


# ### Create Farmer

# In[5]:


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


# ### Create Boundary

# In[6]:


boundary_id = "contoso_boundary"


boundary_obj = fb_client.boundaries.get(
            farmer_id=farmer_id,
            boundary_id=boundary_id
        )


if boundary_obj is not None:
    print(f"Boundary with id {boundary_obj.id} Exist", end="\n")
            
else:
    
    print(f"Creating boundary with id {boundary_id}... ", end="")
    boundary_obj = fb_client.boundaries.create_or_update(
        farmer_id=farmer_id,
        boundary_id=boundary_id,
        boundary=Boundary(
            description="Created by SDK",
            geometry=Polygon(
                 coordinates=[
                    [
                        [79.27057921886444, 18.042507660177698],
                        [79.26899135112762, 18.040135849620704],
                        [79.27113711833954, 18.03927382882835],
                        [79.27248358726501, 18.041069275656195],
                        [79.27057921886444, 18.042507660177698]
                    ]
                ]
            )
        )
    )

    print('Created boundary')


# ### Satellite and Weather Jobs

# In[7]:


RUN_ID = uuid.uuid1()
# Start and End data for Satellite and Weather data to be pulled
start_dt = datetime.strptime("01-01-2021", "%d-%m-%Y")
end_dt = datetime.strptime("30-04-2021", "%d-%m-%Y")


# In[8]:


job_id = "s-job" + str(RUN_ID)

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


# In[ ]:


# Weather API inputs
extension_id = farmbeats_config["weather_provider_extension_id"]
extension_data_provider_api_key = farmbeats_config["weather_provider_key"]
extension_data_provider_app_id = farmbeats_config["weather_provider_id"]
extension_api_name = "dailyhistorical"


# In[ ]:


job_id = "w-hist" + str(RUN_ID)
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


# ### Check status of satellite and weather jobs

# In[ ]:


print("Waiting for jobs to complete")
satellite_job.result()
weather_job.result()

# Print job id and status after succeeded
print(satellite_job.result().as_dict()['id'])
print(satellite_job.status())

print(weather_job.result().as_dict()['id'])
print(weather_job.status())

