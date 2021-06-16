# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license.

# Standard library imports
import os
from itertools import tee
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import uuid
import pytz

# Third party imports
import pandas as pd
import numpy as np
import timezonefinder
from shapely import geometry

# Local imports
from utils.config import farmbeats_config

# Library specific imports
from azure.core.exceptions import HttpResponseError, ResourceNotFoundError
from azure.agrifood.farming import FarmBeatsClient
from azure.agrifood.farming.models import (Farmer, Boundary, Polygon,
                                    SatelliteDataIngestionJob,
                                    WeatherDataIngestionJob, 
                                    SatelliteData)


def get_sat_weather_data(fb_client, farmer_id, boundary_id, boundary_polygon, start_dt, end_dt):

    # Create Farmer
    try:
        farmer = fb_client.farmers.get(farmer_id=farmer_id)
        print(f"Farmer '{farmer_id}' exists.")
    except ResourceNotFoundError:        
        print(f"Farmer with id '{farmer_id}' doesn't exist. Creating ... ", end="", flush=True)
        farmer = fb_client.farmers.create_or_update(
            farmer_id=farmer_id,
            farmer=Farmer()
        )
        print(f"Farmer with id '{farmer_id}' created.")
    # Create boundary
    try:
        boundary = fb_client.boundaries.get(
            farmer_id=farmer_id,
            boundary_id=boundary_id
        )
        print(f"Boundary with id '{boundary.id}' exists", end="\n")
        
    except ResourceNotFoundError:
        print(f"Creating boundary with id '{boundary_id}'... ", end="")
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
        print("Boundary '{}' created".format(boundary.id))
    except Exception as e:
        print(e)

    # Satelitte job and check status of it
    sat_job_id = "satellitejob"+ str(uuid.uuid1())
    try:
        print(f"Queuing satellite job for boudary '{boundary.id}'. ", end="", flush=True)
        satellite_job = fb_client.scenes.begin_create_satellite_data_ingestion_job(
            job_id=sat_job_id,
            job=SatelliteDataIngestionJob(
                farmer_id=boundary.farmer_id,
                boundary_id=boundary.id,
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
        print(f"Submitted satellite job '{sat_job_id}'.")

    except HttpResponseError as e:
        print(e.response.body())
        raise

    # Weather (historical) job and status of it
    extension_id = farmbeats_config["weather_provider_extension_id"]
    extension_data_provider_api_key = farmbeats_config["weather_provider_key"]
    extension_data_provider_app_id = farmbeats_config["weather_provider_id"]
    extension_api_name = "dailyhistorical"

    w_hist_job_id = "w-historical" + str(uuid.uuid1())
    st_unix = int(start_dt.timestamp())
    ed_unix = int(end_dt.timestamp())
    try:
        print(f"Queuing weather job for boudary '{boundary.id}'. ", end="", flush=True)
        weather_hist_job = fb_client.weather.begin_create_data_ingestion_job(
            job_id=w_hist_job_id,
            job=WeatherDataIngestionJob(
                farmer_id=boundary.farmer_id,
                boundary_id=boundary.id,
                extension_id=extension_id, 
                extension_api_name=extension_api_name, 
                extension_api_input={"start": st_unix, "end": ed_unix},
                extension_data_provider_api_key=extension_data_provider_api_key,
                extension_data_provider_app_id=extension_data_provider_app_id
            ),
            polling=True
        )
        print(f"Submitted weather job '{w_hist_job_id}'.")
    except HttpResponseError as e:
        print(e.response.body())
        raise

    # Weather (forecast) job and status of it
    extension_api_name = "dailyforecast"
    w_forecast_job_id = "w-forecast"+ str(uuid.uuid1())
    
    try:
        print(f"Queuing weather job for boudary '{boundary.id}'. ", end="", flush=True)
        weather_forecast_job = fb_client.weather.begin_create_data_ingestion_job(
            job_id=w_forecast_job_id,
            job=WeatherDataIngestionJob(
                farmer_id=boundary.farmer_id,
                boundary_id=boundary.id,
                extension_id=extension_id,
                extension_api_name=extension_api_name,
                extension_api_input={"start": 0, "end": 10},
                extension_data_provider_api_key=extension_data_provider_api_key,
                extension_data_provider_app_id=extension_data_provider_app_id
            ),
            polling=True
        )
        print(f"Submitted weather job '{w_forecast_job_id}'.")
    except HttpResponseError as e:
        print(e.response.body())
        raise

    # Wait for all 3 jobs
    print('Waiting for all jobs to complete')
    satellite_job.result()
    weather_hist_job.result()
    weather_forecast_job.result()

    # Status of Jobs and raise error if any job fails
    print(f"Satellite job '{satellite_job.result().as_dict()['id']}' {satellite_job.status()}.")
    print(f"Weather job '{weather_hist_job.result().as_dict()['id']}' {weather_hist_job.status()}.")
    print(f"Weather job '{weather_forecast_job.result().as_dict()['id']}' {weather_forecast_job.status()}.")

def get_timezone(boundary_geometry: list):
    """
    Identify the time zone from boundary geometry
    :param boundary_geometry: list of boundary geometry (longitued and latitudes)
    :return: Timezone
    """
    try:
        P = geometry.Polygon(boundary_geometry)
        lng_centroid, lat_centroid = list(P.centroid.coords)[0]
        tf = timezonefinder.TimezoneFinder()
        timezone_str = tf.certain_timezone_at(lat=lat_centroid, lng=lng_centroid)
        return pytz.timezone(timezone_str)
    except Exception as e:
        return pytz.timezone('UTC')