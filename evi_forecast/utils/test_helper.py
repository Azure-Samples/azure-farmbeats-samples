# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license.

# Standard library imports
import os
from itertools import tee
from pathlib import Path
from urllib.parse import urlparse, parse_qs
import uuid

# Third party imports
import pandas as pd

# Local imports
from utils.config import farmbeats_config

# Library specific imports
from azure.core.exceptions import HttpResponseError
from azure.farmbeats import FarmBeatsClient
from azure.farmbeats.models import (Farmer, Boundary, Polygon,
                                    SatelliteDataIngestionJob,
                                    WeatherDataIngestionJob, 
                                    SatelliteData)


def get_sat_weather_data(fb_client, farmer_id, boundary_id, boundary_polygon, start_dt, end_dt):

    # Create Farmer
    farmer = fb_client.farmers.get(farmer_id=farmer_id)
    if farmer is not None:
        print("Farmer {} Exists.".format(farmer_id))
    else:
        print("Farmer doesn't exist...Creating ... ", end="", flush=True)
        farmer = fb_client.farmers.create_or_update(
            farmer_id=farmer_id,
            farmer=Farmer()
        )
    # Create boundary
    try:
        boundary = fb_client.boundaries.get(
            farmer_id=farmer_id,
            boundary_id=boundary_id
        )
        if boundary is not None:
            print(f"Boundary with id {boundary.id} Exist", end="\n")
        else:
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

    # Satelitte job and check status of it
    sat_job_id = "satellitejob"+ str(uuid.uuid1())
    try:
        print("Queuing satellite job... ", end="", flush=True)
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
        print("Submitted Satellite Job")

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
        print("Queuing weather job... ", end="", flush=True)
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
        print("Submitted Weather Job")
    except HttpResponseError as e:
        print(e.response.body())
        raise

    # Weather (forecast) job and status of it
    extension_api_name = "dailyforecast"
    w_forecast_job_id = "w-forecast"+ str(uuid.uuid1())
    
    try:
        print("Queuing weather job... ", end="", flush=True)
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
        print("Submitted Weather Job")
    except HttpResponseError as e:
        print(e.response.body())
        raise

    # Wait for all 3 jobs
    satellite_job.result()
    weather_hist_job.result()
    weather_forecast_job.result()

    # Status of Jobs and raise error if any job fails
    print(satellite_job.status())
    print(weather_hist_job.status())
    print(weather_forecast_job.status())
