# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.

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
from utils.ard_util import ard_preprocess
from utils.config import farmbeats_config
from utils.constants import CONSTANTS
from utils.satellite_util import SatelliteUtil
from utils.test_helper import get_sat_weather_data
from utils.weather_util import WeatherUtil

# Azure imports
from azure.identity import ClientSecretCredential

# SDK imports
from azure.agrifood.farming import FarmBeatsClient


# -

# Called when the deployed service starts
def init():
    global model
    global w_parms
    global weather_mean
    global weather_std
    # read model and weather normalization stats
    model_path = os.getenv("AZUREML_MODEL_DIR") + "/"
    # For deploying pretrained model, use CONSTANTS["model_pretrained"]
    model = tf.keras.models.load_model(model_path + CONSTANTS["model_trained"], compile=False)
    with open(model_path + CONSTANTS["w_pkl"], "rb") as f:
        w_parms, weather_mean, weather_std = pickle.load(f)

def call_farmbeats(farmbeats_config):
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
    return fb_client

def get_ARD_df_scoring(fb_client, farmer_id, boundary_id, boundary_geometry):
    
    end_dt = datetime.strptime(datetime.now().strftime("%Y-%m-%d"), "%Y-%m-%d")
    start_dt = end_dt - timedelta(days=60)

    # Create Boundary and get satelite and weather (historical and forecast)
    get_sat_weather_data(fb_client, 
                    farmer_id, 
                    boundary_id,
                    boundary_geometry, 
                    start_dt, 
                    end_dt)

    # get boundary object
    boundary = fb_client.boundaries.get(
                farmer_id=farmer_id,
                boundary_id=boundary_id
            )
    
    root_dir = CONSTANTS['root_dir']
    sat_links = SatelliteUtil(farmbeats_client = fb_client).download_and_get_sat_file_paths(farmer_id, [boundary], start_dt, end_dt, root_dir)

    # get last available data of satellite data
    end_dt_w = datetime.strptime(
        sat_links.sceneDateTime.sort_values(ascending=False).values[0][:10], "%Y-%m-%d"
    )
    # calculate 30 days from last satellite available date
    start_dt_w = end_dt_w - timedelta(days=CONSTANTS["input_days"] - 1)
    
    
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
    
    weather_df = pd.concat([w_df_hist, w_df_forecast], axis=0, ignore_index=True)
    
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
    
    return ard, frcst_st_dt, sat_links.filePath.values[0]

# Handle requests to the service
def run(data):
    try:
        parms = json.loads(data)
        fb_client = call_farmbeats(parms["config"])
        farmer_id = parms["farmer_id"]
        boundary_id = parms["boundary_id"]
        boundary_geometry = parms["bonudary_geometry"]
        sat_res_x = parms.get("sat_res_x", 1)
        var_name = parms.get("var_name", "NDVI")
        sat_data_days = parms.get("sat_data_days", 60)
        if sat_data_days < 30:
            sat_data_days = 60
            print("Note: Satellite data for last 60 days will be downloaded")
            
        # prepare ARD for new data
        # frcst_st_dt reprresents last available scene of satellite
        # forecast will be done for 10 days from last available scene
        ard, frcst_st_dt, ref_tif = get_ARD_df_scoring(
            fb_client, 
            farmer_id,
            boundary_id, 
            boundary_geometry
            )
        
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

        # Prepare result and return output
        result = {'ref_tif': str(ref_tif), 'model_preds': tmp_df.to_dict()}
        return result
    
    except Exception as e:
        error = str(e)
        return error
