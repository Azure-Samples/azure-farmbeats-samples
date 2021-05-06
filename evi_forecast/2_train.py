#!/usr/bin/env python
# coding: utf-8

# Copyright (c) Microsoft Corporation. All rights reserved.
# 
# Licensed under the MIT License.

# # Train EVI Forecast

# ### Import libraries

# In[ ]:


# Standard library imports
import os
import pickle
import sys
from datetime import datetime

# Third party imports
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import tensorflow as tf
from tensorflow import keras

# Disable unnecessary logs 
import logging
logging.disable(sys.maxsize)
import warnings
warnings.filterwarnings("ignore")

# Local  imports
from utils.config import farmbeats_config
from utils.constants import CONSTANTS
from utils.ard_util import ard_preprocess
from utils.satellite_util import SatelliteUtil
from utils.weather_util import WeatherUtil


# ### Get Satellite and Weatther Data

# In[ ]:


root_dir = CONSTANTS['root_dir']


# #### Load satellite data  local paths

# In[ ]:


sat_links = pd.read_csv(os.path.join(CONSTANTS["results_dir"], "satellite_paths.csv"))
sat_links["fileExist"] = sat_links.filePath.apply(os.path.exists)
sat_links.head()

# TODO: Check fileExist is True for all rows and raise error  


# #### List of weather parameter used in model training

# In[ ]:


weather_parms = [
    'airTempMax-F', 
    'airTempMin-F', 
    'cloudCover-%', 
    'dewPoint-F', 
    'dewPointMax-F', 
    'dewPointMin-F', 
    'iceAccPeriod-in', 
    'liquidAccPeriod-in', 
    'longWaveRadiationAvg-W/m^2', 
    'petPeriod-in', 
    'precipitation-in', 
    'relativeHumidity-%', 
    'relativeHumidityMax-%', 
    'relativeHumidityMin-%', 
    'shortWaveRadiationAvg-W/m^2', 
    'snowAccPeriod-in', 
    'sunshineDuration-hours', 
    'temperature-F', 
    'windSpeed-mph', 
    'windSpeed2mAvg-mph', 
    'windSpeed2mMax-mph', 
    'windSpeed2mMin-mph', 
    'windSpeedMax-mph', 
    'windSpeedMin-mph'
]


# ### Prepare Train and Validation sets

# In[ ]:


# Combine satellite file paths and weather file per boundary

trainval = (
    sat_links.drop_duplicates(["boundaryId", "fileExist"])
    .groupby(["boundaryId"])["fileExist"]
    .agg({"count"})
    .reset_index()
    .query("count == 1")
    .drop(["count"], axis=1)
)


# Check for weather file exists or not
trainval["w_exists"] = (trainval["boundaryId"] + "_historical.csv").apply(lambda x: os.path.join(root_dir, x)).apply(
    os.path.exists
)

trainval = trainval.query("w_exists")


# In[ ]:


# Split data into train and validation sets in 80% and 20% respectively
np.random.seed(10)
trainval["trainval"] = np.where(
    np.random.uniform(0, 1, trainval.shape[0]) < 0.8, "Train", "Val"
)


# In[ ]:


print(trainval)


# In[ ]:


# get mean and standard deviation of training data weather parameters for normalization
w_stats = pd.concat(
    [
        pd.read_csv(os.path.join(root_dir, x + "_historical.csv"))
        for x in trainval.query('trainval == "Train"').boundaryId.values
    ],
    axis=0,
)[weather_parms].agg({"mean", "std"})


# ### Get weather statistics for Normalization

# In[ ]:


# get mean and standard deviation of training data weather parameters for normalization
w_stats = pd.concat(
    [
        pd.read_csv(os.path.join(root_dir, x + "_historical.csv"))
        for x in trainval.query('trainval == "Train"').boundaryId.values
    ],
    axis=0,
)[weather_parms].agg({"mean", "std"})


weather_mean = w_stats.filter(like="mean", axis=0)[weather_parms].values
weather_std = w_stats.filter(like="std", axis=0)[weather_parms].values

# Save weather parameters normalization stats
os.makedirs(os.path.dirname(CONSTANTS["w_pkl"]), exist_ok=True)
with open(CONSTANTS["w_pkl"], "wb+") as f:
    pickle.dump([weather_parms, weather_mean, weather_std], f)


# In[ ]:


def get_ARD(boundaryId):
    # function for preparing Analysis Ready Dataset
    # intended for use in _2_build_model.py
    
    boundary_id_sat_links = sat_links.query(
        'boundaryId == @boundaryId'
    )
     
    # in reading w_df, if error occurs with farm_code, change it to field_id
    w_df = pd.read_csv(os.path.join(root_dir, boundaryId + "_historical.csv"))
    
    da_pc = ard_preprocess(
        sat_file_links=boundary_id_sat_links,
        w_df=w_df,
        sat_res_x=20,
        var_name=CONSTANTS["var_name"],
        interp_date_start=CONSTANTS["interp_date_start"],
        interp_date_end=CONSTANTS["interp_date_end"],
        w_parms=weather_parms,
        input_days=CONSTANTS["input_days"],
        output_days=CONSTANTS["output_days"],
        ref_tm=CONSTANTS["ref_tm_model"],
        w_mn=weather_mean,
        w_sd=weather_std,
    )
    return da_pc.query(
        "nan_input_evi and nan_input_w and nan_output_evi and nan_output_w and input_evi_le1 and output_evi_le1"
    )


# ### Create Analysis Ready Dataset

# In[ ]:


# Get analysis ready dataset
from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=100) as executor:
    ards_fetch = [executor.submit(get_ARD, x) for x in trainval.boundaryId.values]


# In[ ]:


ards_fetch[0].result() # sample model input xarray of first boundary


# In[ ]:


data = pd.concat(
    [
        ards_fetch[x]
        .result()
        .assign(
            boundary_code=trainval.boundaryId.values[x], trainval=trainval.trainval.values[x]
        )
        for x in range(len(trainval.boundaryId.values))
        if ards_fetch[x].exception() == None
    ],
    axis=0,
)


# ### Model Data Preparation

# In[ ]:


data_train = data.query('trainval == "Train"')
data_val = data.query('trainval == "Val"')

# Prepare train and validation tensors
# converting list variables in ARD DataFrame to numpy array (tensors)
X_train = [
    np.array(data_train.input_evi.to_list()),
    np.array(data_train.input_weather.to_list()),
    np.array(data_train.forecast_weather.to_list()),
]
Y_train = np.array(data_train.output_evi.to_list())


X_val = [
    np.array(data_val.input_evi.to_list()),
    np.array(data_val.input_weather.to_list()),
    np.array(data_val.forecast_weather.to_list()),
]
Y_val = np.array(data_val.output_evi.to_list())

# Save Analysis Ready Dataset (ARD)
os.makedirs(os.path.dirname(CONSTANTS["ardpkl"]), exist_ok=True)
with open(CONSTANTS["ardpkl"], "wb") as f:
    pickle.dump(data, f)


# ### Model Architecture

# In[ ]:


def get_model(input_weather, x, y, z):
    """
    Model architecture
    """
    # intended for use in _2_build_model.py
    # Define the tensors for the three input images
    evi_input = keras.Input((CONSTANTS["input_days"], 1), name="evi_input")
    weather_input = keras.Input(
        (CONSTANTS["input_days"], input_weather), name="weather_input"
    )
    forecast_input = keras.Input(
        (CONSTANTS["output_days"], input_weather), name="forecast_input"
    )

    dense_1 = keras.layers.LSTM(
        x, activation="relu", name="DeNse_1", dropout=0.1, recurrent_dropout=0.1
    )(evi_input)
    dense_2 = keras.layers.LSTM(
        y, activation="relu", name="DeNse_2", dropout=0.1, recurrent_dropout=0.1
    )(weather_input)
    dense_3 = keras.layers.LSTM(
        z,
        activation="relu",
        name="lstm_1",
        return_sequences=True,
        dropout=0.1,
        recurrent_dropout=0.1,
    )(forecast_input)

    dense_12 = keras.layers.concatenate(axis=-1, inputs=[dense_1, dense_2])
    dense_12_1 = keras.layers.RepeatVector(10)(dense_12)
    dense_123 = keras.layers.concatenate(axis=-1, inputs=[dense_12_1, dense_3])
    prediction = keras.layers.LSTM(
        1, activation="relu", name="lstm_2", return_sequences=True
    )(dense_123)
    # Connect the inputs with the outputs
    finnet = keras.Model(
        inputs=[evi_input, weather_input, forecast_input], outputs=prediction
    )
    # return the model
    return finnet


# ### Model Training

# In[ ]:


model = get_model(len(weather_parms), 100, 100, 100)
optimizer = tf.keras.optimizers.SGD(learning_rate=0.1, momentum=0.9)
model.compile(loss="mse", optimizer=optimizer, metrics=["mse"])
# Model run
training_history = model.fit(
    X_train,
    Y_train,
    epochs=20,
    verbose=1,
    validation_data=(X_val, Y_val),
    callbacks=[],
    batch_size=1000,
)
val_pred = model.predict(X_val)
# Save model to h5 format
tf.keras.models.save_model(
    model, filepath= CONSTANTS["modelh5"], save_format="h5", overwrite=True
)


# ### Visualization

# In[ ]:


# visualize model error as function of forecast days
err_pred = Y_val[:, :, 0] - val_pred[:, :, 0]
err_base = -Y_val[:, :, 0] + X_val[0][:, -1, 0][:, np.newaxis]
df_err_mn = pd.DataFrame(
    {
        "Day_i_vs_Day_0": np.sqrt(np.mean(err_base ** 2, axis=0)),
        "Predicted_vs_Actual": np.sqrt(np.mean(err_pred ** 2, axis=0)),
        "Day": 1 + np.arange(CONSTANTS["output_days"]),
    }
).set_index(["Day"])
df_err_mn.plot()
plt.suptitle(
    "RMSE plot comparing Day i to Day 0 vs Day i to ANN model prediction\n Validation RMSE: "
    + str(np.round(np.sqrt(training_history.history["val_mse"][-1]), 4))
)
plt.savefig(CONSTANTS["model_result_png"])

