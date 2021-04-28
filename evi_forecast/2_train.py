# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %% [markdown]
# # Train EVI Forecast
# %% [markdown]
# ### Import Packages

# %%
# System Imports
from datetime import datetime
import numpy as np
import os
import pandas as pd
import pickle

# Local Imports
from utils.config import farmbeats_config
from utils.weather_util import WeatherUtil
from utils.satellite_util import SatelliteUtil
from utils.constants import CONSTANTS
from utils.ard_util import ard_preprocess

#3rd party Imports
import matplotlib.pyplot as plt
import tensorflow as tf
from tensorflow import keras
configuration = tf.compat.v1.ConfigProto()
configuration.gpu_options.allow_growth = True
session = tf.compat.v1.Session(config=configuration)
tf.test.gpu_device_name()

# %% [markdown]
# ### Get Satellite and Weatther Data

# %%
sat_links = pd.read_csv(CONSTANTS["sat_file_paths"])
sat_links["fileExist"] = sat_links.filePath.apply(os.path.exists)
sat_links.head()

# TODO: Check fileExist is True for all rows 


# %%
w_parms = [
    "airTempMin-F",
    "dewPointMin-F",
    "windSpeed-mph",
    "precipitation-in",
    "relativeHumidity-%",
    "temperature-F",
    "windSpeed2mAvg-mph",
    "snowAccPeriod-in",
    "liquidAccPeriod-in",
    "windSpeed2mMin-mph",
    "sunshineDuration-hours",
    "relativeHumidityMax-%",
    "relativeHumidityMin-%",
    "shortWaveRadiationAvg-W/m^2",
    "dewPointMax-F",
    "petPeriod-in",
    "windSpeedMin-mph",
    "iceAccPeriod-in",
    "airTempMax-F",
    "windSpeed2mMax-mph",
    "dewPoint-F",
    "cloudCover-%",
    "longWaveRadiationAvg-W/m^2",
    "windSpeedMax-mph",
]


# %%
trainval = (
    sat_links.drop_duplicates(["boundaryId", "fileExist"])
    .groupby(["boundaryId"])["fileExist"]
    .agg({"count"})
    .reset_index()
    .query("count == 1")
    .drop(["count"], axis=1)
)


trainval["weather_data_exists"] = (CONSTANTS["weather_data_fldr"] + trainval["boundaryId"] + "_historical.csv").apply(
    os.path.exists
)

trainval = trainval.query("w_exists")
np.random.seed(10)
trainval["trainval"] = np.where(
    np.random.uniform(0, 1, trainval.shape[0]) < 0.8, "Train", "Val"
)


# %%
# TODO: Tobe removed once no_boundaries are more than 1
trainval = pd.concat([trainval]*2, ignore_index=True)
trainval.loc[1,'trainval'] = 'Val'
trainval


# %%
# get mean and standard deviation of training data weather parameters for normalization
w_stats = pd.concat(
    [
        pd.read_csv(CONSTANTS["weather_data_fldr"] + x + "_historical.csv")
        for x in trainval.query('trainval == "Train"').boundaryId.values
    ],
    axis=0,
)[w_parms].agg({"mean", "std"})
w_mn = w_stats.filter(like="mean", axis=0)[w_parms].values
w_sd = w_stats.filter(like="std", axis=0)[w_parms].values


# %%
def get_ARD(boundaryId):
    # function for preparing Analysis Ready Dataset
    # intended for use in _2_build_model.py
    
    sat_links1 = sat_links.query(
        'boundaryId == @boundaryId'
    )
     
    # in reading w_df, if error occurs with farm_code, change it to field_id
    w_df = pd.read_csv(CONSTANTS["weather_data_fldr"] + boundaryId + "_historical.csv")
    
    da_pc = ard_preprocess(
        sat_links1=sat_links1,
        w_df=w_df,
        sat_res_x=CONSTANTS["sat_res_x_model"],
        var_name=CONSTANTS["var_name"],
        interp_date_start=CONSTANTS["interp_date_start"],
        interp_date_end=CONSTANTS["interp_date_end"],
        w_parms=w_parms,
        input_days=CONSTANTS["input_days"],
        output_days=CONSTANTS["output_days"],
        ref_tm=CONSTANTS["ref_tm_model"],
        w_mn=w_mn,
        w_sd=w_sd,
    )
    return da_pc.query(
        "nan_input_evi and nan_input_w and nan_output_evi and nan_output_w and input_evi_le1 and output_evi_le1"
    )

# %% [markdown]
# ### Create Analysis Ready Dataset

# %%
# Get analysis ready dataset
from concurrent.futures import ThreadPoolExecutor
with ThreadPoolExecutor(max_workers=100) as executor:
    ards_fetch = [executor.submit(get_ARD, x) for x in trainval.boundaryId.values]


# %%
ards_fetch[0].result()


# %%
da_fin = pd.concat(
    [
        ards_fetch[x]
        .result()
        .assign(
            farm_code=trainval.boundaryId.values[x], trainval=trainval.trainval.values[x]
        )
        for x in range(len(trainval.boundaryId.values))
        if ards_fetch[x].exception() == None
    ],
    axis=0,
)

# %% [markdown]
# ### Model Data Preparation

# %%
da_train = da_fin.query('trainval == "Train"')
da_val = da_fin.query('trainval == "Val"')

# Prepare train and validation tensors
# converting list variables in ARD DataFrame to numpy array (tensors)
X_train = [
    np.array(da_train.input_evi.to_list()),
    np.array(da_train.input_weather.to_list()),
    np.array(da_train.forecast_weather.to_list()),
]
Y_train = np.array(da_train.output_evi.to_list())
X_val = [
    np.array(da_val.input_evi.to_list()),
    np.array(da_val.input_weather.to_list()),
    np.array(da_val.forecast_weather.to_list()),
]
Y_val = np.array(da_val.output_evi.to_list())

# Save Analysis Ready Dataset (ARD)
os.makedirs(os.path.dirname(CONSTANTS["ardpkl"]), exist_ok=True)
with open(CONSTANTS["ardpkl"], "wb") as f:
    pickle.dump(da_fin, f)

# Save weather parameters normalization stats
os.makedirs(os.path.dirname(CONSTANTS["w_pkl"]), exist_ok=True)
with open(CONSTANTS["w_pkl"], "wb+") as f:
    pickle.dump([w_parms, w_mn, w_sd], f)

# %% [markdown]
# ### Model Architecture

# %%
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

# %% [markdown]
# ### Model Training

# %%
model = get_model(len(w_parms), 100, 100, 100)
optimizer = tf.keras.optimizers.SGD(learning_rate=0.1, momentum=0.9)
model.compile(loss="mse", optimizer=optimizer, metrics=["mse"])
# Model run
history1 = model.fit(
    X_train,
    Y_train,
    epochs=20,
    verbose=0,
    validation_data=(X_val, Y_val),
    callbacks=[],
    batch_size=1000,
)
val_pred = model.predict(X_val)
# Save model to h5 format
tf.keras.models.save_model(
    model, filepath= CONSTANTS["modelh5"], save_format="h5", overwrite=True
)

# %% [markdown]
# ### Visualization

# %%
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
    + str(np.round(np.sqrt(history1.history["val_mse"][-1]), 4))
)
plt.savefig(CONSTANTS["model_result_png"])

