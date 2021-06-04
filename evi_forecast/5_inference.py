# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from IPython import get_ipython

# %% [markdown]
# Copyright (c) Microsoft Corporation. All rights reserved.
# 
# Licensed under the MIT License.
# %% [markdown]
# # Inference: EVI Forecast on Area of Interest using ML Webservice
# %% [markdown]
# ### Import libraries

# %%
# System Imports
import json
import os
import pickle
import requests
from datetime import datetime

# Third party libraries
import numpy as np
import pandas as pd

# Local Imports
from utils.config import farmbeats_config

# %% [markdown]
# ### Load the service endpoint and token

# %%
"""
Open service uri and token for https endpoint
"""

with open("results//service_uri.pkl", "rb") as f:
    scoring_uri, token = pickle.load(f)

# %% [markdown]
# ### Area of Interest (AOI) for inference

# %%
farmer_id = "contoso_farmer"
boundary_id = "sample-boundary-32" 
boundary_geometry = "[[-121.5283155441284,38.16172478418468],[-121.51544094085693,38.16172478418468],[-121.51544094085693,38.16791636919515],[-121.5283155441284,38.16791636919515],[-121.5283155441284,38.16172478418468]]"

# %% [markdown]
# ### Send Request to WebService

# %%
headers = {"Content-Type": "application/json"}
headers["Authorization"] = f"Bearer {token}"
test_data = json.dumps(
    {
        "config": farmbeats_config,
        "farmer_id": farmer_id,
        "boundary_id": boundary_id,
        "bonudary_geometry": json.loads(boundary_geometry)
    }
)
response = requests.post(
    scoring_uri, data=test_data, headers=headers, timeout=(300, 300)
)

# %% [markdown]
# ### Model Response Body

# %%
print(response.status_code)
print(response.elapsed)

# %% [markdown]
# ### Convert Model Response to DataFrame

# %%
pred_df = pd.DataFrame.from_dict(json.loads(response.content)['model_preds'])
pred_df.dropna().head()

# %% [markdown]
# ### Write Output to TIF Files

# %%
get_ipython().run_line_magic('matplotlib', 'inline')
import time
from IPython import display
import rasterio
from rasterio.plot import show
import shutil

ref_tif = json.loads(response.content)['ref_tif']
with rasterio.open(ref_tif) as src:
    ras_meta = src.profile
    
time_stamp = datetime.strptime(datetime.now().strftime("%d/%m/%y %H:%M:%S"), "%d/%m/%y %H:%M:%S")
output_dir = "results/model_output_"+str(time_stamp)+"/"
try:
    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.mkdir(output_dir)
except Exception as e:
    print(e)


# %%
for coln in pred_df.columns[:-2]: # Skip last 2 columns: lattiude, longitude
    try:   
        file_name = os.path.join(output_dir, coln + '.tif')
        if os.path.exists(file_name):
            os.remove(file_name)
        data_array = np.array(pred_df[coln].astype('float32')).reshape(src.shape)
        with rasterio.open(file_name, 'w', **ras_meta) as dst:
            dst.write(data_array, indexes=1)
    except Exception as e:
        print(e)

# %% [markdown]
# ### Visualize EVI Forecast Maps

# %%
for coln in pred_df.columns[:-2]: # Skip last 2 columns: lattiude, longitude
    try:
        file_name = os.path.join(output_dir, coln + '.tif')
        src = rasterio.open(file_name)
        show(src.read(), transform=src.transform, title=coln)
        #show_hist(src)
        display.clear_output(wait=True)
        time.sleep(1) 
    except Exception as e:
        print(e)


