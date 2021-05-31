# To add a new cell, type '# %%'
# To add a new markdown cell, type '# %% [markdown]'
# %%
from IPython import get_ipython

# %% [markdown]
# # Inference: EVI Forecast on Area of Interest using ML Webservice
# %% [markdown]
# Copyright (c) Microsoft Corporation. All rights reserved.
# 
# Licensed under the MIT License.
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


# %%
scoring_uri = 'http://40.88.249.55:80/api/v1/service/ndviforecastservice/score'
token = 'eyJhbGciOiJSUzI1NiIsImtpZCI6IjY3MzJGREYzMEIwREU4NjMzREI5QTFBMkFBQTFBMzFGNjE4QjJCNTAiLCJ0eXAiOiJKV1QifQ.eyJjYW5SZWZyZXNoIjoiRmFsc2UiLCJ3b3Jrc3BhY2VJZCI6IjRlZWFkNmE0LWUzMjktNGNmNi1hM2EwLTllOGJhMGRhNmZhYSIsInRpZCI6IjcyZjk4OGJmLTg2ZjEtNDFhZi05MWFiLTJkN2NkMDExZGI0NyIsIm9pZCI6ImQxZDJhNjNlLWJlNzMtNDVkMS1hNTVlLTljNGFmNjY3MmYxNiIsImFjdGlvbnMiOiJbXCJNaWNyb3NvZnQuTWFjaGluZUxlYXJuaW5nU2VydmljZXMvd29ya3NwYWNlcy9yZWFkXCIsXCJNaWNyb3NvZnQuTWFjaGluZUxlYXJuaW5nU2VydmljZXMvd29ya3NwYWNlcy9zZXJ2aWNlcy9ha3Mvc2NvcmUvYWN0aW9uXCJdIiwic2VydmljZUlkIjoibmR2aWZvcmVjYXN0c2VydmljZSIsImV4cCI6MTYyMjU0MDM1MSwiaXNzIjoiYXp1cmVtbCIsImF1ZCI6ImF6dXJlbWwifQ.KHadO-PsMOZsZ14tsFPIxp_tlrlJtibrChqRF1A1JF8dVQzv9MygJaVlFQ7vrq3UNFQjsTNw0RqNatDbQZkWmC9eqlMHPt_r-CiGq2zyKKKaEl2ggz7ymOxCWKnVfhtAr3ixxorm96G45PXreXsCgTAdUpSZ26sRmvnUJZoAE5PTnp4p0AP6U3FKmwDT8DzpVrQ6oeQmJU1PWUO0FSJl8dROqjiw_OvoK2jzXvBHghTd4-vFEHyRYrBoRaES8E09XDbEVbbF4ElOtfj8jJoz1OYoFhbSIQkzZpKsp0aQMGSZv7_5_xU7QDa-Td68wGQdX4wVXqJO-pZl8UqnC14zBg'

# %% [markdown]
# ### Area of Interest (AOI) for inference

# %%
farmer_id = "contoso_farmer"
boundary_id = "sample-boundary-32" # TODO: Check later for geometry also
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
print(pred_df.head())

# %% [markdown]
# ### Write Output to TIF Files

# %%
get_ipython().run_line_magic('matplotlib', 'inline')
import time
from IPython import display
import rasterio
from rasterio.plot import show

output_dir = "results/"
ref_tif = json.loads(response.content)['ref_tif']
with rasterio.open(ref_tif) as src:
    ras_meta = src.profile


# %%
for coln in pred_df.columns[:-2]: # Skip last 2 columns: lattiude, longitude
    data_array = np.array(pred_df[coln].astype('float32')).reshape(src.shape)
    with rasterio.open(os.path.join(output_dir, coln + '.tif'), 'w', **ras_meta) as dst:
        dst.write(data_array, indexes=1)

# %% [markdown]
# ### Visualize EVI Forecast Maps

# %%
for coln in pred_df.columns[:-2]: # Skip last 2 columns: lattiude, longitude
    src = rasterio.open(os.path.join(output_dir, coln + '.tif'))
    show(src.read(), transform=src.transform, title=coln)
    #show_hist(src)
    display.clear_output(wait=True)
    time.sleep(1) 


