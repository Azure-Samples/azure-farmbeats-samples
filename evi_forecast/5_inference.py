#!/usr/bin/env python
# coding: utf-8

# # Inference

# Copyright (c) Microsoft Corporation. All rights reserved.
# 
# Licensed under the MIT License.

# In[ ]:


# System Imports
import json
import os
import numpy as np
import pandas as pd
import requests
from datetime import datetime

# Local Imports
from utils.config import farmbeats_config


# In[ ]:


"""
Open service uri and token for https endpoint
"""

with open("results//service_uri.pkl", "rb") as f:
    scoring_uri, token = pickle.load(f)


# In[ ]:


farmer_id = "annaresh_farmer"
boundary_id = "boundary1-annaresh" # TODO: Check later for geometry also
bonudary_geometry = '[[-88.55981782720959, 39.767198541032606], [-88.54924932608098, 39.766569945555425], [-88.55007951533537, 39.75856308368464], [-88.56064684852868, 39.75919160723301], [-88.55981782720959, 39.767198541032606]]'


# In[ ]:


headers = {"Content-Type": "application/json"}
headers["Authorization"] = f"Bearer {token}"
test_data = json.dumps(
    {
        "config": farmbeats_config,
        "farmer_id": farmer_id,
        "boundary_id": boundary_id,
        "bonudary_geometry": json.loads(bonudary_geometry)
    }
)
response = requests.post(
    scoring_uri, data=test_data, headers=headers, timeout=(240, 240)
)


# ### Model Response Body

# In[ ]:


print(response.json())


# In[ ]:


print(response.status_code)
print(response.elapsed)


# In[ ]:


pred_df = pd.DataFrame.from_dict(response['model_preds'].json())
print(pred_df.head())


# ### Write output to tif files

# In[ ]:


get_ipython().run_line_magic('matplotlib', 'inline')
import time
from IPython import display
from rasterio.plot import show

output_dir = "results/"
ref_tif = response['ref_tif']
with rasterio.open(ref_tif) as src:
    ras_meta = src.profile


# In[ ]:


for coln in pred_df.columns[:-2]: # Skip last 2 columns: lattiude, longitude
    data_array = np.array(pred_df[coln]).reshape(src.shape)
    with rasterio.open(os.path.join(output_dir, coln + '.tif'), 'w', **ras_meta) as dst:
        dst.write(data_array, indexes=1)


# ### Visualize Outputs

# In[ ]:


for coln in pred_df.columns[:-2]: # Skip last 2 columns: lattiude, longitude
    src = rasterio.open(os.path.join(output_dir, coln + '.tif'))
    show(src.read(), transform=src.transform, title=coln)
    #show_hist(src)
    display.clear_output(wait=True)
    time.sleep(1) 

