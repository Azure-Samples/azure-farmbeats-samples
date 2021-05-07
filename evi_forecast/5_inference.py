#!/usr/bin/env python
# coding: utf-8

# # Inference

# In[ ]:


import numpy as np
import requests
import json
import pandas as pd
import pickle
from configs.config import config, farmer_details
from scripts.c_funcs.constants import CONSTANTS
from scripts.c_funcs.start_dwnld_in_fb import start_dwnld_in_fb
from datetime import datetime


boundary_id = 'boundary1'

"""
Open service uri and token for https endpoint
"""

with open(CONSTANTS["service_uri"], "rb") as f:
    scoring_uri, token = pickle.load(f)

headers = {"Content-Type": "application/json"}
headers["Authorization"] = f"Bearer {token}"
test_data = json.dumps(
    {
        "config": config,
        "farmer_id": farmer_details["farmer_id"],
        "field_id": field_id,
        "boundary_id": boundary_id,
    }
)
response = requests.post(
    scoring_uri, data=test_data, headers=headers, timeout=(240, 240)
)
res1 = pd.DataFrame.from_dict(response.json())
res1
print(response.status_code)
print(response.elapsed)

