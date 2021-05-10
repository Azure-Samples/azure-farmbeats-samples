#!/usr/bin/env python
# coding: utf-8

# # Inference

# In[ ]:


import numpy as np
import requests
import json
import pandas as pd
import pickle
from datetime import datetime

from utils.config import farmbeats_config


# In[ ]:


"""
Open service uri and token for https endpoint
"""

with open("results//service_uri.pkl", "rb") as f:
    scoring_uri, token = pickle.load(f)


# In[ ]:


token


# In[ ]:


farmer_id = 'contoso_farmer'
boundary_id = 'boundary1'


# In[ ]:


headers = {"Content-Type": "application/json"}
headers["Authorization"] = f"Bearer {token}"
test_data = json.dumps(
    {
        "config": farmbeats_config,
        "farmer_id": farmer_id,
        "boundary_id": boundary_id
    }
)
response = requests.post(
    scoring_uri, data=test_data, headers=headers, timeout=(240, 240)
)


# In[ ]:


print(response.status_code)
print(response.elapsed)


# In[ ]:


res1 = pd.DataFrame.from_dict(response.json())
print(res1.head())

