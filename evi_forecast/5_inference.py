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


# In[ ]:


response.json()


# In[ ]:


print(response.status_code)
print(response.elapsed)


# In[ ]:


res1 = pd.DataFrame.from_dict(response.json())
print(res1.head())

