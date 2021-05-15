#!/usr/bin/env python
# coding: utf-8

# # Quick Start
# 
# In this notebook, we demonstrate the capabilitis of Azure Farmbeats python SDK

# In[ ]:


import sys
print(sys.executable)
print (sys.version)


# In[ ]:


from azure.farmbeats.models import Farmer


# In[ ]:


from configs.config import config, farmer_details


# ### Farmbeats Configuration

# In[ ]:


# FarmBeats Client definition
FB_Client = call_farmbeats(config)


# ### Create Farmer

# In[ ]:


# Farmer object creation if not yet created
farmer = FB_Client.farmers.create(
    farmer_id=farmer_details["farmer_id"],
    farmer=Farmer(
        name=farmer_details["farmer_name"]
        + "'s SDK Farmer "
        + str(farmer_details["TOKEN"])
    ),
)

