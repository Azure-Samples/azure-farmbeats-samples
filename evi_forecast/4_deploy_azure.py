#!/usr/bin/env python
# coding: utf-8

# # Deploy Model to Azure ML

# In[ ]:


from azureml.core import Workspace
from azureml.core.compute import AmlCompute, AksCompute, ComputeTarget
from azureml.core.compute_target import ComputeTargetException
from azureml.core.conda_dependencies import CondaDependencies
from azureml.core.environment import Environment
from azureml.core.model import InferenceConfig, Model
from azureml.core.webservice import AksWebservice
import glob
import pickle
from utils.constants import CONSTANTS

import os


# #### Import Workspace Config

# In[ ]:


ws = Workspace.from_config(path=os.path.join('utils', 'ws_config.json'))


# #### Register Model

# In[ ]:


model = Model.register(
    model_path="model",
    model_name="NDVI_forecast_model",
    description="NDVI forecast ANN h5 file, weather parameter normalization mean and SD",
    workspace=ws,
)


# In[ ]:


model = Model(name="NDVI_forecast_model", workspace=ws)


# #### Create Environment

# In[ ]:


py_version = "3.6.9"

conda_reqs = [
    "conda==4.7.12",
    "tensorflow==2.1.0",
    "scipy==1.4.1",
    "tensorboard==2.1.0",
    "scikit-learn"
]

pip_reqs = [
    "petastorm",
    "torchvision",
    "pyarrow",
    "azureml-defaults",
    "geopandas==0.7.0",
    "numpy",
    "pandas==1.0.3",
    "rasterio==1.1.5",
    "shapely==1.7.0",
    "xarray",
    "statsmodels==0.12.2"
]

myenv = Environment(name="myenv")
conda_dep = CondaDependencies()
conda_dep.set_python_version(py_version)
conda_dep.add_channel("conda-forge")
whl_url = Environment.add_private_pip_wheel(
    workspace=ws, file_path=glob.glob("..//*.whl")[0], exist_ok=True
)
for x in conda_reqs:
    conda_dep.add_conda_package(x)

for x in pip_reqs + [whl_url]:
    conda_dep.add_pip_package(x)

myenv.python.conda_dependencies = conda_dep


# #### Create AKS 

# In[ ]:


# Adding Scoring file
inference_config = InferenceConfig(
    entry_script="scoring_file.py", source_directory=".//utils", environment=myenv
)

AKS_NAME = 'annareshaks1'
# Create the AKS cluster if not available
try:
    aks_target = ComputeTarget(workspace=ws, name=AKS_NAME)
except ComputeTargetException:
    prov_config = AksCompute.provisioning_configuration(vm_size="Standard_D3_v2")
    aks_target = ComputeTarget.create(
        workspace=ws, name=AKS_NAME, provisioning_configuration=prov_config
    )
    aks_target.wait_for_completion(show_output=True)


# #### Deploy

# In[ ]:


# deployment configuration of pods
deployment_config = AksWebservice.deploy_configuration(
    cpu_cores=1,
    memory_gb=2,
    token_auth_enabled=True,
    auth_enabled=False,
    scoring_timeout_ms=300000,
)

service = Model.deploy(
    ws,
    "ndviforecastservice",
    [model],
    inference_config,
    deployment_config,
    aks_target,
    overwrite=True,
)
service.wait_for_deployment(True)


# In[ ]:


service.get_logs()


# In[ ]:


print(ws.webservices)


# In[ ]:


from azureml.core import Webservice

service = Webservice(ws, 'ndviforecastservice')
print(service.get_logs())


# In[ ]:


print(service.state)
print("scoring URI: " + service.scoring_uri)
token, refresh_by = service.get_token()
print(token)

with open("results//service_uri.pkl", "wb") as f:
    pickle.dump([service.scoring_uri, token], f)

