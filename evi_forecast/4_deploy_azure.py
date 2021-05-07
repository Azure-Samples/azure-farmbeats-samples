#!/usr/bin/env python
# coding: utf-8

# # Deploy Model to Azure ML

# In[ ]:


from azureml.core import Workspace
from azureml.core.compute import AmlCompute, AksCompute, ComputeTarget
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


model = Model(name="NDVI_forecast_files", workspace=ws)


# #### Create Environment

# In[ ]:


conda_reqs = [
    "conda==4.7.12",
    "tensorflow==2.1.0",
    "scipy==1.4.1",
    "tensorboard==2.1.0",
    "rasterio",
    "scikit-learn",
    "xarray",
    "shapely",
]

pip_reqs = [
    "petastorm",
    "torchvision",
    "pyarrow",
    "statsmodels",
    "geotiff",
    "azureml-defaults",
]

py_version = "3.7.7"

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
    CONSTANTS["service_name"],
    [model],
    inference_config,
    deployment_config,
    aks_target,
    overwrite=True,
)
service.wait_for_deployment(True)


# In[ ]:


print(service.state)
print("scoring URI: " + service.scoring_uri)
token, refresh_by = service.get_token()
print(token)

with open("results//service_uri.pkl", "wb") as f:
    pickle.dump([service.scoring_uri, token], f)

