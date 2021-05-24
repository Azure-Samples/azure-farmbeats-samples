---
page_type: sample
languages:
- python
products:
- azure
description: "This repository contains Python samples for building ML models using Azure FarmBeats python SDK."
urlFragment: azure-FarmBeats-samples
---

# Microsoft Azure FarmBeats Samples (ML Models) for Python 

This template contains end to end Python samples/ Jupyter notebooks which demonstrate creating farms, boundaries, ingesting satellite and weather data, and building ML models related to AgriFood applications. It also contains a set of utilities for data processing.

## Features

These samples demonstrate the following features:

Introduction:
* sample.ipynb notebook demonstrates a how to create a farm, boundary, satellite job and weather job. A satellite job typically ingests data from satellite data provider (e.g., Sentinel) to Azure FarmBeats PaaS for a given location and duration. Similarly, weather ingests data into Azure Farmbeats Pass system from weather data provider (e.g., DTN ClearAg, DTN Content Services)

EVI-Forecast:

* An End-to-End ML Model sample that demonstrates how to pull satellite and weather data for 1000 farms using Azure FarmBeats SDK, how to create analysis ready datasets (ARD), ML model building/training, deploying ML model to Azure, and consume web service.

| Notebook | Description |  
| --- | --- |
| [1_download_data.ipynb](1_download_data.ipynb) | This notebook demonstrates how to download satellite and weather data for given boundaries to buidl EVI forecast model.
| [2_train.ipynb](2_train.ipynb) | This notebook demonstrates building end to end Deep Leanring model using satellite and weather data.
| [3_test.ipynb](3_test.ipynb) | In this notebook, model forecasts EVI for next 10 days on a new Area of Interest.
| [4_deploy_azure.ipynb](4_deploy_azure.ipynb) | This notebook demonstrates how to deploy model and create webservice using Azure ML SDK.
| [5_inference.ipynb](5_inference.ipynb) | This notebook demonstrates model inference on a new Area of Interest (AOI) using model webserive endpoint and generte EVI forecast for next 10 days in advance.

## Getting Started

### Prerequisites
1. An Azure FarmBeats Resource. If you don't have an Azure FarmBeats Resource, create one before you begin using the documation [here](https://portal.azure.com). 
2. Get FarmBeats Credentials (client id, client secret, instance url)
3. Get Credentials (APP_ID, APP_KEY) of Weather Provider. Follow the documentation [here](https://portal.azure.com). 


### Run Samples on Azure Machine Learning Service
1. First create [AML Compute](https://docs.microsoft.com/en-us/azure/machine-learning/how-to-create-attach-compute-studio) 

### Steps
1. Launch the terminal of AML compute
2. git clone https://github.com/Azure-Samples/azure-farmBeats-samples
3. Go to "Notebooks" pane and click on folder "azure-farmbeats-samples" under your alias
4. Go to folder evi_forecast and click on file 1_download_data.ipynb
5. Select compute and jupyter kernel (AzureML(py36))

## Contributing
This project doesn't accept contributions and suggestions at this moment!


## Resources
1. FarmBeats documentation link
2. FarmBeats Public Preview link