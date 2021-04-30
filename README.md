---
page_type: sample
languages:
- python
products:
- azure
description: "This repository contains Python samples for building ML models using Azure Farmbeats python SDK."
urlFragment: azure-farmbeats-samples
---

# Microsoft Azure Farmbeats Samples (ML Models) for Python 

This template contains end to end Python samples/ jupyter notebooks which demonstrate creating farms, boundaries, ingesting satellite and weather data and building ML models related to AgriFood Applications. It also contains a set of utilities helps in data processing.

## Features

These samples demonstrates the following features:

Introduction:
* It shows a sample how to create a farm, boundary, satellite job and weather job. 

EVI-Forecast:

* An End to End ML Model sample that demostrate how to pull the satellite and weather data for 1k farms using Azure Farmbeats SDK, how to create ARD dataset, ML model building, deploying ML model to Azure, and consume web service.


## Getting Started

### Prerequisites
1. An Azure Farmbeats Resource. If you don't have an Azure Farmbeats Resource, create one before you begin. 
2. Follow steps here to create Azure Farmnbeats Resource
3. Get FarmBeats Credentials (client id, client secret, instance url)
3. Get Credentials (APP_ID, APP_KEY) of Weather Provider


### Run Samples on Azure Machine Learning Service
1. First create [AML Compute](https://docs.microsoft.com/en-us/azure/machine-learning/how-to-create-attach-compute-studio) 

### Steps
1. Launch the terminal of AML compute
2. git clone https://github.com/Azure-Samples/azure-farmbeats-samples
3. Go "Notebooks" pane and go to your username and click folder "azure-farmbeats-samples"  
4. Go to evi_forecast/1_download_data.ipynb
5. Go to compute and click on 'jupyter' and it will take you to broswer
6. Go to evi_forecast/1_download_data.ipynb

## Contributing
This project doesn't accept contributions and suggestions at this moment!


## Resources
