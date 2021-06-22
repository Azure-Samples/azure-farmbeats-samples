---
page_type: sample
languages:
- python
products:
- azure
description: "This repository contains Python samples for building ML models using Azure FarmBeats python SDK."
urlFragment: azure-FarmBeats-python-samples
---

# Azure FarmBeats samples for Python 

This project contains end to end Python samples and Jupyter notebooks which demonstrate creating farms, boundaries, ingesting satellite and weather data, and building Machine Learning (ML) models related to agricultural applications. It also contains a set of utilities for data processing.

These samples leverage [Azure FarmBeats][product_docs] and its corresponding [Python SDK][azure-agrifood-farming] and it runs on [Azure Machine Learning Compute][aml-compute].

## Prerequisites

To run these samples, you must have:
- Azure subscription - [Create a free account][azure_subscription]
- Azure FarmBeats resource - [Install FarmBeats][install_farmbeats]
- A subscription with one of the supported weather data providers for FarmBeats - [Weather Integration Docs][farmbeats-weather-docs]
- Azure Machine Learning (AML) compute resource - [Create AML Compute][aml-compute-create] (with Python 3.6)

## Features & use case

This project aims to demonstrate following:

### Creation of farms, boundaries and ingesting satellite & weather data:
* [`quick_start.ipynb`](quick_start/quick_start.ipynb) notebook demonstrates how to create farms, boundaries, and ingestion jobs for satellite and weather data. A satellite ingestion job ingests satellite data (from Sentinel-2) to Azure FarmBeats data store for a given area of interest(AOI) and duration. Similarly, weather job ingests data into Azure Farmbeats store from weather data providers (example: DTN ClearAg, DTN Content Services)

### Build NDVI forecasting model:

* An end to end ML model sample that demonstrates how to pull satellite and weather data for any number of farms using Azure FarmBeats SDK, how to create analysis ready datasets (ARD), ML model building, training and deploying ML model to Azure using [Azure Machine Learning][azure-ml].

| Notebook | Description |  
| --- | --- |
| [`1_download_data.ipynb`](ndvi_forecast/1_download_data.ipynb) | This notebook demonstrates how to download satellite and weather data for given boundaries to build NDVI forecast model.|
| [`2_train.ipynb`](ndvi_forecast/2_train.ipynb) | This notebook demonstrates building end to end deep learning model using satellite and weather data.|
| [`3_test.ipynb`](ndvi_forecast/3_test.ipynb) | In this notebook, the model forecasts NDVI for next 10 days for an 'Area of Interest' (AOI).|
| [`4_deploy_azure.ipynb`](ndvi_forecast/4_deploy_azure.ipynb) | This notebook demonstrates how to deploy model and create webservice using Azure ML SDK.|
| [`5_inference.ipynb`](nddvi_forecast/5_inference.ipynb) | This notebook demonstrates model inference on a new AOI using the AzureML webservice endpoint and generates NDVI forecast for the next 10 days.|

## Getting Started


### Steps to run samples and build an NDVI forecasting model
1. Launch the terminal of AML compute.
2. Run the following command `git clone https://github.com/Azure-Samples/azure-farmBeats-samples`.
3. Open Azure Command Line Interface (CLI) and execute following commands to create an environment with all required libraries.
    <br />a. `cd azure-farmbeats-samples` (Go to azure-farmbeats-samples folder)
    <br />b. `conda env create -f environment.yml` (Create a new environemt 'farmbeats-sdk-env' with all required packages)
    <br />c. `python -m ipykernel install --user --name=farmbeats-sdk-env` (Activate the farmbeats-sdk-env' kernel in AML. Each notebook should select this kernel before running)
3. Go to `Notebooks` pane and click on folder `azure-farmbeats-samples` under your alias.
4. Select `compute` and `jupyter kernel` (AzureML(py36)).
5. Update [`utils/config.py`](ndvi_forecast/utils/config.py) with FarmBeats credentials.
6. Go to folder `ndvi_forecast` and run the notebooks enumerated in the table above starting with `1_download_data.ipynb`.


## Contributing
Please refer to [CONTRIBUTING.md](CONTRIBUTING.md)

<!-- LINKS -->
[aml-compute]:https://docs.microsoft.com/en-us/azure/machine-learning/concept-compute-instance
[aml-compute-create]:https://docs.microsoft.com/en-us/azure/machine-learning/how-to-create-manage-compute-instance?tabs=python#create
[azure-agrifood-farming]:https://pypi.org/project/azure-agrifood-farming/
[azure-ml]:https://azure.microsoft.com/en-in/services/machine-learning/
[azure_subscription]: https://azure.microsoft.com/free/
[farmbeats-weather-docs]: https://aka.ms/FarmBeatsWeatherDocs/
[install_farmbeats]: https://aka.ms/FarmBeatsInstallDocumentationPaaS/
[product_docs]: https://aka.ms/FarmBeatsProductDocumentationPaaS/
