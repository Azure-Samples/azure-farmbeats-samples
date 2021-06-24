---
page_type: sample
languages:
- python
products:
- azure
- azure-farmbeats
description: "This sample demonstrates how to build a NDVI forecast model using Azure FarmBeats Python SDK."
urlFragment: ndvi-forecast
---

# NDVI Forecast using Azure FarmBeats Python SDK
In this sample, a Normalized Difference Vegetation Index (NDVI) forecast model is built using satellite and weather (historical and forecast) datasets, which predicts NDVI for next 10 days in advance for a given Area of Interest (AOI).


This sample leverages [Azure FarmBeats][product_docs] and its corresponding [Python SDK][azure-agrifood-farming] and it runs on [Azure Machine Learning Compute][aml-compute].

## Prerequisites

To run these samples, you must have:
- Azure subscription - [Create a free account][azure_subscription]
- Azure FarmBeats resource - [Install FarmBeats][install_farmbeats]
- A subscription with one of the supported weather data providers for FarmBeats - [Weather Integration Docs][farmbeats-weather-docs]
- Azure Machine Learning (AML) compute resource - [Create AML Compute][aml-compute-create] (with Python 3.6)

## Running the sample

1. Launch the terminal of AML compute.
2. Run the following command `git clone https://github.com/Azure-Samples/azure-farmBeats-samples`.
3. Go to `Notebooks` pane and click on folder `azure-farmbeats-samples` under your alias.
4. Select `compute` and `jupyter kernel` (AzureML(py36)).
5. Update [`utils/config.py`](utils/config.py) with FarmBeats credentials.
6. Go to folder `ndvi_forecast` and run the notebooks enumerated in the table above starting with `1_download_data.ipynb`.

This sample comprises of the following notebooks:

| Notebook | Description |  
| --- | --- |
| [`1_download_data.ipynb`](1_download_data.ipynb) | This notebook demonstrates how to download satellite and weather data for given boundaries to build NDVI forecast model.|
| [`2_train.ipynb`](2_train.ipynb) | This notebook demonstrates building end to end deep learning model using satellite and weather data.|
| [`3_test.ipynb`](3_test.ipynb) | In this notebook, the model forecasts NDVI for next 10 days for an 'Area of Interest' (AOI).|
| [`4_deploy_azure.ipynb`](4_deploy_azure.ipynb) | This notebook demonstrates how to deploy model and create webservice using Azure ML SDK.|
| [`5_inference.ipynb`](5_inference.ipynb) | This notebook demonstrates model inference on a new AOI using the AzureML webservice endpoint and generates NDVI forecast for the next 10 days.|

## Contributing
Please refer to [CONTRIBUTING.md](../CONTRIBUTING.md)

<!-- LINKS -->
[aml-compute]:https://docs.microsoft.com/en-us/azure/machine-learning/concept-compute-instance
[aml-compute-create]:https://docs.microsoft.com/en-us/azure/machine-learning/how-to-create-manage-compute-instance?tabs=python#create
[azure-agrifood-farming]:https://pypi.org/project/azure-agrifood-farming/
[azure-ml]:https://azure.microsoft.com/en-in/services/machine-learning/
[azure_subscription]: https://azure.microsoft.com/free/
[farmbeats-weather-docs]: https://aka.ms/FarmBeatsWeatherDocs/
[install_farmbeats]: https://aka.ms/FarmBeatsInstallDocumentationPaaS/
[product_docs]: https://aka.ms/FarmBeatsProductDocumentationPaaS/