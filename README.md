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
- AgriFood (FarmBeats) resource - [Install FarmBeats][install_farmbeats]
- Azure Machine Learning (AML) Compute Resource - [Create AML Compute][aml-compute-create] (with Python 3.6)

## Features & use case

This project aims to demonstrate following things:

### Creation of farms, boundaries and ingesting satellite & weather data:
* `sample.ipynb` notebook demonstrates how to create farms, boundaries, and ingestion jobs for satellite and weather data. A satellite ingestion job ingests satellite data (from Sentinel-2) to Azure FarmBeats PaaS data store for a given boundary and duration. Similarly, weather job ingests data into Azure Farmbeats PaaS data store from weather data providers (example: DTN ClearAg, DTN Content Services)

### EVI forecasting model:

* An end-to-end ML model sample that demonstrates how to pull satellite and weather data for any number of farms using Azure FarmBeats SDK, how to create analysis ready datasets (ARD), ML model building, training and deploying ML model to Azure using [Azure Machine Learning][azure-ml].

| Notebook | Description |  
| --- | --- |
| [`1_download_data.ipynb`](evi_forecast/1_download_data.ipynb) | This notebook demonstrates how to download satellite and weather data for given boundaries to build EVI forecast model.
| [`2_train.ipynb`](evi_forecast/2_train.ipynb) | This notebook demonstrates building end to end Deep Leanring model using satellite and weather data.
| [`3_test.ipynb`](evi_forecast/3_test.ipynb) | In this notebook, the model forecasts EVI for next 10 days on a new Area of Interest.
| [`4_deploy_azure.ipynb`](evi_forecast/4_deploy_azure.ipynb) | This notebook demonstrates how to deploy model and create webservice using Azure ML SDK.
| [`5_inference.ipynb`](evi_forecast/5_inference.ipynb) | This notebook demonstrates model inference on a new Area of Interest (AOI) using the AzureML webservice endpoint and generte EVI forecast for next 10 days in advance.

## Getting Started


### Steps to run samples
1. Launch the terminal of AML compute
2. Run the following command `git clone https://github.com/Azure-Samples/azure-farmBeats-samples`
3. Go to `Notebooks` pane and click on folder `azure-farmbeats-samples` under your alias
4. Select `compute` and `jupyter kernel` (AzureML(py36))
5. Update [`utils/config.py`](evi_forecast/utils/config.py) with FarmBeats credentials 
6. Go to folder `evi_forecast` and start with `1_download_data.ipynb`


## Contributing
Please refer to [CONTRIBUTING.md](CONTRIBUTING.md)

<!-- LINKS -->
[aml-compute]:https://docs.microsoft.com/en-us/azure/machine-learning/concept-compute-instance
[aml-compute-create]:https://docs.microsoft.com/en-us/azure/machine-learning/how-to-create-manage-compute-instance?tabs=python#create
[api_docs]: https://aka.ms/FarmBeatsAPIDocumentationPaaS
[authenticate_with_token]: https://docs.microsoft.com/azure/cognitive-services/authentication?tabs=powershell#authenticate-with-an-authentication-token
[azure-agrifood-farming]:https://pypi.org/project/azure-agrifood-farming/
[azure-ml]:https://azure.microsoft.com/en-in/services/machine-learning/
[azure_identity_credentials]: https://github.com/Azure/azure-sdk-for-python/tree/master/sdk/identity/azure-identity#credentials
[azure_identity_pip]: https://pypi.org/project/azure-identity/
[azure_subscription]: https://azure.microsoft.com/free/
[change_log]: https://github.com/Azure/azure-sdk-for-python/tree/master/sdk/agrifood/azure-agrifood-farming/CHANGELOG.md
[cla]: https://cla.microsoft.com
[coc_contact]: mailto:opencode@microsoft.com
[coc_faq]: https://opensource.microsoft.com/codeofconduct/faq/
[code_of_conduct]: https://opensource.microsoft.com/codeofconduct/
[default_azure_credential]: https://github.com/Azure/azure-sdk-for-python/tree/master/sdk/identity/azure-identity#defaultazurecredential/
[farm_hierarchy]: https://aka.ms/FarmBeatsFarmHierarchyDocs
[farm_operations_docs]: https://aka.ms/FarmBeatsFarmOperationsDocumentation
[install_farmbeats]: https://aka.ms/FarmBeatsInstallDocumentationPaaS
[product_docs]: https://aka.ms/FarmBeatsProductDocumentationPaaS
[pip]: https://pypi.org/project/pip/
[pypi]: https://pypi.org/
[python]: https://www.python.org/downloads/
[python_logging]: https://docs.python.org/3.5/library/logging.html
[samples]: https://github.com/Azure/azure-sdk-for-python/tree/master/sdk/agrifood/azure-agrifood-farming/samples/
[scenes]: https://aka.ms/FarmBeatsSatellitePaaSDocumentation
[source_code]: https://github.com/Azure/azure-sdk-for-python/tree/master/sdk/agrifood/azure-agrifood-farming/
