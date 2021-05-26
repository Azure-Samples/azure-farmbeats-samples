# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license.

"""
Specifications for each constant from here
that includes options for AKS compute, input days and output days specs, filenames
"""

CONSTANTS= {
    # data directories
    "root_dir": "/tmp/farmbeats",  # Store the satellite and weather data

    # model specs
    "input_days": 30,  # input number of days for NDVI/EVI and weather
    "output_days": 10,  # number of days of weather forecast
    "ref_tm_model": "10-08-2020",  # start of growing season
    "sat_res_x_model": 10,  # spatial sampling in model training, this might differ for inferance
    "interp_date_start": "10-06-2020",  # model iterpolation start date
    "interp_date_end": "10-05-2021",  # model iterpolation start date
    
    # model results filenames
    "results_dir": "results/",
    "ardpkl": "results/ARD_18mar2021.pkl",  # Analysis ready dataset pickle file
    "w_pkl": "model/weather_parms_18mar2021.pkl",  # training data weather parameters list and sttaistics
    "modelh5": "model/model_100_100_100_18mar2021.h5",  # trained model in h5 format
    "model_result_png": "results/ANN_results_100_100_100_18mar2021.png",  # validation error results
    "var_name": "ndvi",
}