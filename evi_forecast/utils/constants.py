"""
Specifications for each constant from here
that includes options for AKS compute, input days and output days specs, filenames
"""

CONSTANTS= {
    "weather_data_fldr": "results//",  # weather csv's to be saved here with name field_id.csv
    "sat_job_ids": "results//job_ids_11feb2021.csv",  # satellite job ids along with polygon will be saved in csv. Output for _1_download_fb
    "sat_file_paths": "satellite_paths.csv",  # filePaths of tif files. Output for _1_download_fb and imput for _2_build_model
    "weather_data_fldr": "C:\\azure-farmbeats-samples\\evi_forecast\\",
    
    # model specs
    "input_days": 30,  # input number of days for NDVI/EVI and weather
    "output_days": 10,  # number of days of weather forecast
    "ref_tm_model": "01-05-2020",  # start of growing season
    "sat_res_x_model": 10,  # spatial sampling in model training, this might differ for inferance
    "interp_date_start": "26-04-2020",  # model iterpolation start date
    "interp_date_end": "26-04-2021",  # model iterpolation start date
    
    # model results filenames
    "ardpkl": "results//ARD_18mar2021.pkl",  # Analysis ready dataset pickle file
    "w_pkl": "results//weather_parms_18mar2021.pkl",  # training data weather parameters list and sttaistics
    "modelh5": "results//model_100_100_100_18mar2021.h5",  # trained model in h5 format
    "model_result_png": "results//ANN_results_100_100_100_18mar2021.png",  # validation error results
    "var_name": "ndvi",
}