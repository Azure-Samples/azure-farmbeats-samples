# System imports
import json

# Local Imports
from azure.farmbeats import FarmbeatsClient

#3rd Party Imports
import numpy as np
import pandas as pd
from pandas.io.json import json_normalize

class WeatherUtil:
    """
    provides utility functions for the weather data
    """

    def __init__(self, farmbeats_client: FarmbeatsClient):
        self.farmbeats_client = farmbeats_client
    
    def fetch_weather(
        farmer_id,
        field_id,
        start_dt,
        end_dt,
        weather_data_type,
        out_folder=False,
    ):
        """
        # Fetch weather data for a farm which has complete job from get_weather
        # if out_folder is not False, the data will be saved to CSV file in out_folder folder with name as field_id
        # weather_data_type is historical or forecast
        """
        weather_data = farmbeats_client.weatherdata.get_weather_data(
            farmer_id=farmer_id,
            field_id=field_id,
            start_date_time=start_dt,
            end_date_time=end_dt,
            extension_id="DTN.ClearAg",
            weather_data_type=weather_data_type,
            granularity="daily",
        )
        # raise error if empty object
        if len(weather_data) == 0:
            raise ValueError(
                field_id
                + ": No "
                + weather_data_type
                + " data found between "
                + start_dt.strftime("%Y-%m-%d")
                + " and "
                + end_dt.strftime("%Y-%m-%d")
            )
        # below code is for converting weather data from json to DataFrame
        # Serialize weather object to DataFrame
        weather_df = pd.json_normalize([x.serialize() for x in weather_data])
        weather_df.columns = [x.replace("properties.", "") for x in weather_df.columns]
        w_colnames = pd.DataFrame(
            [x.split(".") for x in weather_df.columns], columns=["parm", "value"]
        ).query('value == "unit" or value == "value"')
        for x in np.unique(w_colnames.parm.values):
            if not (
                x + ".unit" in weather_df.columns and x + ".value" in weather_df.columns
            ):
                pass
            for unit in np.unique(weather_df[x + ".unit"].tolist()):
                if unit != "nan":
                    weather_df[x + "-" + unit] = np.where(
                        unit == weather_df[x + ".unit"].values,
                        weather_df[x + ".value"],
                        np.nan,
                    )
            del weather_df[x + ".unit"], weather_df[x + ".value"]
        # if out_folder is provided, data will be saved to csv
        if out_folder != False:
            weather_df.to_csv(out_folder + field_id + ".csv", index=False)
        return weather_df

    def get_weather(
        FB_Client,
        farmer_id,
        field_id,
        start_dt,
        end_dt,
        api_name,
        name,
        config,
        convert_2_unix=True,
    ):
        """
        # function for queueing weather job for a Farm
        # intended for _5_inferance_script and _1_download_fb.py
        # apiname = dailyhistorical or dailyforecast
        # convert_2_unix is true if apiname is dailyhistorical, else it should be false
        """
        
        try:
            if convert_2_unix:
                st_unix = int(start_dt.timestamp())  # UNIX start date
                ed_unix = int(end_dt.timestamp())  # UNIX end date
            else:
                st_unix, ed_unix = start_dt, end_dt
            weather_job = FB_Client.jobs.queue_weather_job(
                job=WeatherIngestionJobRequest(
                    name=name,
                    farmer_id=farmer_id,
                    field_id=field_id,
                    extension_id="DTN.ClearAg",
                    api_name=api_name,  # "dailyhistorical",
                    provider_input={"start": st_unix, "end": ed_unix},
                ),
                x_ms_farm_beats_data_provider_key=config["weather_provider_key"],
                x_ms_farm_beats_data_provider_id=config["weather_provider_id"],
            )
            # if want to check status of job, use below code
            # FB_Client.jobs.wait_for_job(job_id = weather_job.id)
            print("Weather ID")
            print(weather_job.id)
        
        except Exception as e:
            return e
            
        return weather_job