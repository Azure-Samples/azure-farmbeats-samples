# System imports
import json

#3rd Party Imports
import numpy as np
import pandas as pd

class WeatherUtil:
    """
    provides utility functions for the weather data
    """
    @staticmethod
    def get_weather_data_df(weather_data:str) -> "DataFrame":
        """
        Creates pandas data frame for weather response provided by FarmBeats API
        :param weather_data: List of json
        :return: DataFrame
        """        
        # Check for empty response TODO: Raise error for this case
        if len(weather_data) == 0:
            print('Weather data is not available for the given inputs and check your inputs once!')
            return
                
        # Flatten out the json to a big data frame
        df_flat = pd.json_normalize([x.serialize() for x in weather_data])   
        df_flat_proceesed = df_flat.drop(columns=df_flat.columns[(df_flat == "n/a").any()]) # n/a string are present in ClearAg response
        # Rename columns with unit and drop these columns afterwards
        unit_cols= [col for col in df_flat_proceesed.columns if col.endswith('unit')]
        new_cols, curr_cols = [], []
        for unit_col in unit_cols:
            curr_col = unit_col.replace('.unit', '.value')
            if df_flat_proceesed[unit_col].dropna().nunique() == 1: 
                unit = df_flat_proceesed[unit_col].dropna().unique()[0]
            else: #TODO: Raise an error
                print('More than one unit type present for %s column or ', unit_col)    
            new_col =  curr_col.replace('.value', '-' + unit).replace('properties.', '')
            curr_cols.append(curr_col)
            new_cols.append(new_col)
        df_flat_proceesed.rename(columns = dict(zip(curr_cols, new_cols)), inplace = True)
        final_df = df_flat_proceesed.drop(columns=unit_cols)
        final_df.columns = final_df.columns.str.replace("properties.", "")
        
        return final_df
