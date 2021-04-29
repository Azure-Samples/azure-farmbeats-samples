import json
import numpy as np
import os
import pandas as pd
import rasterio
import xarray as xr

from datetime import datetime, timedelta
from statsmodels.nonparametric.smoothers_lowess import lowess


def ard_preprocess(
    sat_links1,
    w_df,
    sat_res_x,
    var_name,
    interp_date_start,
    interp_date_end,
    w_parms,
    input_days,
    output_days,
    ref_tm,
    w_mn,
    w_sd,
):

    """
    This method takes boundary satellite paths and weather data, creates Analysis Ready DataSet (ARD) 
    """
    sat_links1["sat_data"] = [
        rasterio.open(x).read(1) for x in sat_links1.filePath.values
    ]
    getgeo1 = rasterio.open(
        sat_links1.filePath.values[0]
    ).transform  # coordinates of farm    
    
    # GDAL: (258012.37107330866, 2.11668210080698, 0.0, 163176.6385398821, 0.0, -2.1168501270110074)
    # Raster io: (2.11668210080698, 0.0, 258012.37107330866,0.0, -2.1168501270110074, 163176.6385398821)
    sat_data = np.array(sat_links1.sat_data.values.tolist())
    
    
    msk = np.broadcast_to(
        np.mean(sat_data == 0, axis=0) < 1, sat_data.shape
    )  # mask for removing pixels with 0 value always
    sat_data1 = np.where(msk, sat_data, np.nan)[
        :, ::sat_res_x, ::sat_res_x
    ]  # spatial sampling
    
    
    idx = pd.date_range(interp_date_start, interp_date_end)  # interpolation range
    idx_time = pd.date_range(
        w_df.dateTime.sort_values().values[0][:10],
        w_df.dateTime.sort_values(ascending=False).values[0][:10],
    )
    # read satellite data into data array
    da = (
        xr.DataArray(
            sat_data1,
            [
                ("time", pd.to_datetime(sat_links1.sceneDateTime).dt.date),
                (
                    "lat",
                    getgeo1[2] + getgeo1[0] * sat_res_x * np.arange(sat_data1.shape[1]),
                ),
                (
                    "long",
                    getgeo1[5] + getgeo1[4] * sat_res_x * np.arange(sat_data1.shape[2]),
                ),
            ],
        )
        .to_dataframe(var_name)
        .dropna()
        .unstack(level=[1, 2])
    )
    
    # lowess smoothing to remove outliers and cubic sline interpolation
    xvals = (pd.Series(da.index) - da.index.values[0]).dt.days
    da_inter = pd.DataFrame(
        {
            x: lowess(da[x], xvals, is_sorted=True, frac=0.2, it=0)[:, 1]
            for x in da.columns
        }
    )
    da_inter.index = da.index
    da_inter1 = (
        da_inter.reindex(idx, fill_value=np.nan)
        .interpolate(method="cubic", limit_direction="both", limit=100)
        .reindex(idx_time, fill_value=np.nan)
    )

    # Read Weather Data and normalization
    w_df[w_parms] = (w_df[w_parms] - w_mn) / w_sd
    w_df["time"] = pd.to_datetime(w_df.dateTime).dt.date
    
    # combine interpolated satellite data array with weather data
    da1 = (
        da_inter1.stack([1, 2], dropna=False)
        .rename_axis(["time", "lat", "long"])
        .reset_index()
    )
    da1["time"] = pd.to_datetime(da1.time).dt.date
    da1 = da1.merge(w_df, on=["time"], how="inner").sort_values(["lat", "long", "time"])
    # remove unused data frames
    da = None
    da_inter = None
    da_inter1 = None
    # define group as every 40 days from referance time
    ref_tm1 = datetime.strptime(ref_tm, "%d-%m-%Y").date()
    da1["diffdays"] = (da1.time - ref_tm1).apply(lambda x: x.days)
    da1["grp1"] = (da1.diffdays / (input_days + output_days)).apply(np.floor)
    da1["d_remainder"] = da1.diffdays - da1.grp1 * (input_days + output_days)
    # defining input and forecast
    da1["label"] = np.where(da1.d_remainder.values < input_days, "input", "output")
    # combining NDVI and weather variables to a list variable
    da1["lst1"] = da1[[var_name] + w_parms].values.tolist()
    # remove data before growing season
    da2 = (
        da1.query("grp1 >= 0")
        .sort_values(["lat", "long", "label", "time"])
        .groupby(["lat", "long", "grp1", "label"])["lst1"]
        .apply(list)
        .to_frame()
        .unstack()
        .dropna(subset=[("lst1", "input"), ("lst1", "output")])
        .reset_index()
    )
    da1 = None
    da2.columns = ["_".join(col).strip() for col in da2.columns.values]
    # checking for input and output time steps are complete or not
    da2["len_input"] = np.array([len(x) for x in da2.lst1_input.values])
    da2["len_output"] = np.array([len(x) for x in da2.lst1_output.values])
    # removing rows with nan values and incomplete time steps
    da2 = da2.query(
        "len_input == " + str(input_days) + " and len_output == " + str(output_days)
    )
    # separating out NDVI/EVI from weather parameters
    da2["input_evi"] = np.array(da2.lst1_input.tolist())[:, :, 0:1].tolist()
    da2["input_weather"] = np.array(da2.lst1_input.tolist())[:, :, 1:].tolist()
    da2["forecast_weather"] = np.array(da2.lst1_output.tolist())[:, :, 1:].tolist()
    da2["output_evi"] = np.array(da2.lst1_output.tolist())[:, :, 0:1].tolist()
    da3 = da2[
        [
            "lat_",
            "long_",
            "grp1_",
            "input_evi",
            "input_weather",
            "forecast_weather",
            "output_evi",
        ]
    ]
    # checking for NDVI between - 1 and 1 in both input and output
    da3["input_evi_le1"] = (
        np.nanmax(np.abs(np.array(da3.input_evi.to_list())), axis=(1, 2)) <= 1
    )
    da3["output_evi_le1"] = (
        np.nanmax(np.abs(np.array(da3.output_evi.to_list())), axis=(1, 2)) <= 1
    )
    # checking for missing values
    da3["nan_input_evi"] = [
        np.sum(np.isnan(np.array(x))) == 0 for x in da3.input_evi.values
    ]
    da3["nan_input_w"] = [
        np.sum(np.isnan(np.array(x))) == 0 for x in da3.input_weather.values
    ]
    da3["nan_output_evi"] = [
        np.sum(np.isnan(np.array(x))) == 0 for x in da3.output_evi.values
    ]
    da3["nan_output_w"] = [
        np.sum(np.isnan(np.array(x))) == 0 for x in da3.forecast_weather.values
    ]
    return da3