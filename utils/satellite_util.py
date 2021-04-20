import os
import pandas as pd
import numpy as np

from azure.farmbeats import FarmbeatsClient
from azure.farmbeats.models import (
    Farmer,
    Farm,
    Field,
    Boundary,
    GeoJsonObject,
    Polygon,
    SatelliteIngestionJobRequest,
    DataProvider as SatelliteJobDataProvider,
    Source as SatelliteJobSource,
    SatelliteData as SatelliteJobData,
    ImageName as SatelliteJobImageName,
    ImageResolution as SatelliteJobImageResolutions,
    ImageFormat as SatelliteJobImageFormat,
    WeatherIngestionJobRequest,
)

def satellite_job_request(
    FB_Client,
    farm_geojson,
    farm_code,
    farm_id,
    field_id,
    boundary_id,
    farmer_id,
    out_file,
    farmer_name,
    start_dt,
    end_dt,
):
    # function for creation of boundary ID and satellite job
    # Return the Satellite job ID in the out_file
    # intended for use in _1_download_fb.py

    # Farm Creation
    farm = FB_Client.farms.create(
        farmer_id=farmer_id,
        farm_id=farm_id,
        farm=Farm(name=farmer_name + "'s SDK Farm " + farm_code),
    )

    # Field Creation

    field = FB_Client.fields.create(
        farmer_id=farmer_id,
        field_id=field_id,
        field=Field(name=farmer_name + "'s SDK Field " + farm_code, farm_id=farm_id),
    )

    # Boundary Creation
    boundary = FB_Client.boundaries.create(
        farmer_id=farmer_id,
        boundary_id=boundary_id,
        boundary=Boundary(
            name=farmer_name + "'s SDK Boundary " + farm_code,
            parent_id=field_id,
            is_primary=True,
            geometry=Polygon(coordinates=[farm_geojson]),
        ),
    )
    # creation of satellite job
    satellite_job = queue_sat_data(
        FB_Client=FB_Client,
        farmer_id=farmer_id,
        name1=farmer_name + "'s SDK Satellite Job " + farm_code,
        boundary_id=boundary_id,
        start_dt=start_dt,
        end_dt=end_dt,
    )
    print('Satellite job id:')
    print(satellite_job.id)
    try:
        id1 = satellite_job.id
        # save job id to file
        pd.DataFrame(
            {
                "farmcode": [farm_code],
                "polygon": [farm_geojson],
                "jobid": [id1],
                "farm_id": [farm_id],
                "field_id": [field_id],
                "boundary_id": [boundary_id],
                "farmer_id": [farmer_id],
            }
        ).to_csv(out_file, mode="a", header=False, index=False)
    except:
        print(farm_code)
return satellite_job

def queue_sat_data(FB_Client, farmer_id, name1, boundary_id, start_dt, end_dt):
    # starts satellite job in FarmBeats
    # intended for _5_inferancence_script and _1_download_fb.py
    satellite_job = FB_Client.jobs.queue_satellite_job(
        job=SatelliteIngestionJobRequest(
            name=name1,
            farmer_id=farmer_id,
            boundary_id=boundary_id,
            start_date=start_dt,
            end_date=end_dt,
            provider=SatelliteJobDataProvider.MICROSOFT,
            source=SatelliteJobSource.SENTINEL2_L2_A,
            data=SatelliteJobData(
                image_names=[
                "B01",
                ],
                image_resolutions=[
                    SatelliteJobImageResolutions.TEN,
                    SatelliteJobImageResolutions.TWENTY,
                    SatelliteJobImageResolutions.SIXTY,
                ],
                image_formats=[
                    SatelliteJobImageFormat.TIF,
                ],
            ),
        )
    )
return satellite_job

def get_sat_file_paths(FB_Client, boundary_ids, farmer_id, start_dt, end_dt, band_name):
    # Downloiad satellite tif files from FarmBeats to local suystem
    # intended for _1_download_fb.py and _4_scoring_file.py
    all_scenes = []
    for boundary_id in boundary_ids:
        scenes = FB_Client.scenes.get_all(
            farmer_id=farmer_id,
            boundary_id=boundary_id,
            start_date=start_dt,
            end_date=end_dt,
        )
        all_scenes.append(scenes)
    if np.sum([len(x) for x in all_scenes]) == 0:
        raise ValueError(
            "No scenes found between "
            + start_dt.strftime("%Y-%m-%d")
            + " and "
            + end_dt.strftime("%Y-%m-%d")
        )
    df_allscenes = pd.json_normalize(
        [y.serialize() for x in all_scenes for y in x],
        "imageFiles",
        [
            "id",
            "sceneDateTime",
            "boundaryId",
            "cloudCoverPercentage",
            "darkPixelPercentage",
        ],
    )
    # For NDVI files, filter on name
    # For EVI and other bands, cloud mask, darkpixel mask and other resolutions, similar filtering can be performed
    df_allscenes_band = df_allscenes.query(
        'name == "'
        + band_name
        + '" and resolution == 10 and cloudCoverPercentage == 0 and darkPixelPercentage < .1'
    )
    df_allscenes_band["boundary_count"] = df_allscenes_band.groupby(
        "boundaryId"
    ).name.transform(len)
    # Download files to local folder
    # the local path will be assigned by FarmBeats in the below format
    # C:\Users\dimattap\farmbeats\temp\scene_download\microsoft\sentinel_2_l2a\DIMATTAP_SDK_FARMER_1986\DIMATTAP_SDK_BOUNDARY_133_1986\2019-06-19\00-00-00\ndvi_10.tif
    df_allscenes_band["filePath"] = [
        os.path.normpath(FB_Client.scenes.download_scene_data(x))
        for x in df_allscenes_band.fileLink.values
    ]
return df_allscenes_band