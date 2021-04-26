# Local Imports
from azure.farmbeats import FarmBeatsClient
from azure.farmbeats.models import Boundary, Polygon, SatelliteIngestionJobRequest
from azure.core.exceptions import HttpResponseError

# 3rd part Imports
import os
from pathlib import Path
import numpy as np
import pandas as pd
from urllib.parse import unquote, urlparse, parse_qs
from itertools import tee

class SatelliteUtil:
    """
    provides utility functions for the satellite data
    """

    def __init__(self, farmbeats_client: FarmBeatsClient):
        self.farmbeats_client = farmbeats_client
    
    def print_error(exception):
        print("Error:")
        try:
            pprint(exception.model.as_dict())
        except:
            print(exception.response.body())
            print("Couldn't print error info")

    def parse_file_path_from_file_link(self, file_link):
        return parse_qs(urlparse(file_link).query)['filePath'][0]


    def download_image(self, file_link, root_dir):
        print(f"Downloading image {file_link}... ", end="", flush=True)
        file_path = self.parse_file_path_from_file_link(file_link)
        out_path = Path(os.path.join(root_dir, file_path))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, 'wb') as tif_file:
            file_stream = self.farmbeats_client.scenes.download(file_path)
            for bits in file_stream:
                tif_file.write(bits)
        return out_path

    def download_scenes(self, boundary, start_date_time, end_date_time, root_dir):
        
        scenes = self.farmbeats_client.scenes.list(
            "annam_farmer",
            "boundary0102",
            start_date_time=start_date_time,
            end_date_time=end_date_time,
        )
        scenes, scenes_2 = tee(scenes)

        for scene in scenes:
            for image_file in scene.image_files:
                self.download_image(image_file.file_link, root_dir)
        
        return scenes_2


    def create_boundary(self, farmer_id, boundary_id, bbox):
        try:
            boundary = self.farmbeats_client.boundaries.get(
                farmer_id=farmer_id,
                boundary_id=boundary_id
            )
            
            if boundary is not None:
                print("Exist")
                return boundary
            else:
                print(f"Creating boundary with id {boundary_id}... ", end="")
                boundary = self.farmbeats_client.boundaries.create_or_update(
                    farmer_id=farmer_id,
                    boundary_id=boundary_id,
                    boundary=Boundary(
                        description="Created by SDK",
                        geometry=Polygon(
                            coordinates=[
                            bbox
                            ]
                        )
                    )
                )
            print("Done")
            return boundary
        except Exception as e:
            print(e)


    def queue_satellite_job(
        self,
        farmer_id, 
        boundary_id, 
        job_id, 
        start_date_time, 
        end_date_time, 
        polling
    ):
        try:
            print("Queuing satellite job... ", end="", flush=True)
            satellite_job = self.farmbeats_client.scenes.begin_create_satellite_data_ingestion_job(
                job_id=job_id,
                job=SatelliteIngestionJobRequest(
                    farmer_id=farmer_id,
                    boundary_id=boundary_id,
                    start_date_time=start_date_time,
                    end_date_time=end_date_time,
                ),
                polling=polling
            )
            """
            if polling:
                print("Waiting for result... ", end="", flush=True)
                result = satellite_job.result()
                print(f"Done with status {satellite_job.status()}")
                return result
            else:
            """
            print("Submitted Satellite Job")
            return satellite_job
                
        except HttpResponseError as e:
            print(e)
            raise
        #TODO: Save Failed Job IDs with Job Request Details


    def download_and_get_sat_file_paths(
        self, 
        farmer_id, 
        boundaries, 
        start_date_time, 
        end_date_time, 
        root_dir, 
        band_name = "NDVI"
    ):

        """
        Downloads scenes (all bands) to local and gets the local paths saved to file
        """
        
        all_scenes = []
        for boundary in boundaries:
            scenes = self.download_scenes(boundary, start_date_time, end_date_time, root_dir)
            all_scenes.append(scenes)

        """
        if np.sum([len(x) for x in all_scenes]) == 0:
            raise ValueError("No scenes found between "+ start_dt.strftime("%Y-%m-%d") + " and " + end_dt.strftime("%Y-%m-%d"))
        """
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

        df_allscenes_band["filePath"] = [
        os.path.normpath(self.download_image(x,root_dir))
        for x in df_allscenes_band.fileLink.values
        ]

        return df_allscenes_band