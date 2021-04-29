# Copyright (c) Microsoft. All rights reserved.
# Licensed under the MIT license.

# Standard library imports
import os
from itertools import tee
from pathlib import Path
from urllib.parse import urlparse, parse_qs

# Third party imports
import pandas as pd

# Library specific imports
from azure.core.exceptions import HttpResponseError
from azure.farmbeats import FarmBeatsClient


class SatelliteUtil:
    """provides utility functions for the satellite data."""

    def __init__(self, farmbeats_client: FarmBeatsClient):
        self.farmbeats_client = farmbeats_client
    
    def parse_file_path_from_file_link(self, file_link):
        return parse_qs(urlparse(file_link).query)['filePath'][0]

    def download_image(self, file_link, root_dir):
        file_path = self.parse_file_path_from_file_link(file_link)
        out_path = Path(os.path.join(root_dir, file_path))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, 'wb') as tif_file:
            file_stream = self.farmbeats_client.scenes.download(file_path)
            for bits in file_stream:
                tif_file.write(bits)
        return out_path

    def download_scenes(self, boundary, start_date_time, end_date_time, band_names, root_dir):   
        scenes = self.farmbeats_client.scenes.list(
            boundary.farmer_id,
            boundary.id,
            start_date_time=start_date_time,
            end_date_time=end_date_time,
            image_names=band_names
        )
        scenes, scenes_2 = tee(scenes)

        for scene in scenes:
            for image_file in scene.image_files:
                self.download_image(image_file.file_link, root_dir)      
        return scenes_2

    def download_and_get_sat_file_paths(
        self,
        farmer_id,
        boundaries,
        start_date_time,
        end_date_time,
        root_dir,
        band_names=["NDVI"]
    ):

        """
        Downloads scenes (all bands) to local and
        gets the local paths saved to file
        """
        print("Downloading Images to Local ...")
        all_scenes = []
        for boundary in boundaries:
            scenes = self.download_scenes(boundary, start_date_time, end_date_time, band_names, root_dir)
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
            + band_names[0]
            + '" and resolution == 10 and cloudCoverPercentage == 0 and darkPixelPercentage < .1'
        )

        df_allscenes_band.loc[:, 'boundary_count'] = df_allscenes_band.groupby(
            "boundaryId"
        ).name.transform(len)

        df_allscenes_band.loc[:, 'filePath'] = [
            Path(os.path.join(root_dir, self.parse_file_path_from_file_link(x)))
            for x in df_allscenes_band.fileLink.values
        ]
        print("Finished Downloading!")
        return df_allscenes_band
