from pprint import pprint
import time
from azure.core.exceptions import HttpResponseError
from azure.identity import ClientSecretCredential
from azure.farmbeats import FarmBeatsClient
from azure.farmbeats.models import Farmer, Boundary, SatelliteIngestionJobRequest, Polygon, WeatherIngestionJobRequest
import random
from datetime import datetime, timedelta
from pytz import utc
from urllib.parse import unquote, urlparse, parse_qs
import os
from pathlib import Path

from utils.satellite_util import SatelliteUtil
from utils.constants import CONSTANTS



def ensure_farmer(client, farmer_id):
    try:
        print(
            f"Create/updating farmer with id {farmer_id}... ", end="", flush=True)
        farmer = client.farmers.create_or_update(
            farmer_id=farmer_id,
            farmer=Farmer()
        )
        print("Done")
        return farmer
    except HttpResponseError as e:
        print("Ooops... here's the error:")
        print_error(e)
        return e

def ensure_boundary(client, farmer_id, boundary_id):
    try:
        print(f"Checking if boundary with id {boundary_id} exists... ", end="", flush=True)
        boundary = client.boundaries.get(
            farmer_id=farmer_id,
            boundary_id=boundary_id
        )
        print("Exists")
        return boundary
    except HttpResponseError as e:
        if e.status_code == 404:
            print("Boundary doesn't exist... ", end="", flush=True)
            return create_boundary(farmer_id, boundary_id)
        else:
            raise


def create_boundary(client, farmer_id, boundary_id):
    try:
        print(f"Creating boundary with id {boundary_id}... ", end="")
        boundary = client.boundaries.create_or_update(
            farmer_id=farmer_id,
            boundary_id="agadhika-boundary",
            boundary=Boundary(
                description="Created by SDK",
                geometry=Polygon(
                    coordinates=[
                        [
                            [79.27057921886444, 18.042507660177698],
                            [79.26899135112762, 18.040135849620704],
                            [79.27113711833954, 18.03927382882835],
                            [79.27248358726501, 18.041069275656195],
                            [79.27057921886444, 18.042507660177698]
                        ]
                    ]
                )
            )
        )
        print("Done")
        return boundary
    except HttpResponseError as e:
        print_error(e)


def queue_satellite_job(client, boundary, start_date_time, end_date_time, polling=False):
    try:
        print("Queuing satellite job... ", end="", flush=True)
        satellite_job = client.scenes.begin_create_satellite_data_ingestion_job(
            job_id=f"agadhika-sdk-job-{random.randint(0, 1000)}",
            job=SatelliteIngestionJobRequest(
                farmer_id="agadhika-farmer",
                boundary_id="agadhika-boundary",
                start_date_time=start_date_time,
                end_date_time=end_date_time,
            ),
            polling=polling
        )
        if polling:
            print("Waiting for result... ", end="", flush=True)
            result = satellite_job.result()
            print(f"Done with status {satellite_job.status()}")
            return result
        else:
            print("Done")
            return satellite_job
            
    except HttpResponseError as e:
        print_error(e)
        raise

def queue_weather_job(self, 
        farmer_id, 
        boundary_id, 
        job_id, 
        start_date_time, 
        end_date_time, 
        polling):

    st_unix = int(start_date_time.timestamp())  # UNIX start date
    ed_unix = int(end_date_time.timestamp())  # UNIX end date
    try:
        print("Queuing weather job... ", end="", flush=True)
        weather_job = fb_client.weather.begin_create_data_ingestion_job(
            job_id=f"agadhika-sdk-weather-job-{random.randint(0, 1000)}",
            job=WeatherIngestionJobRequest(
                farmer_id="agadhika-farmer",
                boundary_id="agadhika-boundary",
                extension_id= "dtn.clearAg", 
                extension_api_name="dailyhistorical", 
                extension_api_input= {"start": st_unix, "end": ed_unix},
                extension_data_provider_api_key= "c743a0f0d236e4115aef962a1eed0709",
                extension_data_provider_app_id="95615ca0"
            ),
            polling=polling
        )
        # if polling:
        #     print("Waiting for result... ", end="", flush=True)
        #     result = weather_job.result()
        #     print(f"Done with status {weather_job.status()}")
        #     return result
        # else:
        print("Submitted Weather Job")
        return weather_job
            
    except HttpResponseError as e:
        print_error(e)
        raise


def print_error(exception):
    print("Error:")
    try:
        pprint(exception.model.as_dict())
    except:
        print(exception.response.body())
        print("Couldn't print error info")

# def wait_for_satellite_job(client, job, polling_interval=1, timeout=300):
#     poll_start_time = datetime.now()
#     while datetime.now() - poll_start_time < timedelta(seconds=timeout):
#         try:
#             job_description = client.scenes.get_satellite_data_ingestion_job_details(job.id)
#             if job_description.job_status in ["Succeeded", "Failed", "Cancelled"]:
#                 return job_description
#             if job_description.job_status in ["Waiting", "Running"]:
#                 continue
#             else:
#                 pprint(job_description.as_dict())
#                 raise ValueError(f"Unexpected value of job_status: {job_description.job_status}")
#         except HttpResponseError as e:
#             print_error(e)
#             raise

def download_scenes(client, start_date_time, end_date_time, boundary, root_dir):
    scenes = client.scenes.list(
        boundary.farmer_id,
        boundary.id,
        start_date_time=start_date_time,
        end_date_time=end_date_time,
    )

    for scene in scenes:
        for image_file in scene.image_files:
            download_image(client, image_file.file_link, root_dir)

def parse_file_path_from_file_link(file_link):
    return parse_qs(urlparse(file_link).query)['filePath'][0]


def download_image(client, file_link, root_dir):
    print(f"Downloading image {file_link}... ", end="", flush=True)
    file_path = parse_file_path_from_file_link(file_link)
    out_path = Path(os.path.join(root_dir, file_path))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, 'wb') as tif_file:
        file_stream = client.scenes.download(file_path)
        for bits in file_stream:
            tif_file.write(bits)
    print("Done")


def download_weather(client, boundary):

    weather_list = client.weather.list(
        farmer_id=  boundary.farmer_id,
        boundary_id= boundary.id,
        extension_id="dtn.clearAg", 
        weather_data_type= "historical", 
        granularity="daily")
    
    w_list = []
    for w_data in weather_list:
        w_list.append(w_data)

    return w_list
    
def main():

    credential = ClientSecretCredential(
        tenant_id="e21b7e4f-4b6c-4ead-bb26-b615da83f381",
        client_id="640027d7-3e96-49fc-8ecf-775f47a1e7b8",
        client_secret="rC.NHz~YPJn_xKGMh-OTg3M4P2kn6EbI90",
        authority="https://login.windows-ppe.net"
    )

    credential_scopes = ["https://farmbeats-dogfood.azure.net/.default"]

    client = FarmBeatsClient(
        base_url="https://agadhibjs-one.farmbeats-dogfood.azure.net",
        credential=credential,
        credential_scopes=credential_scopes,
        logging_enable=True
    )

    farmer = ensure_farmer(client, "agadhika-farmer")

    boundary = ensure_boundary(client, "agadhika-farmer", "agadhika-boundary")

    # job = queue_satellite_job(client, boundary, datetime(2020, 1, 1, tzinfo=utc), datetime(2020, 1, 31, tzinfo=utc), polling=True)
    
    # download_scenes(
    #     client,
    #     datetime(2020, 1, 1, tzinfo=utc),
    #     datetime(2020, 1, 31, tzinfo=utc),
    #     boundary,
    #     "agadhika-foobar")
    
    # weather_job = queue_weather_job(client, boundary, datetime(2020, 1, 1, tzinfo=utc), datetime(2020, 4, 30, tzinfo=utc), polling=True)
    
    # weather_data = download_weather(client, boundary)
    # from utils.weather_util import WeatherUtil
    # w_df = WeatherUtil.get_weather_data_df2(weather_data)
    # w_df.to_csv("weather_data_sample.csv", index=False)


    #from utils.satellite_util import SatelliteUtil
    #boundary = SatelliteUtil(farmbeats_client=client).create_boundary(farmer.id, boundary.id, "add")
    #print(boundary.as_dict())
    start_dt = datetime.strptime(CONSTANTS["interp_date_start"], "%d-%m-%Y")
    end_dt = datetime.strptime("15-07-2019", "%d-%m-%Y")
    #SatelliteUtil(farmbeats_client = client).queue_satellite_job(farmer.id, boundary.id, "annam-job-0", start_dt, end_dt, True)
    df = SatelliteUtil(farmbeats_client = client).download_and_get_sat_file_paths("annam-farmer", "b", start_dt, end_dt, "C:\\farmbeats\\")
    df.to_csv("satellite_paths.csv", index=None)
    
if __name__ == "__main__":
    main()