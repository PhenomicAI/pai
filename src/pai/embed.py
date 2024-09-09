import json
import logging
import os
import time
import zipfile

import requests

BACKEND_API_URI = "https://backend-api.scref.phenomic.ai"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p")
logger = logging.getLogger(__name__)


class PaiEmbeddings:
    def __init__(self, tmp_dir):
        self.tmp_dir = tmp_dir
    
    def download_example_h5ad(self):
        logger.info("Downloading example h5ad")
        url = os.path.join(BACKEND_API_URI, "download_example_h5ad")

        response = requests.get(url)

        adatas_dir = os.path.join(self.tmp_dir, "adatas")
        if not os.path.exists(adatas_dir):
            os.mkdir(adatas_dir)

        file_path = os.path.join(adatas_dir, "anndata_example.h5ad")
        with open(file_path, "wb") as binary_file:
            binary_file.write(response.content)

    def inference(self, h5ad_path, tissue_organ):

        assert h5ad_path.endswith(".h5ad")
        assert os.path.exists(h5ad_path)

        job_id = self.upload_h5ad(h5ad_path, tissue_organ)
        self.listen_job_status(job_id)
        self.download_job(job_id)

    def upload_h5ad(self, h5ad_path, tissue_organ):
        logger.info("Uploading h5ad file...")
        url = os.path.join(BACKEND_API_URI, "upload_h5ad")
        data = { "tissueOrgan": tissue_organ }  # body

        with open(h5ad_path, "rb") as file:
            file_name = h5ad_path.split("/")[-1]
            files = { "file": (file_name, file, "multipart/form-data") }
            
            response = requests.post(url, data=data, files=files)

        if response.status_code >= 200 and response.status_code < 300:
            job_id = json.loads(response.content)["id"]
            logger.info(f"Upload complete, job id: {job_id}")
            return job_id
        else:
            raise Exception(response.status_code, response.reason)

    def get_job_status(self, job_id):
        url = os.path.join(BACKEND_API_URI, "job")  # TODO
        params = {"job_id": job_id}

        response = requests.get(url, params=params)

        if response.status_code >= 200 and response.status_code < 300:
            status = json.loads(response.content)["status"]
            logger.info(f"Job status: {status}")
            return status
        else:
            raise Exception(response.status_code, response.reason)

    def listen_job_status(self, job_id):
        logger.info("Listening for job status")
        while True:
            status = self.get_job_status(job_id)
            if status in ["SUBMITTED", "VALIDATING", "RUNNING"]:
                time.sleep(5)  # sleep 5s
                continue
            elif status in ["COMPLETED", "FAILED", "ERROR"]:
                break

    def download_job(self, job_id):
        logger.info("Downloading job")
        url = os.path.join(BACKEND_API_URI, "download")
        data = {"job_id": job_id}

        response = requests.post(url, json=data)

        zips_dir = os.path.join(self.tmp_dir, "zips")
        if not os.path.exists(zips_dir):
            os.mkdir(zips_dir)

        results_dir = os.path.join(self.tmp_dir, "results")
        if not os.path.exists(results_dir):
            os.mkdir(results_dir)

        zip_path = os.path.join(zips_dir, f"{job_id}.zip")
        job_dir = os.path.join(results_dir, job_id)

        with open(zip_path, "wb") as binary_file:
            binary_file.write(response.content)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(job_dir)

        # TODO consider option to cleanup zip file
