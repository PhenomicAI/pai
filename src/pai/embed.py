import json
import logging
import os
import re
import time
import zipfile

import requests

BACKEND_API_URI = "https://backend.sctx.phenomic.ai"

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p")
logger = logging.getLogger(__name__)


class PaiEmbeddings:
    def __init__(self, tmp_dir):
        self.tmp_dir = tmp_dir
        self.access_token = self.get_access_token()

    @staticmethod
    def get_access_token():
        AUTH0_URL = os.getenv("AUTH0_URL")
        CLIENT_ID = os.getenv("CLIENT_ID")
        CLIENT_SECRET = os.getenv("CLIENT_SECRET")

        assert AUTH0_URL is not None, "AUTH0_URL environment variable not defined"
        assert CLIENT_ID is not None, "CLIENT_ID environment variable not defined"
        assert CLIENT_SECRET is not None, "CLIENT_SECRET environment variable not defined"

        url = AUTH0_URL
        headers = { "content-type": "application/json" }
        data = { "client_id": f"{CLIENT_ID}", "client_secret": f"{CLIENT_SECRET}", "audience":"https://sctx.auth.phenomic.ai", "grant_type":"client_credentials" }

        response = requests.post(url, headers=headers, json=data)

        if response.status_code >= 200 and response.status_code < 300:
            return json.loads(response.content.decode("utf-8"))["access_token"]
        else:
            raise Exception("Credentials failed")  # TODO
    
    def download_example_h5ad(self):
        logger.info("Downloading example h5ad")
        url = os.path.join(BACKEND_API_URI, "download_example_h5ad")
        headers = { "Authorization": f"Bearer {self.access_token}" }

        response = requests.get(url, headers=headers)

        adatas_dir = os.path.join(self.tmp_dir, "adatas")  # /home/ubuntu/tmp/pai/embed/adatas
        if not os.path.exists(adatas_dir):
            os.mkdir(adatas_dir)

        file_path = os.path.join(adatas_dir, "anndata_example.h5ad")
        with open(file_path, "wb") as binary_file:
            binary_file.write(response.content)

    def inference(self, h5ad_path, tissue_organ):

        # TODO h5ad_path
        # /home/ubuntu/tmp/pai/embed/adatas/anndata.h5ad
        # tmp/pai/embed/adatas/anndata.h5ad

        assert h5ad_path.endswith(".h5ad")
        assert os.path.exists(h5ad_path)

        job_id = self.upload_h5ad(h5ad_path, tissue_organ)
        self.listen_job_status(job_id)
        self.download_job(job_id)

    def upload_h5ad(self, h5ad_path, tissue_organ):
        logger.info("Uploading h5ad file...")
        # url = "http://127.0.0.1:5000/upload_h5ad"  # TODO
        url = os.path.join(BACKEND_API_URI, "upload_h5ad")  # TODO
        headers = { "Authorization": f"Bearer {self.access_token}" }
        data = { "tissueOrgan": tissue_organ }  # body

        with open(h5ad_path, "rb") as file:
            file_name = h5ad_path.split("/")[-1]
            files = { "file": (file_name, file, "multipart/form-data") }
            response = requests.post(url, headers=headers, data=data, files=files)

        if response.status_code >= 200 and response.status_code < 300:
            job_id = json.loads(response.content)["id"]
            logger.info(f"Upload complete, job id: {job_id}")
            return job_id
        else:
            raise Exception(response.status_code, response.reason)

    def get_job_status(self, job_id):
        url = os.path.join(BACKEND_API_URI, "job")  # TODO
        headers = { "Authorization": f"Bearer {self.access_token}" }
        params = {"job_id": job_id}

        response = requests.get(url, headers=headers, params=params)

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
        headers = { "Content-Type": "application/json", "Authorization": f"Bearer {self.access_token}" }
        data = {"job_id": job_id}

        response = requests.post(url, headers=headers, json=data)

        zips_dir = os.path.join(self.tmp_dir, "zips")  # /home/ubuntu/tmp/pai/embed/zips
        if not os.path.exists(zips_dir):
            os.mkdir(zips_dir)

        results_dir = os.path.join(self.tmp_dir, "results")  # /home/ubuntu/tmp/pai/embed/results
        if not os.path.exists(results_dir):
            os.mkdir(results_dir)

        zip_path = os.path.join(zips_dir, f"{job_id}.zip")
        job_dir = os.path.join(results_dir, job_id)

        with open(zip_path, "wb") as binary_file:
            binary_file.write(response.content)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(job_dir)

        # TODO consider option to cleanup zip file
