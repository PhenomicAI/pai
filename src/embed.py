import json
import zipfile
import time
import logging
import os

from dotenv import load_dotenv
import requests

load_dotenv()

AUTH0_URL = os.getenv("AUTH0_URL")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
BACKEND_API_URI = os.getenv("BACKEND_API_URI")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p")
logger = logging.getLogger(__name__)


class PaiEmbeddings:
    def __init__(self, results_dir="tmp/results/"):
        self.results_dir = results_dir
        self.access_token = self.get_access_token()

    @staticmethod
    def get_access_token():
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

        if not os.path.exists("tmp/adatas/"):
            os.mkdir("tmp/adatas/")

        with open("tmp/adatas/anndata_example.h5ad", "wb") as binary_file:
            binary_file.write(response.content)

    def inference(self, h5ad_path, tissue_organ):

        assert h5ad_path.endswith(".h5ad")

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

        if not os.path.exists("tmp/zips/"):
            os.mkdir("tmp/zips/")

        if not os.path.exists(self.results_dir):
            os.mkdir(self.results_dir)

        zip_path = f"tmp/zips/{job_id}.zip"  # TODO consider including adata filename as part of results path
        results_dir = os.path.join(self.results_dir, job_id)  # update results_dir wrt. job_id

        with open(zip_path, "wb") as binary_file:
            binary_file.write(response.content)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(results_dir)

        # TODO consider option to cleanup zip file
