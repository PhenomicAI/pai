import json
import logging
import os
import time
import zipfile
import hashlib
import math
import requests

BACKEND_API_URI = "https://backend.sctx.phenomic.ai"  # DEV http://127.0.0.1:5000
CHUNK_SIZE = 2**20  # 1 Megabyte

# https://docs.hdfgroup.org/hdf5/v1_14/_f_m_t3.html#Superblock
H5AD_SIGNATURE = bytes.fromhex("894844460d0a1a0a")

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%m/%d/%Y %I:%M:%S %p"
)
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
        assert (
            CLIENT_SECRET is not None
        ), "CLIENT_SECRET environment variable not defined"

        url = AUTH0_URL
        headers = {"content-type": "application/json"}
        data = {
            "client_id": f"{CLIENT_ID}",
            "client_secret": f"{CLIENT_SECRET}",
            "audience": "https://sctx.auth.phenomic.ai",
            "grant_type": "client_credentials",
        }

        response = requests.post(url, headers=headers, json=data)

        if response.status_code >= 200 and response.status_code < 300:
            return json.loads(response.content.decode("utf-8"))["access_token"]
        else:
            raise Exception("Credentials failed")  # TODO

    def download_example_h5ad(self):
        logger.info("Downloading example h5ad")
        url = BACKEND_API_URI + "/download_example_h5ad"
        headers = {"Authorization": f"Bearer {self.access_token}"}

        response = requests.get(url, headers=headers)

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

    def get_upload_uuid(self, chunks):
        logger.info("Getting upload id")
        headers = {"Authorization": f"Bearer {self.access_token}"}
        url = BACKEND_API_URI + "/start_upload"
        response = requests.post(url, headers=headers, json={"chunk_count": chunks})

        if response.ok:
            self.upload_uuid = json.loads(response.json())["uuid"]
            logger.info(f"Recieved uuid: {self.upload_uuid}")
        else:
            raise Exception("Upload uuid not recieved", response)

    def upload_chunks(self, chunks, file_path):
        headers = {"Authorization": f"Bearer {self.access_token}"}
        hash_md5 = hashlib.md5()
        with open(file_path, "rb") as file:
            for i in chunks:
                chunk = file.read(CHUNK_SIZE)
                hash_md5.update(chunk)
                response = requests.post(
                    BACKEND_API_URI + "/upload_chunk",
                    headers=headers,
                    data={"chunk_id": i, "uuid": self.upload_uuid},
                    files={"file": chunk},
                )
        return hash_md5.hexdigest()

    def upload_h5ad(self, h5ad_path, tissue_organ):
        logger.info("Uploading h5ad file...")
        url = BACKEND_API_URI + "/upload_h5ad"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        data = {"tissueOrgan": tissue_organ}  # body

        size = os.path.getsize(h5ad_path)
        chunks = math.ceil(size / CHUNK_SIZE)
        self.get_upload_uuid(chunks)

        check_h5ad_signature(h5ad_path)
        hash = self.upload_chunks(range(chunks), h5ad_path)
        job_data = {
            "uuid": self.upload_uuid,
            "hash": hash,
            "tissueOrgan": tissue_organ,
        }
        response = requests.post(
            BACKEND_API_URI + "/upload_status",
            json=job_data,
            headers=headers,
        )

        if response.status_code == 200:
            job_id = json.loads(response.content)["id"]
            logger.info(f"Upload complete, job id: {job_id}")
            return job_id
        elif response.status_code == 201:
            # TODO Handle missing chunks
            pass
        else:
            raise Exception(response.status_code, response.reason)

    def get_job_status(self, job_id):
        url = BACKEND_API_URI + "/job"  # TODO
        headers = {"Authorization": f"Bearer {self.access_token}"}
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
        url = BACKEND_API_URI + "/download"
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {self.access_token}",
        }
        data = {"job_id": job_id}

        response = requests.post(url, headers=headers, json=data)

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


def check_h5ad_signature(file_path):
    with open(file_path, "rb") as file:
        signature = file.read(8)
        if signature != H5AD_SIGNATURE:
            logger.error("H5AD Signature mismatch")
            raise Exception("H5AD file does not match signature")

        # TODO consider option to cleanup zip file
