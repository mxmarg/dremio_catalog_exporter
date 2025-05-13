import logging
import requests

logger = logging.getLogger(__name__)

class DremioAPI:

    def __init__(self, dremio_pat: str, dremio_url: str, timeout=10, verify=False):

        dremio_url = dremio_url.rstrip("/")
        if "dremio.cloud/v0/projects" in dremio_url:
            logger.info(f"Dremio Cloud endpoint detected as {dremio_url}")
        else:
            logger.info("Dremio Software endpoint detected")
            if not dremio_url.endswith("/api/v3"):
                dremio_url = dremio_url + "/api/v3"
        logger.info(f"Configured Dremio REST API Endpoint as {dremio_url}")
        self.dremio_url = dremio_url
        self.timeout = timeout
        self.verify = verify
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + dremio_pat
        }
        # Validate token
        response = requests.request("GET", self.dremio_url + '/catalog', headers=self.headers, timeout=self.timeout, verify=self.verify)
        if response.status_code != 200:
            raise Exception(f"Unable to log into {self.dremio_url}. Please validate endpoint and PAT.")

    def get_dataset_id(self, dataset: str):
        dataset_path = dataset.replace(".","/").replace('"','')
        url = self.dremio_url + '/catalog/by-path/'  + dataset_path

        logger.info(f"Getting ID of {dataset}")
        response = requests.request("GET", url, headers=self.headers, timeout=self.timeout, verify=self.verify)
        data = response.json()
        try:
            dataset_id = data["id"]
        except KeyError:
            logger.warning(data)
            logger.warning(f"Dataset ID for {dataset_path} not found")
            dataset_id = ""
        return dataset_id

    def get_catalog(self, catalog_id=""):
        response = requests.get(
            self.dremio_url + f'/catalog/{catalog_id}',
            headers=self.headers, timeout=self.timeout, verify=self.verify
            )
        data = response.json()
        logger.debug(f"GET /catalog/{catalog_id}")
        return data

    def get_query_info(self, job_id: str):
        logger.debug("Waiting for job completion...")
        while True:
            response = requests.get(
                self.dremio_url + '/job/' + job_id,
                headers=self.headers,
                timeout=self.timeout,
                verify=self.verify
            )
            data = response.json()
            job_state = data['jobState']
            if job_state == 'COMPLETED':
                logger.debug('Job successful')
                break
            elif job_state in {"CANCELED", "FAILED"}:
                status = job_state + " - " + data.get("errorMessage", "")
                logger.warning(status)
                break
        return job_state
