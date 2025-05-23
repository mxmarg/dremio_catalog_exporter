import logging
import json
import dremio_api
import dremio_collect_catalog
import sys
import urllib3
urllib3.disable_warnings()

# Configure logging
logging.basicConfig(stream=sys.stdout,
                    format="%(levelname)s\t%(asctime)s - %(message)s",
                    level=logging.INFO)
logger = logging.getLogger(__name__)


if __name__ == '__main__':

    DREMIO_ENDPOINT = "<SET_VALUE>"
    # For Dremio Software:
    # DREMIO_ENDPOINT = "https://<DREMIO_HOST>[:<DREMIO_PORT>]/api/v3"
    # For Dremio Cloud:
    # DREMIO_ENDPOINT = "https://api.[eu.]dremio.cloud/v0/projects/<PROJECT_ID>"

    DREMIO_PAT = "<SET_VALUE>"

    # To limit scope, set the space names and source folder paths that you want to export:
    # source_selector = [["my-s3-bucket", "folder1"], ["my-glue-catalog", "db1"]]
    # space_selector = {"DremioSpace1", "UseCase2"}
    space_selector=set()
    source_selector=[[]]

    api = dremio_api.DremioAPI(DREMIO_PAT, DREMIO_ENDPOINT, timeout=60)

    catalog_entries = dremio_collect_catalog.get_catalog_entries(api, space_selector, source_selector)
    json_filename = 'dremio_catalog_entries.json'
    with open(json_filename, 'w') as f:
        json.dump(catalog_entries, f)
        logger.info(f"Created {json_filename} with {len(catalog_entries)} entries")
