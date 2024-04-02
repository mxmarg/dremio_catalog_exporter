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

    DREMIO_ENDPOINT = "https://api.dremio.cloud/v0/projects/<PROJECT_ID>"
    DREMIO_PAT = "<SET_VALUE>"

    api = dremio_api.DremioAPI(DREMIO_PAT, DREMIO_ENDPOINT, timeout=60)

    catalog_entries = dremio_collect_catalog.get_catalog_entries(api)
    json_filename = 'dremio_catalog_entries.json'
    with open(json_filename, 'w') as f:
        json.dump(catalog_entries, f)
        logger.info(f"Created {json_filename} with {len(catalog_entries)} entries")
