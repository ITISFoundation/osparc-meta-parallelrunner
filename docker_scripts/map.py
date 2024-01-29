import json
import os
import time

import pathlib as pl
import logging

logging.basicConfig(
    level=logging.INFO, format="[%(filename)s:%(lineno)d] %(message)s"
)
logger = logging.getLogger(__name__)

POLLING_INTERVAL = 1  # second

import osparc
import osparc_client


def main():
    """Main"""

    input_path = pl.Path(os.environ["DY_SIDECAR_PATH_INPUTS"])

    pyrunner = MapRunner(input_path)

    try:
        pyrunner.setup()
        pyrunner.start()
        pyrunner.teardown()
    except Exception as err:  # pylint: disable=broad-except
        logger.error(f"{err} . Stopping %s", exc_info=True)


class MapRunner:
    def __init__(self, input_path, polling_time=1):
        """Constructor"""

        self.input_path = input_path  # path where osparc write all our input
        self.template_id = "f448d118-bc54-11ee-ba85-02420a000022"
        self.key_values_path = self.input_path / "key_values.json"

    def setup(self):
        """Setup the Python Runner"""
        self.osparc_cfg = osparc.Configuration(
            host="10.43.103.149.nip.io:8006",
            username="test_T1QyAxKBUX",
            password="0dsHA6zdYDNEtwNKsXZHBQq8eHuPbd",
        )
        self.api_client = osparc.ApiClient(self.osparc_cfg)
        self.studies_api = osparc_client.StudiesApi(self.api_client)

    def start(self):
        """Start the Python Runner"""
        logger.info("Starting map ...")

        while not self.key_values_path.exists():
            logger.info("Waiting for key_values.json to exist ...")
            time.sleep(POLLING_INTERVAL)

        # logger.info(json.loads(self.input_path.read_text()))

        new_job = self.studies_api.create_study_job(
            "02621c5a-bbba-11ee-ba85-02420a000022", job_inputs={"values": {}}
        )

        logger.info(
            self.studies_api.start_study_job(
                study_id=self.template_id, job_id=new_job.id
            )
        )

        print(new_job)

    def teardown(self):
        logger.info("Completing map ...")
        self.api_client.close()

    def read_keyvalues(self):
        """Read keyvalues file"""

        keyvalues_unprocessed = json.loads(self.keyvalues_path.read_text())

        keyvalues = {}
        for key, value in keyvalues_unprocessed.items():
            keyvalues[key] = {}
            keyvalues[key][value["key"]] = value["value"]

        return keyvalues


if __name__ == "__main__":
    main()
