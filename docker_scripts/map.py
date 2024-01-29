import json
import os
import time
import pathos


import pathlib as pl
import logging

logging.basicConfig(
    level=logging.INFO, format="[%(filename)s:%(lineno)d] %(message)s"
)
logger = logging.getLogger(__name__)

POLLING_INTERVAL = 1  # second
TEMPLATE_ID_KEY = "input_0"
N_OF_WORKERS_KEY = "input_1"
INPUT_PARAMETERS_KEY = "input_2"

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

        import getpass

        logger.info(f"User: {getpass.getuser()}, UID: {os.getuid()}")
        logger.info(f"Input path: {self.input_path.resolve()}")
        logger.info(f"Input content: {list(self.input_path.rglob('*'))}")

        while not self.key_values_path.exists():
            logger.info("Waiting for key_values.json to exist ...")
            time.sleep(POLLING_INTERVAL)

        key_values = json.loads(self.key_values_path.read_text())

        logger.info(f"Key/Values: {key_values}")

        template_id = key_values[TEMPLATE_ID_KEY]["value"]
        if template_id is None:
            raise ValueError("Template ID can't be None")

        n_of_workers = key_values[N_OF_WORKERS_KEY]["value"]
        if n_of_workers is None:
            raise ValueError("Number of workers can't be None")

        input_parameters = key_values[INPUT_PARAMETERS_KEY]["value"]
        if input_parameters is None:
            raise ValueError("Input parameters can't be None")

        executor = pathos.pools.ThreadPool(nodes=n_of_workers)

        job_id = self.studies_api.create_study_job(
            study_id=self.template_id, job_inputs={"values": {}}
        )
        logger.info(job_id)

        def map_func(input):
            logger.info(f"Running worker for: {input}")

            output = {}

            job = self.studies_api.create_study_job(
                study_id=self.template_id, job_inputs={"values": {}}
            )

            job_status = self.studies_api.start_study_job(
                study_id=self.template_id, job_id=job.id
            )

            while (
                job_status.state != "SUCCESS" and job_status.state != "FAILED"
            ):
                job_status = self.studies_api.inspect_study_job(
                    study_id=template_id, job_id=job.id
                )
                # logger.info(f"'{job_status.state}'")
                time.sleep(1)

            output["status"] = job_status.state

            if job_status.state == "FAILED":
                output["results"] = None
            else:
                output["results"] = self.studies_api.get_study_job_outputs(
                    study_id=template_id, job_id=job.id
                ).results

            self.studies_api.delete_study_job(
                study_id=self.template_id, job_id=job.id
            )

            logger.info("Worker has finished")

            return output

        map_args = [parameter_set for parameter_set in input_parameters]

        print(list(executor.map(map_func, map_args)))

        """
        logger.info(
            self.studies_api.start_study_job(
                study_id=self.template_id, job_id=new_job.id
            )
        )
        """

    def teardown(self):
        logger.info("Closing map ...")
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
