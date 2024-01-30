import json
import os
import time

import tempfile
import zipfile

import pathlib as pl
import logging

logging.basicConfig(
    level=logging.INFO, format="[%(filename)s:%(lineno)d] %(message)s"
)
logger = logging.getLogger(__name__)

import pathos

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
        self.key_values_path = self.input_path / "key_values.json"

    def setup(self):
        """Setup the Python Runner"""
        self.osparc_cfg = osparc.Configuration(
            host="10.43.103.149.nip.io:8006",
            username=os.environ["OSPARC_API_KEY"],
            password=os.environ["OSPARC_API_SECRET"],
        )
        self.api_client = osparc.ApiClient(self.osparc_cfg)
        self.studies_api = osparc_client.StudiesApi(self.api_client)

    def start(self):
        """Start the Python Runner"""
        logger.info("Starting map ...")

        import getpass

        logger.info(f"User: {getpass.getuser()}, UID: {os.getuid()}")
        logger.info(f"Input path: {self.input_path.resolve()}")

        while not self.key_values_path.exists():
            logger.info("Waiting for key_values.json to exist ...")
            time.sleep(POLLING_INTERVAL)

        key_values = json.loads(self.key_values_path.read_text())

        logger.info(f"Key/Values: {key_values}")

        self.template_id = key_values[TEMPLATE_ID_KEY]["value"]
        if self.template_id is None:
            raise ValueError("Template ID can't be None")

        n_of_workers = key_values[N_OF_WORKERS_KEY]["value"]
        if n_of_workers is None:
            raise ValueError("Number of workers can't be None")

        input_parameters_dir_path = self.input_path / INPUT_PARAMETERS_KEY

        json_list = list(input_parameters_dir_path.glob("*.json"))
        if len(json_list) != 1:
            raise ValueError(
                f"More than 1 or no json file found in the input parameters path: {json_list}"
            )

        input_parameters_path = json_list[0]

        input_parameters = json.loads(input_parameters_path.read_text())

        logger.info(f"Evaluating: {input_parameters}")

        logger.info(f"Starting {n_of_workers} workers")
        executor = pathos.pools.ThreadPool(nodes=n_of_workers)

        def map_func(input):
            logger.info(f"Running worker for input: {input}")

            output = {"input": input}

            tmp_dir = tempfile.TemporaryDirectory()
            tmp_dir_path = pl.Path(tmp_dir.name)
            tmp_input_file_path = tmp_dir_path / "input.json"
            tmp_input_file_path.write_text(json.dumps(input))

            input_data_file = osparc.FilesApi(self.api_client).upload_file(
                file=tmp_input_file_path
            )

            job = self.studies_api.create_study_job(
                study_id=self.template_id,
                job_inputs={"values": {"InputFile1": input_data_file}},
            )

            job_status = self.studies_api.start_study_job(
                study_id=self.template_id, job_id=job.id
            )

            while (
                job_status.state != "SUCCESS" and job_status.state != "FAILED"
            ):
                job_status = self.studies_api.inspect_study_job(
                    study_id=self.template_id, job_id=job.id
                )
                time.sleep(1)

            output["status"] = job_status.state

            if job_status.state == "FAILED":
                output["results"] = None
            else:
                results = self.studies_api.get_study_job_outputs(
                    study_id=self.template_id, job_id=job.id
                ).results

                output["results"] = {}
                for probe_name, probe_output in results.items():
                    if isinstance(
                        probe_output, osparc_client.models.file.File
                    ):
                        output_file = pl.Path(
                            osparc.FilesApi(self.api_client).download_file(
                                probe_output.id
                            )
                        )
                        with zipfile.ZipFile(output_file, "r") as zip_file:
                            file_results_path = zipfile.Path(
                                zip_file, at="output.json"
                            )
                            file_results = json.loads(
                                file_results_path.read_text()
                            )

                        output["results"][probe_name] = file_results
                    else:
                        output["results"][probe_name] = probe_output

            self.studies_api.delete_study_job(
                study_id=self.template_id, job_id=job.id
            )

            logger.info(f"Worker has finished with output: {output}")

            return output

        map_args = [parameter_set for parameter_set in input_parameters]

        print(list(executor.map(map_func, map_args)))

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
