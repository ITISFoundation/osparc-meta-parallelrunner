import json
import os
import time
import uuid

import tempfile
import zipfile

import pathlib as pl
import logging

logging.basicConfig(
    level=logging.DEBUG, format="[%(filename)s:%(lineno)d] %(message)s"
)
logger = logging.getLogger(__name__)

import pathos

POLLING_INTERVAL = 1  # second
TEMPLATE_ID_KEY = "input_0"
N_OF_WORKERS_KEY = "input_1"
INPUT_PARAMETERS_KEY = "input_2"

import osparc
import osparc_client
import osparc_client.models.file
from osparc_filecomms import handshakers


def main():
    """Main"""

    input_path = pl.Path(os.environ["DY_SIDECAR_PATH_INPUTS"])
    output_path = pl.Path(os.environ["DY_SIDECAR_PATH_OUTPUTS"])

    pyrunner = MapRunner(
        input_path, output_path, polling_interval=POLLING_INTERVAL
    )

    try:
        pyrunner.setup()
        pyrunner.start()
        pyrunner.teardown()
    except Exception as err:  # pylint: disable=broad-except
        logger.error(f"{err} . Stopping %s", exc_info=True)


class MapRunner:
    def __init__(self, input_path, output_path, polling_interval=1):
        """Constructor"""

        self.input_path = input_path  # path where osparc write all our input
        self.output_path = output_path  # path where osparc write all our input
        self.key_values_path = self.input_path / "key_values.json"

        self.input_tasks_dir_path = self.input_path / "input_2"
        self.input_tasks_path = self.input_tasks_dir_path / "input_tasks.json"

        self.output_tasks_dir_path = self.output_path / "output_1"
        self.output_tasks_path = (
            self.output_tasks_dir_path / "output_tasks.json"
        )

        if self.output_tasks_path.exists():
            self.output_tasks_path.unlink()

        self.polling_interval = polling_interval
        self.caller_uuid = None
        self.uuid = str(uuid.uuid4())

        self.handshaker = handshakers.FileHandshaker(
            self.uuid,
            self.input_tasks_dir_path,
            self.output_tasks_dir_path,
            is_initiator=True,
            verbose_level=logging.DEBUG,
            polling_interval=0.1,
            print_polling_interval=100,
        )

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

        waiter = 0
        while not self.key_values_path.exists():
            if waiter % 10 == 0:
                logger.info("Waiting for key_values.json to exist ...")
            time.sleep(self.polling_interval)
            waiter += 1

        key_values = json.loads(self.key_values_path.read_text())

        self.caller_uuid = self.handshaker.shake()
        logger.info(f"Performed handshake with caller: {self.caller_uuid}")

        waiter = 0
        while (
            INPUT_PARAMETERS_KEY not in key_values
            or key_values[INPUT_PARAMETERS_KEY]["value"] is None
        ):
            if waiter % 10 == 0:
                logger.info(
                    f"Waiting for {INPUT_PARAMETERS_KEY} to exist in key_values..."
                )
            key_values = json.loads(self.key_values_path.read_text())
            time.sleep(self.polling_interval)
            waiter += 1

        self.template_id = key_values[TEMPLATE_ID_KEY]["value"]
        if self.template_id is None:
            raise ValueError("Template ID can't be None")

        n_of_workers = key_values[N_OF_WORKERS_KEY]["value"]
        if n_of_workers is None:
            raise ValueError("Number of workers can't be None")

        waiter = 0
        while not self.input_tasks_path.exists():
            if waiter % 10 == 0:
                logger.info(
                    f"Waiting for input file at {self.input_tasks_path}..."
                )
            time.sleep(self.polling_interval)
            waiter += 1

        last_tasks_uuid = ""
        waiter = 0
        while True:
            input_dict = json.loads(self.input_tasks_path.read_text())
            command = input_dict["command"]
            caller_uuid = input_dict["caller_uuid"]
            map_uuid = input_dict["map_uuid"]
            if caller_uuid != self.caller_uuid or map_uuid != self.uuid:
                if waiter % 10 == 0:
                    logger.info(
                        f"Received command with wrong caller uuid: {caller_uuid} or map uuid: {map_uuid}"
                    )
                time.sleep(self.polling_interval)
                waiter += 1
                continue

            if command == "stop":
                break
            elif command == "run":
                tasks_uuid = input_dict["uuid"]

                if tasks_uuid == last_tasks_uuid:
                    if waiter % 10 == 0:
                        logger.info("Waiting for new tasks uuid")
                    time.sleep(self.polling_interval)
                    waiter += 1
                else:
                    input_tasks = input_dict["tasks"]
                    output_tasks = self.run_tasks(
                        tasks_uuid, input_tasks, n_of_workers
                    )
                    output_tasks_content = json.dumps(
                        {"uuid": tasks_uuid, "tasks": output_tasks}
                    )
                    self.output_tasks_path.write_text(output_tasks_content)
                    logger.info(
                        f"Finished a set of tasks: {output_tasks_content}"
                    )
                    last_tasks_uuid = tasks_uuid
                    waiter = 0
            else:
                raise ValueError("Command unknown: {command}")

            time.sleep(self.polling_interval)

    def run_tasks(self, tasks_uuid, input_tasks, n_of_workers):
        logger.info(f"Evaluating: {input_tasks}")

        def map_func(task):
            logger.info(f"Running worker for task: {task}")

            input = task["input"]
            output = task["output"]

            job_inputs = {"values": {}}

            for param_name, param_input in input.items():
                param_type = param_input["type"]
                param_value = param_input["value"]
                if param_type == "FileJSON":
                    param_filename = param_input["filename"]
                    tmp_dir = tempfile.TemporaryDirectory()
                    tmp_dir_path = pl.Path(tmp_dir.name)
                    tmp_input_file_path = tmp_dir_path / param_filename
                    tmp_input_file_path.write_text(json.dumps(param_value))

                    input_data_file = osparc.FilesApi(
                        self.api_client
                    ).upload_file(file=tmp_input_file_path)
                    job_inputs["values"][param_name] = input_data_file
                elif param_type == "file":
                    file_info = json.loads(param_value)
                    input_data_file = osparc_client.models.file.File(
                        id=file_info["id"],
                        filename=file_info["filename"],
                        content_type=file_info["content_type"],
                        checksum=file_info["checksum"],
                        e_tag=file_info["e_tag"],
                    )
                    job_inputs["values"][param_name] = input_data_file
                elif param_type == "integer":
                    job_inputs["values"][param_name] = int(param_value)
                elif param_type == "float":
                    job_inputs["values"][param_name] = float(param_value)
                else:
                    job_inputs["values"][param_name] = param_value

            logger.info(f"Sending inputs: {job_inputs}")

            job = self.studies_api.create_study_job(
                study_id=self.template_id,
                job_inputs=job_inputs,
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

            task["status"] = job_status.state

            if job_status.state == "FAILED":
                logger.error(f"Task failed: {task}")
            else:
                results = self.studies_api.get_study_job_outputs(
                    study_id=self.template_id, job_id=job.id
                ).results

                for probe_name, probe_output in results.items():
                    if probe_name not in output:
                        raise ValueError(
                            f"Unknown probe in output: {probe_name}"
                        )
                    probe_type = output[probe_name]["type"]

                    if probe_type == "FileJSON":
                        output_file = pl.Path(
                            osparc.FilesApi(self.api_client).download_file(
                                probe_output.id
                            )
                        )
                        with zipfile.ZipFile(output_file, "r") as zip_file:
                            file_results_path = zipfile.Path(
                                zip_file, at=output[probe_name]["filename"]
                            )
                            file_results = json.loads(
                                file_results_path.read_text()
                            )

                        output[probe_name]["value"] = file_results
                    elif probe_type == "file":
                        output[probe_name]["value"] = json.dumps(
                            probe_output.to_dict()
                        )
                    elif probe_type == "integer":
                        output[probe_name]["value"] = int(probe_output)
                    elif probe_type == "float":
                        output[probe_name]["value"] = float(probe_output)
                    else:
                        output[probe_name]["value"] = probe_output

                logger.info(f"Worker has finished task: {task}")

            self.studies_api.delete_study_job(
                study_id=self.template_id, job_id=job.id
            )

            return task

        logger.info(f"Starting tasks on {n_of_workers} workers")
        with pathos.pools.ThreadPool(nodes=n_of_workers) as pool:
            output_tasks = list(pool.map(map_func, input_tasks))
            pool.close()
            pool.join()
            pool.clear()  # Pool is singleton, need to clear old pool

        return output_tasks

    def teardown(self):
        logger.info("Closing map ...")
        self.api_client.close()

    def read_keyvalues(self):
        """Read keyvalues file"""

        keyvalues_unprocessed = json.loads(self.keyvalues_path.read_text())
        self.keyvalues_path.unlink()

        keyvalues = {}
        for key, value in keyvalues_unprocessed.items():
            keyvalues[key] = {}
            keyvalues[key][value["key"]] = value["value"]

        return keyvalues


if __name__ == "__main__":
    main()
