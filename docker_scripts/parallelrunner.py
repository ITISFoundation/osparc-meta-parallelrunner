import contextlib
import getpass
import json
import logging
import os
import pathlib as pl
import tempfile
import time
import uuid
import zipfile

import osparc
import osparc_client
import osparc_client.models.file
import pathos
from osparc_filecomms import handshakers

logging.basicConfig(
    level=logging.INFO, format="[%(filename)s:%(lineno)d] %(message)s"
)
logger = logging.getLogger(__name__)

FILE_POLLING_INTERVAL = 1  # second

MAX_JOB_CREATE_ATTEMPTS = 5
JOB_CREATE_ATTEMPTS_DELAY = 5
MAX_TRIALS = 5
MAX_N_OF_WORKERS = 10

TEMPLATE_ID_KEY = "input_0"
N_OF_WORKERS_KEY = "input_1"
INPUT_PARAMETERS_KEY = "input_2"


class ParallelRunner:
    def __init__(
        self,
        input_path,
        output_path,
        batch_mode=False,
        file_polling_interval=FILE_POLLING_INTERVAL,
        max_n_of_workers=MAX_N_OF_WORKERS,
        max_trials=MAX_TRIALS,
        max_job_create_attempts=MAX_JOB_CREATE_ATTEMPTS,
    ):
        """Constructor"""
        self.test_mode = False

        self.batch_mode = batch_mode
        self.max_n_of_workers = max_n_of_workers
        self.max_trials = max_trials
        self.max_job_create_attempts = max_job_create_attempts

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

        self.file_polling_interval = file_polling_interval
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
        logger.info(f"Using host: [{os.environ['OSPARC_API_HOST']}]")
        logger.info(f"Using key: [{os.environ['OSPARC_API_KEY']}]")
        logger.info(f"Using secret: [{os.environ['OSPARC_API_SECRET']}]")
        self.osparc_cfg = osparc.Configuration(
            host=os.environ["OSPARC_API_HOST"],
            username=os.environ["OSPARC_API_KEY"],
            password=os.environ["OSPARC_API_SECRET"],
        )
        if self.test_mode:
            self.api_client = None
            self.studies_api = None
        else:
            self.api_client = osparc.ApiClient(self.osparc_cfg)
            self.studies_api = osparc_client.StudiesApi(self.api_client)

    def start(self):
        """Start the Python Runner"""
        logger.info("Starting map ...")

        logger.info(f"User: {getpass.getuser()}, UID: {os.getuid()}")
        logger.info(f"Input path: {self.input_path.resolve()}")

        waiter = 0
        while not self.key_values_path.exists():
            if waiter % 10 == 0:
                logger.info("Waiting for key_values.json to exist ...")
            time.sleep(self.file_polling_interval)
            waiter += 1

        key_values = json.loads(self.key_values_path.read_text())

        self.caller_uuid = self.handshaker.shake()
        logger.info(f"Performed handshake with caller: {self.caller_uuid}")

        waiter = 0
        while (
            INPUT_PARAMETERS_KEY not in key_values
            or key_values[INPUT_PARAMETERS_KEY]["value"] is None
            or TEMPLATE_ID_KEY not in key_values
            or key_values[TEMPLATE_ID_KEY]["value"] is None
            or N_OF_WORKERS_KEY not in key_values
            or key_values[N_OF_WORKERS_KEY]["value"] is None
        ):
            if waiter % 10 == 0:
                logger.info(
                    "Waiting for all required keys to "
                    f"exist in key_values, current content: {key_values}..."
                )
            key_values = json.loads(self.key_values_path.read_text())
            time.sleep(self.file_polling_interval)
            waiter += 1

        self.template_id = key_values[TEMPLATE_ID_KEY]["value"]
        if self.template_id is None:
            raise ValueError("Template ID can't be None")

        if self.template_id == "TEST_UUID":
            self.test_mode = True
            self.api_client = None
            self.studies_api = None

        n_of_workers = key_values[N_OF_WORKERS_KEY]["value"]
        if n_of_workers is None:
            raise ValueError("Number of workers can't be None")
        elif n_of_workers > self.max_n_of_workers:
            logger.warning(
                "Attempt to set number of workers to more than "
                f"is allowed ({self.max_n_of_workers}), limiting value "
                "to maximum amount"
            )
            n_of_workers = self.max_n_of_workers

        last_tasks_uuid = ""
        waiter_wrong_uuid = 0
        while True:
            waiter_input_exists = 0
            while not self.input_tasks_path.exists():
                if waiter_input_exists % 10 == 0:
                    logger.info(
                        f"Waiting for input file at {self.input_tasks_path}..."
                    )
                    self.handshaker.retry_last_write()
                time.sleep(self.file_polling_interval)
                waiter_input_exists += 1

            input_dict = json.loads(self.input_tasks_path.read_text())
            command = input_dict["command"]
            caller_uuid = input_dict["caller_uuid"]
            map_uuid = input_dict["map_uuid"]
            if caller_uuid != self.caller_uuid or map_uuid != self.uuid:
                if waiter_wrong_uuid % 10 == 0:
                    logger.info(
                        "Received command with wrong caller uuid: "
                        f"{caller_uuid} or map uuid: {map_uuid}"
                    )
                time.sleep(self.file_polling_interval)
                waiter_wrong_uuid += 1
                continue

            if command == "stop":
                break
            elif command == "run":
                tasks_uuid = input_dict["uuid"]

                if tasks_uuid == last_tasks_uuid:
                    if waiter_wrong_uuid % 10 == 0:
                        logger.info("Waiting for new tasks uuid")
                    time.sleep(self.file_polling_interval)
                    waiter_wrong_uuid += 1
                else:
                    input_tasks = input_dict["tasks"]

                    if self.batch_mode:
                        n_of_batches = n_of_workers
                    else:
                        n_of_batches = len(input_tasks)

                    input_batches = self.batch_input_tasks(
                        input_tasks, n_of_batches
                    )

                    output_batches = self.run_batches(
                        tasks_uuid, input_batches, n_of_workers
                    )

                    output_tasks = self.unbatch_output_tasks(output_batches)

                    output_tasks_content = json.dumps(
                        {"uuid": tasks_uuid, "tasks": output_tasks}
                    )
                    self.output_tasks_path.write_text(output_tasks_content)
                    logger.info(
                        f"Finished a set of tasks: {output_tasks_content}"
                    )
                    last_tasks_uuid = tasks_uuid
                    waiter_wrong_uuid = 0
            else:
                raise ValueError("Command unknown: {command}")

            time.sleep(self.file_polling_interval)

    def batch_input_tasks(self, input_tasks, n_of_batches):
        batches = [[] for _ in range(n_of_batches)]

        for task_i, input_task in enumerate(input_tasks):
            batch_id = task_i % n_of_batches
            batches[batch_id].append(input_task)
        return batches

    def unbatch_output_tasks(self, batches):
        output_tasks = []
        n_of_tasks = sum(len(batch) for batch in batches)

        for task_i in range(n_of_tasks):
            batch_id = task_i % len(batches)
            output_tasks.append(batches[batch_id].pop(0))
        return output_tasks

    def create_job_inputs(self, input):
        """Create job inputs"""

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

                if self.test_mode:
                    processed_param_value = f"File json: {param_value}"
                else:
                    input_data_file = osparc.FilesApi(
                        self.api_client
                    ).upload_file(file=tmp_input_file_path)
                    processed_param_value = input_data_file
            elif param_type == "file":
                file_info = json.loads(param_value)
                if self.test_mode:
                    processed_param_value = None
                else:
                    input_data_file = osparc_client.models.file.File(
                        id=file_info["id"],
                        filename=file_info["filename"],
                        content_type=file_info["content_type"],
                        checksum=file_info["checksum"],
                        e_tag=file_info["e_tag"],
                    )
                    processed_param_value = input_data_file
            elif param_type == "integer":
                processed_param_value = int(param_value)
            elif param_type == "float":
                processed_param_value = float(param_value)
            else:
                processed_param_value = param_value

            job_inputs["values"][param_name] = processed_param_value

        return job_inputs

    def run_job(self, job_inputs):
        """Run a job with given inputs"""

        logger.debug(f"Sending inputs: {job_inputs}")

        if self.test_mode:
            logger.info("Map in test mode, just returning input")
            self.n_of_finished_batches += 1

            return job_inputs, "SUCCESS"

        with self.create_study_job(
            self.template_id, job_inputs, self.studies_api
        ) as job:
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

            status = job_status.state

            if job_status.state == "FAILED":
                logger.error(f"Batch failed: {job_inputs}")
                raise Exception("Job returned a failed status")
            else:
                job_outputs = self.studies_api.get_study_job_outputs(
                    study_id=self.template_id, job_id=job.id
                ).results

                self.n_of_finished_batches += 1

        return job_outputs, status

    def process_job_outputs(self, results, batch, status):
        if self.template_id == "TEST_UUID":
            logger.info("Map in test mode, just returning input")

            return batch

        for task_i, task in enumerate(batch):
            output = task["output"]
            task["status"] = status
            for probe_name, probe_output in results.items():
                if probe_name not in output:
                    raise ValueError(f"Unknown probe in output: {probe_name}")
                probe_type = output[probe_name]["type"]

                if probe_type == "FileJSON":
                    output_file = pl.Path(
                        osparc.FilesApi(self.api_client).download_file(
                            probe_output.id
                        )
                    )
                    with zipfile.ZipFile(output_file, "r") as zip_file:
                        file_results_path = zipfile.Path(
                            zip_file,
                            at=output[probe_name]["filename"],
                        )
                        file_results = json.loads(
                            file_results_path.read_text()
                        )

                    output[probe_name]["value"] = file_results
                elif probe_type == "file":
                    tmp_output_data_file = osparc.FilesApi(
                        self.api_client
                    ).download_file(probe_output.id)
                    output_data_file = osparc.FilesApi(
                        self.api_client
                    ).upload_file(tmp_output_data_file)

                    output[probe_name]["value"] = json.dumps(
                        output_data_file.to_dict()
                    )
                elif probe_type == "integer":
                    output[probe_name]["value"] = int(probe_output)
                elif probe_type == "float":
                    output[probe_name]["value"] = float(probe_output)
                else:
                    output[probe_name]["value"] = probe_output

                if self.batch_mode and probe_type == "FileJSON":
                    output[probe_name]["value"] = output[probe_name]["value"][
                        task_i
                    ]
                else:
                    if self.batch_mode:
                        raise ParallelRunner.FatalException(
                            "Only FileJSON output allowed in batch mode, "
                            f"received {probe_type} for {probe_name} output"
                        )

        return batch

    def transform_batch_to_task_input(self, batch):
        task_input = {}
        for task in batch:
            input = task["input"]
            for param_name, param_input in input.items():
                param_type = param_input["type"]

                if param_type == "FileJSON" and self.batch_mode:
                    param_filename = param_input["filename"]
                    param_value = param_input["value"]

                    if param_name not in task_input:
                        task_input[param_name] = {}
                        task_input[param_name]["value"] = []
                    task_input[param_name]["value"].append(param_value)

                    task_input[param_name]["type"] = param_type
                    task_input[param_name]["type"] = param_type
                    task_input[param_name]["filename"] = param_filename
                else:
                    if param_name in task_input:
                        raise ParallelRunner.FatalException(
                            "Can only handle multiple value of FileJSON in "
                            "one batch, received several "
                            f"{param_type} for {param_name}"
                        )
                    else:
                        task_input[param_name] = param_input

        return task_input

    def run_batches(self, tasks_uuid, input_batches, n_of_workers):
        """Run the tasks"""

        logger.info(f"Evaluating: {input_batches}")

        self.n_of_finished_batches = 0

        def map_func(batch, trial_number=1):
            try:
                logger.info(f"Running worker for batch: {batch}")

                task_input = self.transform_batch_to_task_input(batch)

                job_inputs = self.create_job_inputs(task_input)

                job_outputs, status = self.run_job(job_inputs)

                batch = self.process_job_outputs(job_outputs, batch, status)

                logger.info(
                    "Worker has finished batch "
                    f"{self.n_of_finished_batches} of {len(input_batches)}"
                )
            except ParallelRunner.FatalException as error:
                logger.info(
                    f"Batch {batch} failed with fatal error ({error}) in "
                    f"trial {trial_number}, not retrying, raising error"
                )
                raise error
            except Exception as error:
                if trial_number >= self.max_trials:
                    logger.info(
                        f"Batch {batch} failed with error ({error}) in "
                        f"trial {trial_number}, not retrying, raising error"
                    )
                    raise error
                else:
                    logger.info(
                        f"Batch {batch} failed with error ({error}) in "
                        f"trial {trial_number}, retrying "
                    )
                    batch = map_func(batch, trial_number=trial_number + 1)

            return batch

        logger.info(
            f"Starting {len(input_batches)} batches on {n_of_workers} workers"
        )
        with pathos.pools.ThreadPool(nodes=n_of_workers) as pool:
            output_tasks = list(pool.map(map_func, input_batches))
            pool.close()
            pool.join()
            pool.clear()  # Pool is singleton, need to clear old pool

        return output_tasks

    def teardown(self):
        logger.info("Closing parallelrunner ...")
        if self.api_client:
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

    @contextlib.contextmanager
    def create_study_job(self, template_id, job_inputs, studies_api):
        n_of_create_attempts = 0
        while True:
            try:
                n_of_create_attempts += 1
                job = studies_api.create_study_job(
                    study_id=template_id,
                    job_inputs=job_inputs,
                )
                break
            except osparc_client.exceptions.ApiException as api_exception:
                if n_of_create_attempts >= self.max_job_create_attempts:
                    raise Exception(
                        f"Tried {n_of_create_attempts} times to create a job from "
                        "the study, but failed"
                    )
                elif api_exception.reason.upper() == "NOT FOUND":
                    raise ParallelRunner.FatalException(
                        f"Study template not found: {template_id}"
                    )
                else:
                    logger.exception(api_exception)
                    logger.info(
                        "Received an unhandled API Exception from server "
                        "when creating job, retrying..."
                    )
                time.sleep(JOB_CREATE_ATTEMPTS_DELAY)

        try:
            yield job
        finally:
            studies_api.delete_study_job(template_id, job.id)

    class FatalException(Exception):
        pass
