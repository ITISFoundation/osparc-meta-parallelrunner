import asyncio
import contextlib
import datetime
import getpass
import json
import logging
import multiprocessing
import os
import pathlib as pl
import tempfile
import time
import traceback
import uuid
import zipfile

import osparc
import osparc_client
import osparc_client.models.file
import pathos
from osparc_filecomms import handshakers

import tools

logging.basicConfig(
    level=logging.INFO, format="[%(filename)s:%(lineno)d] %(message)s"
)
logger = logging.getLogger(__name__)


class ParallelRunner:
    def __init__(self, settings):
        """Constructor"""
        self.settings = settings

        self.test_mode = False

        self.key_values_path = self.settings.input_path / "key_values.json"

        self.input_tasks_dir_path = self.settings.input_path / "input_2"
        self.input_tasks_path = self.input_tasks_dir_path / "input_tasks.json"

        self.output_tasks_dir_path = self.settings.output_path / "output_1"
        self.output_tasks_path = (
            self.output_tasks_dir_path / "output_tasks.json"
        )

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
        self.lock = multiprocessing.Lock()

    def setup(self):
        """Setup the Python Runner"""
        logger.info(f"Using API host: [{os.environ['OSPARC_API_HOST']}]")
        self.osparc_cfg = osparc.Configuration(
            host=os.environ["OSPARC_API_HOST"],
            username=os.environ["OSPARC_API_KEY"],
            password=os.environ["OSPARC_API_SECRET"],
            retry_status_codes={429, 502, 503, 504, 404, 506},
            retry_max_count=5,
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
        logger.info(f"Input path: {self.settings.input_path.resolve()}")

        self.caller_uuid = self.handshaker.shake()
        logger.info(f"Performed handshake with caller: {self.caller_uuid}")

        if self.settings.template_id is None:
            raise ValueError("Template ID can't be None")

        if self.settings.template_id == "TEST_UUID":
            self.test_mode = True
            self.api_client = None
            self.studies_api = None

        if self.settings.number_of_workers is None:
            raise ValueError("Number of workers can't be None")
        elif (
            self.settings.number_of_workers
            > self.settings.max_number_of_workers
        ):
            raise ValueError(
                "Attempt to set number of workers to "
                f"{self.settings.number_of_workers} which is more than "
                f"is allowed ({self.settings.max_number_of_workers}), "
                "limit value to maximum amount"
            )

        last_tasks_uuid = ""
        waiter_wrong_uuid = 0
        while True:
            tools.wait_for_path(self.input_tasks_path)
            input_dict = tools.load_json(self.input_tasks_path)
            command = input_dict["command"]
            caller_uuid = input_dict["caller_uuid"]
            map_uuid = input_dict["map_uuid"]
            if caller_uuid != self.caller_uuid or map_uuid != self.uuid:
                if waiter_wrong_uuid % 10 == 0:
                    logger.info(
                        "Received command with wrong caller uuid: "
                        f"{caller_uuid} or map uuid: {map_uuid}"
                    )
                time.sleep(self.settings.file_polling_interval)
                waiter_wrong_uuid += 1
                continue

            if command == "stop":
                break
            elif command == "run":
                tasks_uuid = input_dict["uuid"]

                if tasks_uuid == last_tasks_uuid:
                    if waiter_wrong_uuid % 10 == 0:
                        logger.info("Waiting for new tasks uuid")
                    time.sleep(self.settings.file_polling_interval)
                    waiter_wrong_uuid += 1
                else:
                    if self.output_tasks_path.exists():
                        self.output_tasks_path.unlink()

                    input_tasks = input_dict["tasks"]

                    self.run_input_tasks(input_tasks, tasks_uuid)
                    last_tasks_uuid = tasks_uuid
                    waiter_wrong_uuid = 0
            else:
                raise ValueError("Command unknown: {command}")

            time.sleep(self.settings.file_polling_interval)

    def run_input_tasks(self, input_tasks, tasks_uuid):
        number_of_workers = self.settings.number_of_workers
        self.jobs_settings_file_write(number_of_workers)

        batch_mode = self.settings.batch_mode

        if batch_mode:
            n_of_batches = min(number_of_workers, len(input_tasks))
        else:
            n_of_batches = len(input_tasks)

        input_batches = self.batch_input_tasks(input_tasks, n_of_batches)

        output_tasks = input_tasks.copy()
        for output_task in output_tasks:
            output_task["status"] = "SUBMITTED"
        output_tasks_content = json.dumps(
            {"uuid": tasks_uuid, "tasks": output_tasks}
        )
        self.output_tasks_path.write_text(output_tasks_content)

        output_batches = self.run_batches(
            tasks_uuid, input_batches, number_of_workers
        )

        for output_batch in output_batches:
            output_batch_tasks = output_batch["tasks"]

            for output_task_i, output_task in output_batch_tasks:
                output_tasks[output_task_i] = output_task
                # logging.info(output_task["status"])

            output_tasks_content = json.dumps(
                {"uuid": tasks_uuid, "tasks": output_tasks}
            )
            self.output_tasks_path.write_text(output_tasks_content)
            logger.info(f"Finished a batch of {len(output_batch_tasks)} tasks")
        logger.info(f"Finished a set of {len(output_tasks)} tasks")
        logger.debug(f"Finished a set of tasks: {output_tasks_content}")

    def batch_input_tasks(self, input_tasks, n_of_batches):
        batches = [{"batch_i": None, "tasks": []} for _ in range(n_of_batches)]

        for task_i, input_task in enumerate(input_tasks):
            batch_id = task_i % n_of_batches
            batches[batch_id]["batch_i"] = batch_id
            batches[batch_id]["tasks"].append((task_i, input_task))
        return batches

    def create_job_inputs(self, input):
        """Create job inputs"""

        job_inputs = {"values": {}}

        for param_name, param_input in input.items():
            param_type = param_input["type"]
            param_value = param_input["value"]
            if param_type == "FileJSON":
                param_filename = param_input["filename"]
                with tempfile.TemporaryDirectory() as tmp_dir:
                    tmp_dir_path = pl.Path(tmp_dir)
                    tmp_input_file_path = tmp_dir_path / param_filename
                    tmp_input_file_path.write_text(json.dumps(param_value))

                    if self.test_mode:
                        processed_param_value = f"File json: {param_value}"
                    else:
                        logger.info("Calling upload file api for job input")
                        with self.lock:
                            input_data_file = osparc.FilesApi(
                                self.api_client
                            ).upload_file(file=tmp_input_file_path)
                        logger.info("File upload for job input done")
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

    async def run_job(self, task_input, input_batch):
        """Run a job with given inputs"""

        job_inputs = self.create_job_inputs(task_input)

        logger.debug(f"Sending inputs: {job_inputs}")
        if self.test_mode:
            import datetime

            print(f"Start run job: {datetime.datetime.now()}")
            logger.info("Map in test mode, just returning input")

            done_batch = self.process_job_outputs(
                job_inputs, input_batch, "SUCCESS"
            )
            time.sleep(1)
            print(f"Stop run job: {datetime.datetime.now()}")

            return done_batch

        with self.create_study_job(
            self.settings.template_id, job_inputs, self.studies_api
        ) as job:
            logger.info(f"Calling start study api for job {job.id}")
            with self.lock:
                job_status = self.studies_api.start_study_job(
                    study_id=self.settings.template_id, job_id=job.id
                )
            logger.info(f"Start study api for job {job.id} done")

            while job_status.stopped_at is None:
                job_status = self.studies_api.inspect_study_job(
                    study_id=self.settings.template_id, job_id=job.id
                )
                time.sleep(1)

            if job_status.state != "SUCCESS":
                logger.error(
                    f"Batch failed with {job_status.state}: " f"{job_inputs}"
                )
                raise Exception(
                    f"Job returned a failed status: {job_status.state}"
                )
            else:
                with self.lock:
                    job_outputs = self.studies_api.get_study_job_outputs(
                        study_id=self.settings.template_id, job_id=job.id
                    ).results

            done_batch = self.process_job_outputs(
                job_outputs, input_batch, job_status.state
            )

        return done_batch

    def process_job_outputs(self, results, batch, status):
        if self.settings.template_id == "TEST_UUID":
            logger.info("Map in test mode, just returning input")
            for task_i, task in batch["tasks"]:
                task["status"] = "SUCCESS"

            return batch

        for task_i, task in batch["tasks"]:
            output = task["output"]
            task["status"] = status
            for probe_name, probe_output in results.items():
                if probe_name not in output:
                    raise ValueError(f"Unknown probe in output: {probe_name}")
                probe_type = output[probe_name]["type"]

                if probe_type == "FileJSON":
                    with self.lock:
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
                        file_results = tools.load_json(
                            file_results_path, wait=False
                        )

                    output[probe_name]["value"] = file_results
                elif probe_type == "file":
                    with self.lock:
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

                if self.settings.batch_mode and probe_type == "FileJSON":
                    output[probe_name]["value"] = output[probe_name]["value"][
                        task_i
                    ]
                else:
                    if self.settings.batch_mode:
                        raise ParallelRunner.FatalException(
                            "Only FileJSON output allowed in batch mode, "
                            f"received {probe_type} for {probe_name} output"
                        )

        return batch

    def transform_batch_to_task_input(self, batch):
        task_input = {}
        for task_i, task in batch["tasks"]:
            input = task["input"]
            for param_name, param_input in input.items():
                param_type = param_input["type"]

                if param_type == "FileJSON" and self.settings.batch_mode:
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
                            "Can only handle multiple values of FileJSON in "
                            "one batch, not other input types, received "
                            "several values of type "
                            f"{param_type} for {param_name}"
                        )
                    else:
                        task_input[param_name] = param_input

        return task_input

    def jobs_settings_file_write(self, number_of_workers):
        """Write json file with number of workers for GUI"""

        with self.lock:
            jobs_settings = tools.load_json(self.settings.jobs_settings_path)
            jobs_settings = {
                "number_of_workers": number_of_workers,
            }
            self.settings.jobs_settings_path.write_text(
                json.dumps(jobs_settings)
            )

    def jobs_file_write_new(self, id, name, description, status):
        """Add new job to job status file for GUI"""

        with self.lock:
            jobs_statuses = tools.load_json(self.settings.jobs_status_path)
            jobs_statuses[id] = {
                "name": name,
                "description": description,
                "status": status,
            }
            self.settings.jobs_status_path.write_text(
                json.dumps(jobs_statuses)
            )

    def jobs_file_write_status_change(self, id, status):
        """Update job in job status file for GUI"""

        # Javascript current time
        current_time = int(
            datetime.datetime.now(tz=datetime.timezone.utc).timestamp() * 1000
        )

        # Protect concurrent read/write with a lock
        with self.lock:
            jobs_statuses = tools.load_json(self.settings.jobs_status_path)
            jobs_statuses[id]["status"] = status

            if status == "running":
                jobs_statuses[id]["startTime"] = current_time
            elif status == "done":
                jobs_statuses[id]["endTime"] = current_time
            elif status == "failed":
                jobs_statuses[id]["failTime"] = current_time

            self.settings.jobs_status_path.write_text(
                json.dumps(jobs_statuses)
            )

    def run_batches(self, tasks_uuid, input_batches, number_of_workers):
        """Run the tasks"""

        logger.info(f"Evaluating {len(input_batches)} batches")
        logger.debug(f"Evaluating: {input_batches}")

        self.n_of_finished_batches = 0

        def map_func(batch_with_uuid, trial_number=1):
            return asyncio.run(async_map_func(batch_with_uuid, trial_number))

        def set_batch_status(batch, message):
            for task_i, task in batch["tasks"]:
                task["status"] = "FAILURE"

        async def async_map_func(batch_with_uuid, trial_number=1):
            batch_uuid, batch = batch_with_uuid
            try:
                logger.info(
                    "Running worker for a batch of "
                    f"{len(batch["tasks"])} tasks"
                )
                logger.debug(f"Running worker for batch: {batch}")
                self.jobs_file_write_status_change(
                    id=batch_uuid,
                    status="running",
                )

                task_input = self.transform_batch_to_task_input(batch)

                job_timeout = (
                    self.settings.job_timeout
                    if self.settings.job_timeout > 0
                    else None
                )

                output_batch = await asyncio.wait_for(
                    self.run_job(task_input, batch), timeout=job_timeout
                )

                self.jobs_file_write_status_change(
                    id=batch_uuid,
                    status="done",
                )

                self.n_of_finished_batches += 1
                logger.info(
                    "Worker has finished batch "
                    f"{self.n_of_finished_batches} of {len(input_batches)}"
                )

            except ParallelRunner.FatalException as error:
                logger.info(
                    f"Batch {batch} failed with fatal error ({error}) in "
                    f"trial {trial_number}, not retrying"
                )
                self.jobs_file_write_status_change(
                    id=batch_uuid,
                    status="failed",
                )
                set_batch_status(batch, "FAILURE")
                # raise error
            except Exception:
                if trial_number >= self.settings.max_job_trials:
                    logger.info(
                        f"Batch {batch} failed with error ("
                        f"{traceback.format_exc()}) in "
                        f"trial {trial_number}, reach max number of trials of "
                        f"{self.settings.max_job_trials}, not retrying"
                    )
                    self.jobs_file_write_status_change(
                        id=batch_uuid,
                        status="failed",
                    )
                    set_batch_status(batch, "FAILURE")
                    # raise error
                else:
                    logger.info(
                        f"Batch {batch} failed with error ("
                        f"{traceback.format_exc()}) in "
                        f"trial {trial_number}, retrying "
                        f"{self.settings.max_job_trials-trial_number}" 
                        " more times."
                    )
                    output_batch = map_func(
                        batch_with_uuid, trial_number=trial_number + 1
                    )

            return output_batch

        logger.info(
            f"Starting {len(input_batches)} batches on {number_of_workers} workers"
        )

        input_batches_with_uuid = [
            (str(uuid.uuid4()), input_batch) for input_batch in input_batches
        ]

        for batch_uuid, input_batch in input_batches_with_uuid:
            self.jobs_file_write_new(
                id=batch_uuid,
                name=f"Batch {batch_uuid}",
                description=str(input_batch),
                status="todo",
            )

        with pathos.pools.ThreadPool(nodes=number_of_workers) as pool:
            pool.restart()
            output_tasks = pool.uimap(map_func, input_batches_with_uuid)
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

        keyvalues_unprocessed = tools.load_json(
            self.keyvalues_path.read_text()
        )
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
                logger.info(
                    f"Calling create study api for template {template_id}"
                )
                with self.lock:
                    job = studies_api.create_study_job(
                        study_id=template_id,
                        job_inputs=job_inputs,
                    )
                logger.info(
                    f"Create study api for template {template_id} done"
                )
                break
            except osparc_client.exceptions.ApiException as api_exception:
                if (
                    n_of_create_attempts
                    >= self.settings.max_job_create_attempts
                ):
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
                time.sleep(self.settings.job_create_attempts_delay)

        try:
            yield job
        finally:
            with self.lock:
                studies_api.delete_study_job(template_id, job.id)

    class FatalException(Exception):
        pass
