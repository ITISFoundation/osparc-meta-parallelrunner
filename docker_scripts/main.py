import json
import logging

import pydantic as pyda
import pydantic_settings

import parallelrunner
import tools

logging.basicConfig(
    level=logging.INFO, format="[%(filename)s:%(lineno)d] %(message)s"
)
logger = logging.getLogger(__name__)

HTTP_PORT = 8888
INPUT_CONF_KEY = "input_1"
CONF_SCHEMA_KEY = "conf_json_schema"

DEFAULT_FILE_POLLING_INTERVAL = 1  # second

DEFAULT_NUMBER_OF_WORKERS = 1
DEFAULT_TEMPLATE_ID = ""

DEFAULT_MAX_JOB_CREATE_ATTEMPTS = 5
DEFAULT_JOB_CREATE_ATTEMPTS_DELAY = 5
DEFAULT_MAX_JOB_TRIALS = 5

DEFAULT_JOB_TIMEOUT = 0

MAX_N_OF_WORKERS = 10


def main():
    """Main"""

    settings = ParallelRunnerDynamicSettings()

    # Wait for and read the settings file
    logger.info(
        f"Waiting for settings file to appear at {settings.settings_file_path}"
    )
    settings.read_settings_file()
    logger.info("Settings file was read")

    # Create and start the maprunner
    maprunner = parallelrunner.ParallelRunner(settings)
    maprunner.setup()
    maprunner.start()

    # Stop the maprunner
    maprunner.teardown()


class ParallelRunnerDynamicSettings:
    def __init__(self):
        self._settings = self.ParallelRunnerMainSettings()
        conf_json_schema_path = (
            self._settings.output_path / CONF_SCHEMA_KEY / "schema.json"
        )

        settings_schema = self._settings.model_json_schema()

        # Hide some settings from the user
        for field_name in [
            "max_number_of_workers",
            "JOBS_SETTINGS_PATH",
            "JOBS_STATUS_PATH",
            "DY_SIDECAR_PATH_INPUTS",
            "DY_SIDECAR_PATH_OUTPUTS",
        ]:
            settings_schema["properties"].pop(field_name)

        conf_json_schema_path.write_text(json.dumps(settings_schema, indent=2))

        self.settings_file_path = (
            self._settings.input_path / INPUT_CONF_KEY / "settings.json"
        )

    def __getattr__(self, name):
        if name in self.__dict__:
            return self.__dict__[name]
        else:
            self.read_settings_file()
            return getattr(self._settings, name)

    def read_settings_file(self):
        tools.wait_for_path(self.settings_file_path)
        self._settings = self._settings.parse_file(self.settings_file_path)

    class ParallelRunnerMainSettings(pydantic_settings.BaseSettings):
        template_id: str = pyda.Field(default=DEFAULT_TEMPLATE_ID)
        max_number_of_workers: int = pyda.Field(
            MAX_N_OF_WORKERS, allow_mutation=False
        )
        number_of_workers: int = pyda.Field(
            default=DEFAULT_NUMBER_OF_WORKERS, gt=0, le=MAX_N_OF_WORKERS
        )
        batch_mode: bool = pyda.Field(default=False)
        file_polling_interval: int = pyda.Field(
            default=DEFAULT_FILE_POLLING_INTERVAL
        )
        input_path: pyda.DirectoryPath = pyda.Field(
            alias="DY_SIDECAR_PATH_INPUTS"
        )
        output_path: pyda.DirectoryPath = pyda.Field(
            alias="DY_SIDECAR_PATH_OUTPUTS"
        )
        max_job_trials: int = pyda.Field(default=DEFAULT_MAX_JOB_TRIALS, gt=0)
        file_polling_interval: int = pyda.Field(
            DEFAULT_FILE_POLLING_INTERVAL, gt=0
        )
        max_job_create_attempts: int = pyda.Field(
            default=DEFAULT_MAX_JOB_CREATE_ATTEMPTS, gt=0
        )
        job_create_attempts_delay: int = pyda.Field(
            default=DEFAULT_JOB_CREATE_ATTEMPTS_DELAY, gt=0
        )
        job_timeout: float = pyda.Field(default=DEFAULT_JOB_TIMEOUT, ge=0)
        jobs_settings_path: pyda.FilePath = pyda.Field(
            alias="JOBS_SETTINGS_PATH"
        )
        jobs_status_path: pyda.FilePath = pyda.Field(alias="JOBS_STATUS_PATH")


if __name__ == "__main__":
    main()
