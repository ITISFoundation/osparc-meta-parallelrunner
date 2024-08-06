import http.server
import json
import logging
import pathlib as pl
import socketserver
import threading

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

DEFAULT_NUMBER_OF_WORKERS = None
DEFAULT_TEMPLATE_ID = None

DEFAULT_MAX_JOB_CREATE_ATTEMPTS = 5
DEFAULT_JOB_CREATE_ATTEMPTS_DELAY = 5
DEFAULT_MAX_JOB_TRIALS = 5

DEFAULT_JOB_TIMEOUT = None

MAX_N_OF_WORKERS = 10


def main():
    """Main"""

    settings = ParallelRunnerDynamicSettings()

    http_dir_path = pl.Path(__file__).parent / "http"

    class HTTPHandler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(
                *args, **kwargs, directory=http_dir_path.resolve()
            )

    try:
        logger.info(
            f"Starting http server at port {HTTP_PORT} and serving path {http_dir_path}"
        )
        with socketserver.TCPServer(("", HTTP_PORT), HTTPHandler) as httpd:
            # First start the empty web server
            httpd_thread = threading.Thread(target=httpd.serve_forever)
            httpd_thread.start()

            # Wait for and read the settings file
            logger.info(
                f"Waiting for config file to appear at {settings.settings_file_path}"
            )
            settings.read_settings_file()

            # Create and start the maprunner
            maprunner = parallelrunner.ParallelRunner(settings)
            maprunner.setup()
            maprunner.start()

            # Stop the maprunner
            maprunner.teardown()

            # Stop the webserver
            httpd.shutdown()
    except Exception as err:  # pylint: disable=broad-except
        logger.error(f"{err} . Stopping %s", exc_info=True)


class ParallelRunnerDynamicSettings:
    def __init__(self):
        self._settings = self.ParallelRunnerMainSettings()
        settings_schema = self._settings.model_json_schema()
        logger.info(settings_schema)
        conf_json_schema_path = (
            self._settings.output_path / CONF_SCHEMA_KEY / "schema.json"
        )
        conf_json_schema_path.write_text(json.dumps(settings_schema, indent=2))

        self.settings_file_path = (
            self._settings.input_path / INPUT_CONF_KEY / "parallelrunner.json"
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
        template_id: None | str = DEFAULT_TEMPLATE_ID
        max_number_of_workers: int = pyda.Field(
            MAX_N_OF_WORKERS, allow_mutation=False
        )
        number_of_workers: None | int = pyda.Field(
            DEFAULT_NUMBER_OF_WORKERS, gt=0, le=MAX_N_OF_WORKERS
        )
        batch_mode: bool = False
        file_polling_interval: int = DEFAULT_FILE_POLLING_INTERVAL
        input_path: pyda.DirectoryPath = pyda.Field(
            alias="DY_SIDECAR_PATH_INPUTS"
        )
        output_path: pyda.DirectoryPath = pyda.Field(
            alias="DY_SIDECAR_PATH_OUTPUTS"
        )
        max_job_trials: int = DEFAULT_MAX_JOB_TRIALS
        file_polling_interval: int = DEFAULT_FILE_POLLING_INTERVAL
        max_job_create_attempts: int = DEFAULT_MAX_JOB_CREATE_ATTEMPTS
        job_create_attempts_delay: int = DEFAULT_JOB_CREATE_ATTEMPTS_DELAY
        job_timeout: None | float = DEFAULT_JOB_TIMEOUT


if __name__ == "__main__":
    main()
