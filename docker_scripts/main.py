import http.server
import logging
import pathlib as pl
import socketserver
import threading
import time
import typing

import pydantic as pyda
import pydantic_settings

import parallelrunner

logging.basicConfig(
    level=logging.INFO, format="[%(filename)s:%(lineno)d] %(message)s"
)
logger = logging.getLogger(__name__)

HTTP_PORT = 8888
INPUT_CONF_KEY = "input_3"

FILE_POLLING_INTERVAL = 1  # second

MAX_JOB_CREATE_ATTEMPTS = 5
JOB_CREATE_ATTEMPTS_DELAY = 5
MAX_JOB_TRIALS = 5


def main():
    """Main"""

    settings = MainSettings()
    config_path = settings.input_path / INPUT_CONF_KEY / "parallelrunner.json"

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

            # Now start the real parallel runner
            waiter = 0
            while not config_path.exists():
                if waiter % 10 == 0:
                    logger.info("Waiting for parallelrunner.json to exist ...")
                time.sleep(settings.file_polling_interval)
                waiter += 1

            settings = settings.parse_file(config_path)
            logging.info(f"Received the following settings: {settings}")

            maprunner = parallelrunner.ParallelRunner(**settings.dict())

            maprunner.setup()
            maprunner.start()
            maprunner.teardown()

            httpd.shutdown()
    except Exception as err:  # pylint: disable=broad-except
        logger.error(f"{err} . Stopping %s", exc_info=True)


class MainSettings(pydantic_settings.BaseSettings):
    batch_mode: bool = False
    file_polling_interval: int = FILE_POLLING_INTERVAL
    input_path: pyda.DirectoryPath = pyda.Field(alias="DY_SIDECAR_PATH_INPUTS")
    output_path: pyda.DirectoryPath = pyda.Field(
        alias="DY_SIDECAR_PATH_OUTPUTS"
    )
    max_job_trials: int = MAX_JOB_TRIALS
    file_polling_interval: int = FILE_POLLING_INTERVAL
    max_job_create_attempts: int = MAX_JOB_CREATE_ATTEMPTS
    job_create_attempts_delay: int = JOB_CREATE_ATTEMPTS_DELAY


if __name__ == "__main__":
    main()
