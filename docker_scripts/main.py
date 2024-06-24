import contextlib
import http.server
import logging
import os
import pathlib as pl
import socketserver
import threading

import osparc_client
import osparc_client.models.file

import parallelrunner

logging.basicConfig(
    level=logging.INFO, format="[%(filename)s:%(lineno)d] %(message)s"
)
logger = logging.getLogger(__name__)

MAX_JOB_CREATE_ATTEMPTS = 5
MAX_TRIALS = 5

HTTP_PORT = 8888

MAX_N_OF_WORKERS = 10

POLLING_INTERVAL = 1  # second
TEMPLATE_ID_KEY = "input_0"
N_OF_WORKERS_KEY = "input_1"
INPUT_PARAMETERS_KEY = "input_2"


def main():
    """Main"""

    input_path = pl.Path(os.environ["DY_SIDECAR_PATH_INPUTS"])
    output_path = pl.Path(os.environ["DY_SIDECAR_PATH_OUTPUTS"])

    http_dir_path = pl.Path(__file__).parent / "http"

    class HTTPHandler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(
                *args, **kwargs, directory=http_dir_path.resolve()
            )

    maprunner = parallelrunner.ParallelRunner(
        input_path, output_path, polling_interval=POLLING_INTERVAL
    )

    try:
        logger.info(
            f"Starting http server at port {HTTP_PORT} and serving path {http_dir_path}"
        )
        with socketserver.TCPServer(("", HTTP_PORT), HTTPHandler) as httpd:
            httpd_thread = threading.Thread(target=httpd.serve_forever)
            httpd_thread.start()
            maprunner.setup()
            maprunner.start()
            maprunner.teardown()
            httpd.shutdown()
    except Exception as err:  # pylint: disable=broad-except
        logger.error(f"{err} . Stopping %s", exc_info=True)


@contextlib.contextmanager
def create_study_job(template_id, job_inputs, studies_api):
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
            if n_of_create_attempts >= MAX_JOB_CREATE_ATTEMPTS:
                raise Exception(
                    f"Tried {n_of_create_attempts} times to create a job from "
                    "the study, but failed"
                )
            else:
                logger.exception(api_exception)
                logger.info(
                    "Received an API Exception from server "
                    "when creating job, retrying..."
                )

    try:
        yield job
    finally:
        studies_api.delete_study_job(template_id, job.id)


if __name__ == "__main__":
    main()
