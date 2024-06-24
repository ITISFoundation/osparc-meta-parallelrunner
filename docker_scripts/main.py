import http.server
import logging
import pathlib as pl
import socketserver
import threading
import time

import pydantic as pyda
import pydantic_settings

import parallelrunner

logging.basicConfig(
    level=logging.INFO, format="[%(filename)s:%(lineno)d] %(message)s"
)
logger = logging.getLogger(__name__)

HTTP_PORT = 8888
INPUT_CONF_KEY = "input_3"


def main():
    """Main"""

    settings = MainSettings()
    config_path = settings.input_path / INPUT_CONF_KEY / "parallelrunner.json"

    waiter = 0
    while not config_path.exists():
        if waiter % 10 == 0:
            logger.info("Waiting for parallelrunner.json to exist ...")
        time.sleep(settings.file_polling_interval)
        waiter += 1

    settings = settings.parse_file(config_path)
    logging.info(f"Received the following settings: {settings}")

    http_dir_path = pl.Path(__file__).parent / "http"

    class HTTPHandler(http.server.SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(
                *args, **kwargs, directory=http_dir_path.resolve()
            )

    maprunner = parallelrunner.ParallelRunner(**settings.dict())

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


class MainSettings(pydantic_settings.BaseSettings):
    batch_mode: bool = False
    file_polling_interval: int = 1
    input_path: pyda.DirectoryPath = pyda.Field(alias="DY_SIDECAR_PATH_INPUTS")
    output_path: pl.Path = pyda.Field(alias="DY_SIDECAR_PATH_OUTPUTS")


if __name__ == "__main__":
    main()
