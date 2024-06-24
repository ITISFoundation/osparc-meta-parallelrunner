import http.server
import logging
import pathlib as pl
import socketserver
import threading

import pydantic as pyda
import pydantic_settings

import parallelrunner

logging.basicConfig(
    level=logging.INFO, format="[%(filename)s:%(lineno)d] %(message)s"
)
logger = logging.getLogger(__name__)

HTTP_PORT = 8888


def main():
    """Main"""

    settings = MainSettings()
    settings = settings.parse_file(settings.input_path / "parallelrunner.json")
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
    input_path: pyda.DirectoryPath = pyda.Field(alias="DY_SIDECAR_PATH_INPUTS")
    output_path: pl.Path = pyda.Field(alias="DY_SIDECAR_PATH_OUTPUTS")


if __name__ == "__main__":
    main()
