import json
import logging
import pathlib as pl
import time

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

DEFAULT_FILE_POLLING_INTERVAL = 5

logging.basicConfig(
    level=logging.INFO, format="[%(filename)s:%(lineno)d] %(message)s"
)
logger = logging.getLogger(__name__)


def wait_for_path(file_path):
    path = pl.Path(file_path).resolve()
    if path.exists():
        return str(path)

    # Find the closest existing parent directory
    watch_dir = path
    while not watch_dir.exists():
        watch_dir = watch_dir.parent

    class Handler(FileSystemEventHandler):
        def __init__(self):
            self.created = False

        def on_created(self, event):
            nonlocal path
            created_path = pl.Path(event.src_path).resolve()
            if created_path == path:
                self.created = True
            elif path.is_relative_to(created_path):
                # Update path if a parent directory was created
                path = created_path / path.relative_to(created_path)

    handler = Handler()
    observer = Observer()
    observer.schedule(handler, str(watch_dir), recursive=True)
    observer.start()

    try:
        while not handler.created:
            if path.exists():
                return str(path)
            observer.join(0.1)
        return str(path)
    finally:
        observer.stop()
        observer.join()


def load_json(
    file_path, wait=True, file_polling_interval=DEFAULT_FILE_POLLING_INTERVAL
):
    # if wait:
    #    wait_for_path(file_path)

    while True:
        try:
            content = json.loads(file_path.read_text())
            break
        except json.decoder.JSONDecodeError:
            logger.info(f"JSON read error, retrying read from {file_path}")
            time.sleep(file_polling_interval)

    return content
