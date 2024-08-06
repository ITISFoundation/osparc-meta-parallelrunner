import pathlib as pl

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


def wait_for_path(file_path):
    path = pl.Path(file_path)
    if path.exists():
        return path

    class Handler(FileSystemEventHandler):
        def on_created(self, event):
            if pl.Path(event.src_path) == path:
                self.created = True

    handler = Handler()
    observer = Observer()
    observer.schedule(handler, path.parent, recursive=False)
    observer.start()

    try:
        while not getattr(handler, "created", False):
            observer.join(0.1)
        return path
    finally:
        observer.stop()
        observer.join()
