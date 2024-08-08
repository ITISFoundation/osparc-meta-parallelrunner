import pathlib as pl

from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer


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
