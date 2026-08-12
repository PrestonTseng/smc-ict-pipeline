from __future__ import annotations

import fcntl
from pathlib import Path


class LockUnavailable(RuntimeError):
    pass


class ProcessLock:
    def __init__(self, path: Path):
        self.path = path
        self.handle = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+")
        try:
            fcntl.flock(self.handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            self.handle.close()
            raise LockUnavailable(str(self.path)) from None
        return self

    def __exit__(self, *args):
        fcntl.flock(self.handle, fcntl.LOCK_UN)
        self.handle.close()
