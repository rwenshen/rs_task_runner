__all__ = [
    'RunExeTask',
    'RunExeTaskMp',
]

from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Tuple, Mapping, Iterable
import subprocess
import sys

from ..task import task


@dataclass
class RunExeTask(task.SingleTask):
    fn: Callable[..., Tuple[bool, ...]] = field(init=False)
    kwargs: Mapping = field(init=False)
    needMp: bool = True
    exePath: Path | str | None = None
    errorCheckFn: Callable[[int], Tuple[bool]] | None = None

    def __post_init__(self):
        self.fn = self.runExe
        super().__post_init__()

    def runExe(self, *args):
        try:
            code = subprocess.call([sys.executable, *args])
            if self.errorCheckFn is not None:
                return self.errorCheckFn(code)
            else:
                return code == 0
        except:
            return False


@dataclass
class RunExeTaskMp(task.MpTask):
    fn: Callable[..., Tuple[bool, ...]] = field(init=False)
    kwargs: Mapping = field(init=False)
    kwargsForMp: Iterable[Mapping] = field(init=False)
    exePath: Path | str | None = None

    def __post_init__(self):
        self.fn = self.runExe
        super().__post_init__()

    def runExe(self, *args):
        try:
            code = subprocess.call([sys.executable, *args])
            if self.errorCheckFn is not None:
                return self.errorCheckFn(code)
            else:
                return code == 0
        except:
            return False

