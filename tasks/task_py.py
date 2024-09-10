__all__ = [
    'RunPyTask',
    'RunPyTaskMp',
]

from dataclasses import dataclass, field
from pathlib import Path
import sys

from . import task_exe


@dataclass
class RunPyTask(task_exe.RunExeTask):
    pyPath: Path | str | None = None
    exePath: Path | str | None = field(init=False)

    def __post_init__(self):
        self.exePath = sys.executable
        self.args = [str(self.pyPath), *self.args]
        super().__post_init__()


@dataclass
class RunPyTaskMp(task_exe.RunExeTaskMp):
    pyPath: Path | str | None = None
    exePath: Path | str | None = field(init=False)

    def __post_init__(self):
        self.exePath = sys.executable
        self.args = [str(self.pyPath), *self.args]
        super().__post_init__()
