__all__ = [
    'RmFileTask',
    'RenameTreeTask',
    'RmTreeTask',
    'QuickRmTreeGraph',
]

from dataclasses import dataclass, field
from pathlib import Path
from typing import Hashable, Callable, Tuple
import traceback
import os, stat
import shutil

from ..task import task

from ..task import task_graph


@dataclass
class RmFileTask(task.SingleTask):
    desc: str = field(init=False)
    fn: Callable[..., Tuple[bool, ...]] = field(init=False)
    path: Path | str | None = None

    def __post_init__(self):
        self.fn = RmFileTask.rmFile
        if self.pipe:
            self.args = ()
            self.desc = f'remove file from pipe'
        else:
            assert self.path is not None
            self.args = (Path(self.path),)
            self.desc = f'remove "{str(self.path)}"'
        super().__post_init__()

    def inputPipe(self, *inputArgs, **inputKwargs):
        super().inputPipe(*inputArgs, **inputKwargs)
        path = self.args[0]
        self.desc = f'remove "{str(path)}"'

    @staticmethod
    def rmFile(src: Path):
        if not src.exists():
            print(f'Source {src} is inexistent, just ignore the removing.')
            return True
        if src.is_dir():
            print(f'Source {src} is not a file!')
            return False
        try:
            os.chmod(src, stat.S_IWRITE)
            Path(src).unlink()
            return True
        except Exception as e:
            print('Exception occurred: ', e)
            traceback.print_tb(e.__traceback__)
            return False


@dataclass
class RenameTreeTask(task.SingleTask):
    desc: str = field(init=False)
    fn: Callable[..., Tuple[bool, ...]] = field(init=False)
    path: Path | str | None = None
    newName: str | None = None

    def __post_init__(self):
        self.fn = RenameTreeTask.mvTree
        if self.pipe:
            self.args = ()
            self.desc = f'rename from pipe'
        else:
            assert self.path is not None
            assert self.newName is not None
            self.args = (Path(self.path), self.newName)
            self.desc = f'rename "{str(self.path)}" to "{self.newName}"'
        super().__post_init__()

    def inputPipe(self, *inputArgs, **inputKwargs):
        super().inputPipe(*inputArgs, **inputKwargs)
        path = self.args[0]
        newName = self.args[1]
        self.desc = f'rename "{str(path)}" to "{newName}"'

    @staticmethod
    def mvTree(src: Path, newName: str):
        dest = src.with_stem(newName)
        if not src.exists():
            print(f'Source {src} is inexistent, just ignore the move.')
            return True, None
        if not src.is_dir():
            print(f'Source {src} is not a directory!')
            return False, None
        if dest.exists():
            print(f'Dest {dest} exists!')
            return False, None
        try:
            return True, src.rename(dest)
        except Exception as e:
            print('Exception occurred: ', e)
            traceback.print_tb(e.__traceback__)
            return False, None


@dataclass
class RmTreeTask(task.SingleTask):
    desc: str = field(init=False)
    fn: Callable[..., Tuple[bool, ...]] = field(init=False)
    path: Path | str | None = None

    def __post_init__(self):
        self.fn = RmTreeTask.rmTree
        if self.pipe:
            self.args = ()
            self.desc = f'remove tree from pipe'
        else:
            assert self.path is not None
            self.args = (Path(self.path),)
            self.desc = f'remove "{str(self.path)}"'
        super().__post_init__()

    def inputPipe(self, *inputArgs, **inputKwargs):
        super().inputPipe(*inputArgs, **inputKwargs)
        path = self.args[0]
        self.desc = f'remove "{str(path)}"'

    @staticmethod
    def rmTree(src: Path):
        if not src.exists():
            print(f'Source {src} is inexistent, just ignore the removing.')
            return True
        if not src.is_dir():
            print(f'Source {src} is not a directory!')
            return False
        try:
            def remove_readonly(func, path, excinfo):
                os.chmod(path, stat.S_IWRITE)
                func(path)
            shutil.rmtree(src, onerror=remove_readonly)
            return True
        except Exception as e:
            print('Exception occurred: ', e)
            traceback.print_tb(e.__traceback__)
            return False


# Rename the directory to a temp name, and then remove it in another process.
#   Depending on startId to perform as quick rm
#   Depending on endId to wait full rm
class QuickRmTreeGraph(task_graph.TaskGraph):
    
    def __init__(self, path: Path | str, tmpPrefix: str='__to_removed_'):
        self.__startId = 'rename ' + str(path)
        self.__endId = 'remove ' + str(path)
        self.__tmpName = tmpPrefix+Path(path).stem
        newPath = Path(path).with_name(self.__tmpName)  

        renameTask = RenameTreeTask(path=path, newName=self.__tmpName)
        rmTask = RmTreeTask(path=newPath, needMp=True)
        super().__init__({
            self.__startId: renameTask,
            self.__endId: (rmTask, self.__startId),
        })

    @property
    def startId(self):
        return self.__startId
    @property
    def endId(self):
        return self.__endId
    @property
    def tmpName(self):
        return self.__tmpName
