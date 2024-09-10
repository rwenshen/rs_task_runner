__all__ = [
    'TaskDepDefinition',
    'TaskDefinition',
    'DefinedTaskGraph',
    'DefinedTaskManager',
    'EMgs3Solution',
]

from pathlib import Path
import dataclasses
from typing import Callable, Iterable, Mapping, Hashable
from typing import Tuple, Dict, Any
import shutil
import subprocess
import itertools

from .task import SingleTask, MpTask
from .task_graph import TaskGraph, SubGraphTask
from ..tasks.task_rmtree import QuickRmTreeGraph, RmTreeTask, RmFileTask
from ..main_process import runTaskGraph


@dataclasses.dataclass
class TaskDepDefinition:
    taskType: Hashable
    dependentToTask: bool = True
    dependentToPostTask: bool = False


@dataclasses.dataclass
class MpTaskDefinition:

    desc: str

    # arguments for mp using
    mpArgs:Iterable[Tuple]|None = None
    # arguments for mp using from production of each iterable
    mpArgsToProduct:Iterable[Iterable]|None = None

    fn: Callable[..., bool|Tuple] = None

    # pipe result from precursor task
    need_pipe: bool = False
    # pipe from precursor task as multiprocessing arguments
    need_pipe_for_mp: bool = False
    # pipe multiprocessing result from precursor sub-task of mp task
    need_mp_pipe: bool = False
    
    # function called after core task, used to collect results of mp task
    #   arguments are piped from fn
    post_fn: Callable[[Tuple], bool] = None

    # dep
    deps: Tuple[TaskDepDefinition] = () 

    @property
    def clear_desc(self):
        return 'clear ' + str(self.dest_root)

    @property
    def post_desc(self):
        return 'post ' + self.desc

    @property
    def is_mp_task(self):
        return self.mpArgs is not None\
            or self.mpArgsToProduct is not None\
            or self.need_pipe_for_mp \
            or self.need_mp_pipe


@dataclasses.dataclass
class FileTaskDefinition:

    # relative to root if root is defined, otherwise absolute
    src_root: Path|str|None = None
    # relative to root if root is defined, otherwise absolute
    #   dest_root will be removed when using force mode
    dest_root: Path|str|None = None
    extra_dests: Iterable[Path|str] = ()
    #   ignore_files will be keep when removing dest_root
    ignore_files: Iterable[str] = ()

    #   src -> dest, keep the relative path to src_root

    #   None means no input output file arguments for mp task
    src_suffix: str|None = None
    dest_suffix: str|None = None
    # arguments for mp using, mutually exclusive to src_suffix / dest_suffix
    mpArgs:Iterable[Tuple]|None = None
    # arguments for mp using from production of each iterable
    #   mutually exclusive to src_suffix / dest_suffix and mpArgs
    mpArgsToProduct:Iterable[Iterable]|None = None
    # Final argument used by task
    # For mp task, None args means directly use mpArgs or mpArgsToProduct
    #   the same as ('$arg0', '$arg1', ...)
    # reserved environment
    #   $root: defined in task graph
    #   $src: mp only, path for source data file (absolute path)
    #   $dest: mp only, path for dest data file (absolute path)
    #   $arg0, $arg1, $arg2: mp only, from mpArgs or mpArgsToProduct,
    #       exclusive with $src or $dest
    args: Iterable|None = None

    # tool's executable, mutually exclusive to fn
    tool_exe: str|None = None
    # mgs3 projects info
    tool_project: ToolBuildInfo|None = None
    # core task function, can be multiprocessing, mutually exclusive to tool_exe
    fn: Callable[..., bool|Tuple] = None
    
    # pipe result from precursor task
    need_pipe: bool = False
    # pipe from precursor task as multiprocessing arguments
    need_pipe_for_mp: bool = False
    # pipe multiprocessing result from precursor sub-task of mp task
    need_mp_pipe: bool = False
    
    # function called after core task, used to collect results of mp task
    #   arguments are piped from fn
    post_fn: Callable[[Tuple], bool] = None
    # dep
    deps: Tuple[TaskDepDefinition] = () 

    @property
    def clear_desc(self):
        return 'clear ' + str(self.dest_root)

    @property
    def post_desc(self):
        return 'post ' + self.desc

    @property
    def is_mp_task(self):
        return self.src_suffix is not None\
            or self.src_suffix is not None\
            or self.mpArgs is not None\
            or self.mpArgsToProduct is not None\
            or self.need_pipe_for_mp \
            or self.need_mp_pipe


class DefinedTaskGraph(TaskGraph):

    @staticmethod
    def runExe(self, *args):
        code = subprocess.check_call([str(self.exePath), *args])
        return code == 0

    def __init__(self, taskDef: TaskDefinition,
                force: bool=True,
                commonRoot: Path|str|None=None,
                extraDepDict: Dict[Hashable, 'DefinedTaskGraph']={}):
        self.desc = taskDef.desc
        # paths
        self.commonRoot = None if commonRoot is None else Path(commonRoot)
        self.srcPath = None if taskDef.src_root is None\
                        else Path(taskDef.src_root)
        self.destPath = None if taskDef.dest_root is None\
                        else Path(taskDef.dest_root)
        self.extraDestPaths = []
        if self.commonRoot is not None:
            if self.srcPath is not None:
                self.srcPath = self.commonRoot.joinpath(self.srcPath)
            if self.destPath is not None:
                self.destPath = self.commonRoot.joinpath(self.destPath)
        for extraDest in taskDef.extra_dests:
            if self.commonRoot is not None:
                extraDest = self.commonRoot.joinpath(extraDest)
            self.extraDestPaths.append(extraDest)
        
        taskDict = {}
        depList = []

        # dependencies
        for desc, depGraph in extraDepDict.items():
            depList.append(desc)
            taskDict[depGraph.desc] = SubGraphTask(graph=depGraph)

        # build exe
        if taskDef.tool_project is not None:
            prepareId = self.tryCreatePrepareGraph(
                                        taskDef.tool_project, taskDict)

        # clear task
        clearPendingIds = self.tryCreateClearGraph(taskDef, force, taskDict)

        # task function
        taskFn = taskDef.fn
        if taskDef.tool_exe is not None:
            assert taskDef.fn is None, "tool_exe is mutually exclusive to fn"
            taskFn = DefinedTaskGraph.runExe

        finalArgs = self.prepareArgs(taskDef)
        if not taskDef.is_mp_task:
            runTask = SingleTask(
                taskDef.desc,
                fn=taskFn,
                args=finalArgs,
                needMp=True,
                pipe=taskDef.need_pipe,
            )
        else:
            runTask = MpTask(
                taskDef.desc,
                fn=taskFn,
                argsForMp=finalArgs,
                pipe=taskDef.need_pipe or taskDef.need_pipe_for_mp,
                pipeToMpArgs=taskDef.need_pipe_for_mp,
                mpPipe=taskDef.need_mp_pipe,
            )

        if 'clear' in taskDict:
            depList.extend(clearPendingIds)
        if 'prepare' in taskDict:
            depList.append(prepareId)
        if len(depList) == 0:
            taskDict[taskDef.desc] = runTask
        elif len(depList) == 1:
            taskDict[taskDef.desc] = (runTask, depList[0])
        else:
            taskDict[taskDef.desc] = (runTask, tuple(depList))
        self.taskId = taskDef.desc

        # post task
        self.postTaskId = None
        if taskDef.post_fn is not None:
            self.postTaskId = taskDef.post_desc
            taskDict[self.postTaskId] = (
                SingleTask(
                    self.postTaskId,
                    fn = taskDef.post_fn,
                    pipe=True,
                    needMp=True,
                ),
                taskDef.desc
            )

        super().__init__(taskDict)


    @staticmethod
    def moveIgnoreFiles(srcPath: Path, destName: str,
            ignoreFiles: Iterable[str]):
        destPath = srcPath.with_name(destName)
        for file in ignoreFiles:
            src = srcPath.joinpath(file)
            dest = destPath.joinpath(file)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(src, dest)
        return True

    @staticmethod
    def buildExe(*args):
        return buildProject(*args) == 0

    def tryCreatePrepareGraph(self,
            buildInfo: ToolBuildInfo,
            taskDict: Dict[Hashable, TaskGraph])->str:
        desc = 'build project ' + buildInfo.projName
        taskGraph = TaskGraph({
            desc: SingleTask(
                desc,
                fn=DefinedTaskGraph.buildExe,
                args=(buildInfo.projType, buildInfo.projName,
                        buildInfo.platform, buildInfo.configuration),
                needMp=False),
        })
        taskDict['prepare'] = SubGraphTask(graph=taskGraph)
        return desc

    def tryCreateClearGraph(self, taskDef: TaskDefinition, force: bool,
            taskDict: Dict[Hashable, TaskGraph])->Iterable[Hashable]:
        if force and self.destPath is not None and self.destPath.exists():
            if self.destPath.is_dir():
                clearGraph = QuickRmTreeGraph(path=self.destPath)
                clearPendingId = clearGraph.startId
                # ignore files
                if len(taskDef.ignore_files) > 0:
                    ignoreTmpName = clearGraph.tmpName+'_ignore__'
                    clearGraph = TaskGraph({
                        taskDef.clear_desc+' pre': SingleTask(
                            'move ignored files',
                            fn=DefinedTaskGraph.moveIgnoreFiles,
                            args=(self.destPath, ignoreTmpName,
                                                taskDef.ignore_files),
                            needMp=True,
                        ),
                        1: (SubGraphTask(graph=clearGraph), taskDef.clear_desc+' pre'),
                        taskDef.clear_desc+' post': (
                            SingleTask(
                                'move back ignored files',
                                fn=DefinedTaskGraph.moveIgnoreFiles,
                                args=(self.destPath.with_name(ignoreTmpName),
                                    self.destPath.name, taskDef.ignore_files),
                                needMp=True,
                            ),
                            clearGraph.startId,
                        ),
                        taskDef.clear_desc+' clear': (
                            RmTreeTask(
                                path=self.destPath.with_name(ignoreTmpName),
                                needMp=True,
                            ),
                            taskDef.clear_desc+' post',
                        ),
                    })
            else:
                clearGraph = TaskGraph({
                    taskDef.clear_desc: RmFileTask(
                        path=self.destPath, needMp=True
                    )
                })
                clearPendingId = clearGraph.startId

            result = [clearPendingId]
            for extraDest in self.extraDestPaths:
                if extraDest.is_dir():
                    extraClearGraph = QuickRmTreeGraph(path=extraDest)
                else:
                    desc = 'clear ' + str(extraDest)
                    extraClearGraph = TaskGraph({
                        desc: RmFileTask(path=extraDest, needMp=True)
                    })
                extraClearPendingId = extraClearGraph.startId
                clearGraph = clearGraph.merge(extraClearGraph, {})
                result.append(extraClearPendingId)

            taskDict['clear'] = SubGraphTask(graph=clearGraph)
            return result
        return []

    def prepareArgs(self, taskDef: TaskDefinition
                                        )-> Iterable|Iterable[Iterable]:
        finalArgs = []
        # single args
        if not taskDef.is_mp_task:
            if taskDef.args is not None:
                for arg in taskDef.args:
                    if isinstance(arg, str):
                        arg = arg.replace('$root', str(self.commonRoot))
                    finalArgs.append(arg)
            return finalArgs

        # mp args
        #   pipe
        if taskDef.need_pipe or\
                taskDef.need_pipe_for_mp or\
                taskDef.need_mp_pipe:
            return ()

        #   file args
        if taskDef.src_suffix is not None:
            assert taskDef.args is not None
            assert taskDef.mpArgs is None
            assert taskDef.mpArgsToProduct is None
            assert self.srcPath is not None
            srcList = list(self.srcPath.glob('**\\*'+taskDef.src_suffix))
            if taskDef.dest_suffix is not None:
                assert self.destPath is not None
                destList = [ self.destPath.joinpath(
                                p.relative_to(self.srcPath))\
                                .with_suffix(taskDef.dest_suffix)\
                                    for p in srcList]
            else:
                destList = [None] * len(srcList)

            for arg in taskDef.args:
                if isinstance(arg, str):
                    arg = arg.replace('$root', str(self.commonRoot))
                    if '$src' in arg or '$dest' in arg:
                        finalArgs.append(
                            tuple(arg.replace('$src', str(srcPath))\
                                .replace('dest', str(destPath))\
                            for srcPath, destPath in zip(srcList, destList))
                        )
                    else:
                        finalArgs.append((arg,) * len(srcList))
            return zip(*finalArgs)

        #   mpArgs or mpArgsToProduct
        mpArgs = None
        if taskDef.mpArgs is not None:
            assert taskDef.src_suffix is None
            assert taskDef.dest_suffix is None
            assert taskDef.mpArgsToProduct is None
            mpArgs = taskDef.mpArgs
        if taskDef.mpArgsToProduct is not None:
            assert taskDef.src_suffix is None
            assert taskDef.dest_suffix is None
            assert taskDef.mpArgs is None
            mpArgs = itertools.product(*taskDef.mpArgsToProduct)
        
        if taskDef.args is None:
            return mpArgs
        
        def replaceArgs(arg: str, args: Iterable):
            for index, replaceArg in enumerate(args):
                arg = arg.replace(f'$arg{index}', replaceArg)
        for arg in taskDef.args:
            if isinstance(arg, str):
                arg = arg.replace('$root', str(self.commonRoot))
                if '$arg' in arg:
                    finalArgs.append(
                        (replaceArgs(arg, args) for args in mpArgs)
                    )
                else:
                    finalArgs.append([arg] * len(srcList))
        return zip(*finalArgs)

