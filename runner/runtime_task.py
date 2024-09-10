import inspect
from dataclasses import dataclass, field
from typing import Hashable, Iterable, Mapping, AsyncIterable
from typing import Tuple, Set, Dict
import traceback

from ..task.task import Task, TaskContext, ERunType, EPipeType

async def makePicklable(data):
    if inspect.isgenerator(data):
        return tuple(makePicklable(v) for v in data)
    elif inspect.isasyncgen(data):
        return tuple(makePicklable(v) async for v in data)
    elif isinstance(data, Iterable):
        return type(data)(makePicklable(v) for v in data)
    if isinstance(data, Mapping):
        return {k: makePicklable(v) for k, v in data.items()}
    else:
        return data


@dataclass
class TaskResult:
    success: bool
    results: Tuple = field(default_factory=tuple)

@dataclass
class PipeContext:
    pipeType: EPipeType
    context: TaskContext=field(default_factory=TaskContext)

@dataclass
class RunningTaskContext:
    id: Hashable
    task: Task
    context: TaskContext
    unhandledPrecursorsIdSet: Set[Hashable]
    successorsIdSet: Set[Hashable]
    runType: ERunType
    pipeContextDict: Dict[Hashable, PipeContext]

    def makePicklable(self):
        makePicklable(self.context.args)
        makePicklable(self.context.kwargs)

# region map task
    @property
    def isMapTask(self) -> bool:
        if not self.context.isForMap:
            for pipeContext in self.pipeContextDict.values():
                if pipeContext.pipeType == EPipeType.map:
                    return True
            return False
        else:
            return True

    async def iterSubTask(self) -> 'AsyncIterable[RunningTaskContext]':
        assert self.isMapTask
        pass

    async def iterResultSynchronously(self) -> AsyncIterable[TaskResult]:
        assert self.isMapTask
        pass
# endregion map task

# region run task (non-map) on various process / coroutine
    def __handleException(self, e: Exception):
        if isinstance(e, TypeError) and \
                len(traceback.extract_tb(e.__traceback__)) == 2:
            print(self.task.msgIndent,
                f'Wrong arguments for task "{self.id}: {self.task.desc}"!')
            print(self.task.msgIndent, e)
        else:
            print(f'Exception occurred when execute task "{self.id}: {self.task.desc}":\n', e)
            traceback.print_tb(e.__traceback__)

    def __handleReturnData(self, returnData) -> TaskResult:
        if isinstance(returnData, bool):
            result = TaskResult(success=returnData)
        elif not isinstance(returnData, tuple) or\
                len(returnData) == 0 or\
                not isinstance(returnData[0], bool):
            print(self.task.msgIndent,
                f'Wrong format of return data for task "{self.id}: {self.task.desc}"! '\
                'Please return bool, or (bool, customized data).')
            result = TaskResult(success=False)
        else:
            result = TaskResult(success=returnData[0], results=returnData[1:])
        return result

    def run(self) -> TaskResult:
        try:
            self.task.printMsg(self.task.startMsgPrinter)
            returnData = self.task.fn(
                *self.context.args, **self.context.kwargs)
        except Exception as e:
            self.__handleException(e)
            result = TaskResult(success=False)
        else:
            result = self.__handleReturnData(returnData)
        
        return result

    async def runAsync(self) -> TaskResult:
        if self.task.fnCoroutine is None:
            return self.run()
        else:
            try:
                self.task.printMsg(self.task.startMsgPrinter)
                returnData = await self.task.fnCoroutine(
                    *self.context.args, **self.context.kwargs)
            except Exception as e:
                self.__handleException(e)
                result = TaskResult(success=False)
            else:
                result = self.__handleReturnData(returnData)
            
            return result
# endregion run task

# region result handler
    # call in sub runners
    def handleResult(self, result: TaskResult):
        if self.isMapTask:
            pass # TODO map
        else:
            if not result.success:
                self.task.printMsg(self.task.failureMsgPrinter)
            else:
                self.task.printMsg(self.task.successMsgPrinter)
            # TODO pipe

    # call by scheduler
    def pipeData(self, taskId: Hashable, isSubtask: bool, data: Tuple|None):
        pass

# endregion result handler
