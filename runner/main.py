from dataclasses import dataclass
import multiprocessing
import asyncio
from collections import deque
from typing import Callable, Hashable, Iterable
from typing import Tuple, List, Dict, FrozenSet

from .base import BaseRunner, AsyncioSleepInterval
from .runtime_task import RunningTaskContext, PipeContext
from .mp_runner import MpTaskRunner
from .coroutine_runner import CoroutineTaskRunner
from .synchronous_runner import SynchronousTaskRunner
from ..task.task_graph import TaskGraph, _TaskNode
from ..task.task import ERunType, EPipeType

'''
@dataclass
class _WaitingNode:
    node: _TaskNode
    unhandled: List[Hashable]

    #def isMpPipeNode(self):
    #    return isinstance(self.node.task, MpTask) and self.node.task.mpPipe

    def handle(self, taskId: Hashable,
            result: _TaskNode.HandledData,
            subTaskPool: 'MainProcess.SubTaskPool'):
        assert taskId in self.unhandled, f'Task "{taskId}" is not a precursor'\
                f' of task {self.node.task.id}!'
        
        if self.isMpPipeNode():
            subTask = self.node.pipeMpSubTask(taskId, *result.returnDataMpSub)
            subTaskPool.addPendingPipedSubTasks(subTask)
            if result.finished:
                self.node.finishPipingMpSubTask()
        
        if result.finished:
            if self.node.task.pipe:
                self.node.inputPipe(taskId, *result.returnData)
            self.unhandled.remove(taskId)
            return len(self.unhandled) == 0
        
        return False
'''

class MainProcess:

    def __init__(self, taskGraph: TaskGraph,
                mpProcessCount: int=-1, coroutineWorkerCount: int=-1):

        # runners
        hasAsyncTask = taskGraph.hasMpTask or taskGraph.hasCoroutineTask
            # ensure scheduler working in time
        doesSyncTaskRunOnOtherProcess = taskGraph.hasSynchronousTask and hasAsyncTask
        def getProcessCountFromCpuCount():
            processCount = multiprocessing.cpu_count() -1
            if doesSyncTaskRunOnOtherProcess:
                processCount -= 1
            processCount = max(processCount, 1)
            return processCount
        
        self.__runnerDict: Dict[ERunType, BaseRunner] = {}
        if taskGraph.hasSynchronousTask:
            self.__runnerDict[ERunType.synchronous] = SynchronousTaskRunner(
                    self.pipeData, self.finishTask, doesSyncTaskRunOnOtherProcess)
        if taskGraph.hasMpTask:
            if mpProcessCount == -1:
                mpProcessCount = getProcessCountFromCpuCount()
            self.__runnerDict[ERunType.mp] = MpTaskRunner(
                    self.pipeData, self.finishTask, mpProcessCount)
        if taskGraph.hasCoroutineTask:
            if coroutineWorkerCount == -1:
                coroutineWorkerCount = getProcessCountFromCpuCount()
            self.__runnerDict[ERunType.coroutine] = CoroutineTaskRunner(
                    self.pipeData, self.finishTask, coroutineWorkerCount)

        # runtime handlers
        self.__terminated = False
        self.__rtcDict: Dict[Hashable, RunningTaskContext] = {}

        # init todo dict from root of graph
        self.__todoDict: Dict[ERunType, List[RunningTaskContext]] = {}
        self.__resetTodoDict()
        for node in taskGraph.root.successors:
            taskId = node.taskDef.id
            self.__addRuntimeTask(node)
            rtc = self.__rtcDict[taskId]
            self.__todoDict[rtc.runType].append(rtc)

    def __resetTodoDict(self):
        self.__todoDict = {
            ERunType.synchronous: [],
            ERunType.mp: [],
            ERunType.coroutine: [],
        }

    def __addRuntimeTask(self, node: _TaskNode):
        taskId = node.taskDef.id
        if taskId not in self.__rtcDict:
            pipeContextDict = {}
            for taskId, runType in node.taskDef.pipeTypes.items():
                pipeContextDict[taskId] = PipeContext(runType)
            rtc = RunningTaskContext(
                id=taskId,
                task=node.taskDef.task,
                context=node.taskDef.context,
                unhandledPrecursorsIdSet=node.precursorsIdSet,
                successorsIdSet=node.successorsIdSet,
                runType=node.taskDef.runType,
                pipeContextDict=pipeContextDict,
            )
            self.__rtcDict[taskId] = rtc
            for successor in node.successors:
                self.__addRuntimeTask(successor)

    async def pipeData(self, taskId: Hashable, isSubtask: bool):
        rtc = self.__rtcDict.pop(taskId)


    def finishTask(self, taskId: Hashable, success: bool):
        if success:
            rtc = self.__rtcDict.pop(taskId)
            for nextTaskId in rtc.successorsIdSet:
                nextRtc = self.__rtcDict[nextTaskId]
                nextRtc.unhandledPrecursorsIdSet.remove(taskId)
                if len(nextRtc.unhandledPrecursorsIdSet) == 0:
                    self.__todoDict[nextRtc.runType].append(nextRtc)
        else:
            self.__terminated = True

    async def schedule(self):
        while not self.__terminated:
            # send tasks
            todoDict: Dict[ERunType, List[RunningTaskContext]] = self.__todoDict
            self.__resetTodoDict()
            tasks: List[asyncio.Task] = []
            for runType, runner in self.__runnerDict.items():
                taskNodeList = todoDict[runType]
                if len(taskNodeList) > 0:
                    tasks.append(
                        asyncio.create_task(runner.sendTasks(taskNodeList)))
            if len(tasks) > 0:
                await asyncio.gather(*tasks)
            else:
                await asyncio.sleep(AsyncioSleepInterval)

            # ending check
            if len(self.__rtcDict) == 0:
                for runner in self.__runnerDict.values():
                    await runner.finish()
                break
        return not self.__terminated

    async def run(self) -> bool:
        tasks: List[asyncio.Task] = []
        tasks.append(asyncio.create_task(self.schedule()))
        for runner in self.__runnerDict.values():
            tasks.append(asyncio.create_task(runner.run()))
        results = await asyncio.gather(*tasks)
        return results[0] # from schedule coroutine

'''
class SubTaskPool:
        def __init__(self, conn: PipeConnection):
            self.__conn = conn
            self.__cpuCount =  multiprocessing.cpu_count()
            self.__pendingMpSubTaskNum: int = 0
            self.__mpSendingIterators: List[Iterable] = []
            self.__mpPendingTaskIdSet = set()
            self.__pipedSubTasks = deque()
            self.__currentMpSendingIndex: int = 0
            self.__currentMpSendingCount: int = 0

        def addPendingMpTask(self, node: _TaskNode):
            self.__mpPendingTaskIdSet.add(node.task.id)
            self.__mpSendingIterators.append(node.iterMpSubTask())

        def addPendingPipedSubTasks(self, subTask: SingleTask):
            self.__pipedSubTasks.append(subTask)

        def onHandleResult(self, taskId, finished: bool):
            if taskId in self.__mpPendingTaskIdSet:
                if finished:
                    self.__mpPendingTaskIdSet.remove(taskId)
            self.__pendingMpSubTaskNum -= 1

        def __hasPending(self):
            return len(self.__mpSendingIterators) > 0 or\
                    len(self.__pipedSubTasks) > 0

        def __switchIterator(self):
            if self.__currentMpSendingIndex > len(self.__mpSendingIterators) or\
                    (self.__currentMpSendingIndex ==\
                     len(self.__mpSendingIterators)\
                    and len(self.__pipedSubTasks) == 0):
                self.__currentMpSendingIndex = 0
            self.currentMpSendingCount = 0

        def sendSubTasks(self):
            if not self.__hasPending():
                return
                
            count = max(1, self.__cpuCount * 2 - self.__pendingMpSubTaskNum)
            while self.__hasPending() and count > 0:
                if self.__currentMpSendingIndex ==\
                        len(self.__mpSendingIterators):
                    singleTask = self.__pipedSubTasks.popleft()
                    if len(self.__pipedSubTasks) == 0:
                        self.__switchIterator()
                else:
                    singleTask = next(
                        self.__mpSendingIterators[self.__currentMpSendingIndex],
                        None)
                    if singleTask is None:
                        del self.__mpSendingIterators[self.__currentMpSendingIndex]
                        self.__switchIterator()
                        continue

                self.__conn.send(singleTask)
                self.__pendingMpSubTaskNum += 1
                self.__currentMpSendingCount += 1
                if self.__currentMpSendingCount > self.__cpuCount:
                    self.__currentMpSendingIndex += 1
                    self.__switchIterator()
                count -= 1

'''

'''

    def createScheduler(self):
        self.connMain, self.connSchedule = multiprocessing.Pipe()
        self.schedulerP = multiprocessing.Process(
                        target=schedulerMain, args=(self.connSchedule,))
        self.subTaskPool = TaskRunner.SubTaskPool(self.connMain)
        self.schedulerP.start()

    def __handleResult(self, taskId, result):
        # basic handler defined in task node
        successorsIdSet, handler = self.handleDict[taskId]
        result = handler(result)
        if result.finished:
            del self.handleDict[taskId]
            if not result.result:
                self.__terminate()
        #print(taskId, result.returnData)
        # mp task
        self.subTaskPool.onHandleResult(taskId, result.finished)
        # trigger successors
        for successorId in successorsIdSet:
            waitingNode = self.waitDict[successorId]
            if waitingNode.handle(taskId, result, self.subTaskPool):
                if not waitingNode.isMpPipeNode():
                    self.__sendTask(waitingNode.node)
                del self.waitDict[successorId]

    def __iterResult(self):
        while not self.connMain.closed:
            yield self.connMain.recv()

    def __terminate(self):
        self.connMain.send('END')
        self.terminated = True

    def __sendEnd(self):
        self.connMain.send('END')

    def __iterMpTasks(self):
        for node in self.taskGraph.root.successors:
            todoDict[node.taskDef.runType] = node
        # process order: first mp, then asyncio, final main
'''




# mpProcessCount and coroutineWorkerCount
#   Typical usage:
#       1. no coroutine task
#           leave default settings, the runner will create (CPUcount - 1)
#           processes for multiprocessing
#       2. no multiprocessing task, and coroutine tasks are mainly for sub-processing:
#           leave default setting, and runner will create (CPUcount - 1)
#           coroutine workers for those tasks
#       3. no multiprocessing task, and coroutine tasks are mainly NOT for sub-processing:
#           leave mpProcessCount default, but set coroutineWorkerCount proper
#           value (you can test different value, to get a most optimized one).
#       3. combined by multiprocessing tasks, and coroutine tasks mainly for sub-processing:
#           let mpProcessCount + coroutineWorkerCount equal to (CPUcount - 1).
#       3. combined by multiprocessing tasks, and coroutine tasks mainly NOT for sub-processing:
#           leave mpProcessCount default, but set coroutineWorkerCount proper
#           value (you can test different value, to get a most optimized one).
def runTaskGraph(taskGraph: TaskGraph,
        mpProcessCount: int=-1, coroutineWorkerCount: int=-1) -> bool:
    process = MainProcess(taskGraph, mpProcessCount, coroutineWorkerCount)
    return asyncio.run(process.run())
