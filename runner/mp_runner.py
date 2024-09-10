import multiprocessing
import queue
import asyncio
from typing import Iterable, Callable, Hashable, Awaitable, Any, List

from .base import BaseRunner, AsyncioSleepInterval
from .runtime_task import RunningTaskContext


class MpTaskRunner(BaseRunner):

    def __init__(self,
            pipeDataCb: Callable[[Hashable, bool], Awaitable],
            finishCb: Callable[[Hashable, bool], None],
            processCount: int):
        super().__init__(finishCb, pipeDataCb)
        self.__taskQueue = multiprocessing.JoinableQueue(processCount * 2)
        self.__taskResultQueue = multiprocessing.Queue(processCount * 2)
        self.__processes: List[multiprocessing.Process] = [
            multiprocessing.Process(
                target=MpTaskRunner.__worker,
                args=(self.__taskQueue, self.__taskResultQueue)) \
            for i in range(processCount)
        ]
        self.__processCount = processCount

    @staticmethod
    def __worker(queue: multiprocessing.JoinableQueue,
                resultQueue: multiprocessing.Queue):
        while True:
            taskPak: RunningTaskContext = queue.get()
            resultQueue.put(taskPak.run())
            queue.task_done()

    async def run(self):
        for process in self.__processes:
            process.start()
        
        processedCount = 0
        while True:
            processedCount += 1
            try:
                result = self.__taskResultQueue.get_nowait()
            except queue.Empty:
                await asyncio.sleep(AsyncioSleepInterval)
                processedCount = 0
            except:
                assert 0
            else:
                if result == 'END':
                    break
                else:
                    pass
                    if processedCount == self.__processCount:
                        await asyncio.sleep(0)
        
        for process in self.__processes:
            process.kill()

    @staticmethod
    async def __putToQueue(queue: multiprocessing.Queue, obj: Any):
        while True:
            try:
                queue.put_nowait(obj)
                break
            except queue.Full:
                await asyncio.sleep(AsyncioSleepInterval)

    async def sendTasks(self, rtcList: Iterable[RunningTaskContext]):
        for rtc in rtcList:
            await MpTaskRunner.__putToQueue(self.__taskQueue, rtc)

    async def finish(self):
        self.__taskQueue.join()
        await MpTaskRunner.__putToQueue(self.__taskResultQueue, 'END')
    
    async def terminate(self):
        await MpTaskRunner.__putToQueue(self.__taskResultQueue, 'END')
