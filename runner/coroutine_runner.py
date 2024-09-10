import asyncio
from typing import Iterable, Callable, Hashable, Awaitable, List

from .base import BaseRunner
from .runtime_task import RunningTaskContext


class CoroutineTaskRunner(BaseRunner):

    def __init__(self,
            pipeDataCb: Callable[[Hashable, bool], Awaitable],
            finishCb: Callable[[Hashable, bool], None],
            workerCount: int):
        super().__init__(finishCb, pipeDataCb)
        self.__taskQueue = asyncio.Queue(workerCount * 2)
        self.__taskResultQueue = asyncio.Queue(workerCount * 2)
        self.__workers: List[asyncio.Task] = [
            asyncio.create_task(CoroutineTaskRunner.__worker(
                self.__taskQueue, self.__taskResultQueue)) \
            for i in range(workerCount)
        ]
        self.__workerCount = workerCount

    @staticmethod
    async def __worker(queue: asyncio.Queue,
                resultQueue: asyncio.Queue):
        while True:
            taskPak: RunningTaskContext = await queue.get()
            await resultQueue.put(await taskPak.runAsync())
            queue.task_done()

    async def run(self):
        
        while True:
            result = await self.__taskResultQueue.get()
            if result == 'END':
                break
            pass

        for worker in self.__workers:
            worker.cancel()

    async def sendTasks(self, rtcList: Iterable[RunningTaskContext]):
        for rtc in rtcList:
            await self.__taskQueue.put(rtc)

    async def finish(self):
        await self.__taskQueue.join()
        await self.__taskResultQueue.put('END')

    async def terminate(self):
        await self.__taskResultQueue.put('END')
