from typing import Iterable, Callable, Hashable, Awaitable
import multiprocessing
import asyncio
from multiprocessing.connection import PipeConnection

from .base import BaseRunner, AsyncioSleepInterval
from .runtime_task import RunningTaskContext


class SynchronousTaskRunner(BaseRunner):

    def __init__(self,
            pipeDataCb: Callable[[Hashable, bool], Awaitable],
            finishCb: Callable[[Hashable, bool], None],
            runOnOtherProcess: bool):
        super().__init__(pipeDataCb, finishCb)
        self.__process = None
        if runOnOtherProcess:
            self.__connMain, self.__connMp = multiprocessing.Pipe()
            self.__process = multiprocessing.Process(
                target=SynchronousTaskRunner.__mpMain,
                args=(self.__connMp, )) \

    @staticmethod
    def __mpMain(connMp: PipeConnection):
        while not connMp.closed:
            taskPak: RunningTaskContext = connMp.recv()

    async def run(self):
        if self.__process is not None:
            self.__process.start()

            while not self.__connMain.closed:
                if not self.__connMain.poll():
                    await asyncio.sleep(AsyncioSleepInterval)
                else:
                    result = self.connMain.recv()
                    pass
                    await asyncio.sleep(0)
            
            self.__connMp.close()
            self.__process.kill()

    async def sendTasks(self, rtcList: Iterable[RunningTaskContext]):
        for rtc in rtcList:
            if self.__process is not None:
                pass # TODO mp
            else:
                if rtc.isMapTask:
                    pass # TODO map
                else:
                    # for pure synchronous tasks, just run here
                    result = rtc.run()
                    await self.handleResult(rtc, result, isSubtask=False)
                    if not result.success:
                        break

    async def finish(self):
        if self.__process is not None:
            self.__connMain.close()

    async def terminate(self):
        if self.__process is not None:
            self.__connMain.close()
