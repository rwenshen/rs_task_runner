from dataclasses import dataclass
from typing import Iterable, Callable, Hashable, Awaitable

from .runtime_task import RunningTaskContext, TaskResult

AsyncioSleepInterval: int = 1

class BaseRunner:

    def __init__(self,
                pipeDataCb: Callable[[Hashable, bool], Awaitable],
                finishCb: Callable[[Hashable, bool], None],):
        self.pipeDataCb = pipeDataCb
        self.finishCb = finishCb

    async def run(self):
        pass

    async def sendTasks(self, rtcList: Iterable[RunningTaskContext]):
        pass

    async def finish(self):
        pass

    async def terminate(self):
        pass

    async def handleResult(self,
            rtc: RunningTaskContext, result: TaskResult, isSubtask: bool):
        rtc.handleResult(result)
        if isSubtask:
            pass # TODO map
        else:
            self.finishCb(rtc.id, result.success)
