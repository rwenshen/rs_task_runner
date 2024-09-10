__all__ = [
    'ERunType',
    'EPipeType',
    'TaskContext',
    'Task',
]

from dataclasses import dataclass, field
from typing import Callable, Awaitable, Mapping, Iterable, AsyncIterable
from typing import Tuple, List
from enum import Enum, auto


class ERunType(Enum):
    # run type will be use that set in task graph
    default=auto()
    # tasks will be executed one by one on a single process
    synchronous=auto()
    # tasks will be executed in multiprocessing
    mp=auto()
    # tasks will be executed in coroutine
    #   fnCoroutine will be used
    #   if fnCoroutine is None, fn will be used (not recommended)
    coroutine=auto()

class EPipeType(Enum):
    default=auto()        # treat as single context
    map=auto()            # pipe to multiple context

@dataclass
class TaskContext:
    args: Iterable = field(default_factory=tuple)
    kwargs: Mapping = field(default_factory=dict)
    
    # args map tasks, can be executed in synchronous(map), mp or coroutine
    # for each sub-task
    # fn(*args, *argsForMp[i], **kwargs, **kwargsForMp[i])
    argsForMap: Iterable[Iterable] | AsyncIterable[Iterable] = field(default_factory=tuple)
    kwargsForMap: Iterable[Mapping] | AsyncIterable[Mapping] = field(default_factory=tuple)

    @property
    def isForMap(self) -> bool:
        return not (isinstance(self.argsForMap, tuple) and len(self.argsForMap) == 0\
                and isinstance(self.kwargsForMap, tuple) and len(self.kwargsForMap) == 0)


@dataclass
class Task:
    desc: str
    fn: Callable[..., Tuple[bool, ...]] | None = None
    fnCoroutine: Callable[..., Awaitable[Tuple[bool, ...]]] | None = None

    msgIndent: str = ''
    startMsgPrinter: Callable[[str, str], None] | None = None
    failureMsgPrinter: Callable[[str, str], None] | None = None
    successMsgPrinter: Callable[[str, str], None] | None = None

    def __post_init__(self):
        # default settings
        if self.startMsgPrinter is None:
            self.startMsgPrinter = Task.defaultPrintStartMsg
        if self.failureMsgPrinter is None:
            self.failureMsgPrinter = Task.defaultPrintFailureMsg
        if self.successMsgPrinter is None:
            self.successMsgPrinter = Task.defaultPrintSuccessMsg

    #def inputPipe(self, *inputArgs, **inputKwargs):
    #    assert self.pipe
    #    self.args += genToTuple(inputArgs)
    #    self.kwargs.update(genToTuple(inputKwargs))

    def isMergable(self, other: 'Task'):
        if self is other:
            return True
        result = \
            self.desc == other.desc and\
            self.fn == other.fn
        return result

    @staticmethod
    def defaultPrintStartMsg(desc, indent):
        print(indent, f'Start to {desc}...')

    @staticmethod
    def defaultPrintFailureMsg(desc, indent):
        print(indent, f'Fail to {desc}!')

    @staticmethod
    def defaultPrintSuccessMsg(desc, indent):
        print(indent, f'Success to {desc}.')

    def printMsg(self, printer: Callable[[str, str], None]):
        printer(self.desc, self.msgIndent)


'''

@dataclass
class MpTask(BaseTask):
    # for each sub task
    # fn(*args, *argsForMp, **kwargs, **kwargsForMp)
    argsForMp: Iterable[Iterable] = ()
    kwargsForMp: Iterable[Mapping] = ()
    # Callable to get description of a sub task
    #   Inputs:
    #       desc: desc from MpTask
    #       *argsMp **kwargsMp
    mpTaskDescGetter: Callable[..., str] | None = None
    # working with pipe, pipe to argsForMp instead of args (kwargsForMp, kwargs)
    pipeToMpArgs: bool = False
    # pipe from sub-task from precursor MpTask
    mpPipe: bool = False
    chunkSize: int = 1    # TODO

    def __post_init__(self):
        super().__post_init__()
        if self.mpTaskDescGetter is None:
            self.mpTaskDescGetter = MpTask.defaultMpDescGetter

    @staticmethod
    def defaultMpDescGetter(desc, *argsMp, **kwargsMp)->str:
        s = desc
        withAdded = False
        if len(argsMp) > 0:
            s += ' with '
            if len(argsMp) == 1:
                s += 'arg '
                s += str(argsMp[0])
            else:
                s += 'args'
                s += str(argsMp)
            withAdded = True
        if len(kwargsMp) > 0:
            if not withAdded:
                s += ' with '
            else:
                s += ', '
            s += 'kwargs'
            s += str(kwargsMp)
        return s

    # all args, a tuple (args, argsForMp, kwargs, kwargsForMp)
    @staticmethod
    def subFn(allArgs, fn, desc):
        args = allArgs[0]
        argsMp = allArgs[1]
        kwargs = allArgs[2]
        kwargsMp = allArgs[3]
        try:
            fullResult = fn(*args, *argsMp,
                            **kwargs, **kwargsMp)
        except Exception as e:
            print('Exception occurred: ', e, f'; in {fn}')
            traceback.print_tb(e.__traceback__)
            fullResult = False
        
        if isinstance(fullResult, tuple):
            result = fullResult[0]
            returnData = fullResult[1:]
        elif isinstance(fullResult, bool):
            result = fullResult
            returnData = ()
        else:
            assert False, f'Invalid return data for {desc}! '\
                f'Please return bool, or (bool, customized data).'
        return result, argsMp, kwargsMp, returnData

    def iterSingleTask(self)-> Iterable[SingleTask]:
        argsIterator = (args for args in self.argsForMp)
        kwargsIterator = (kwargs for kwargs in self.kwargsForMp)
        while True:
            args = next(argsIterator, ())
            kwargs = next(kwargsIterator, {})
            if args == () and kwargs == {}:
                break
            if not isinstance(args, tuple):
                args = (args, )
            yield self.pipeMpSubTask(*args, *kwargs)

    def inputPipe(self, *inputArgs, **inputKwargs):
        if not self.pipeToMpArgs:
            super().inputPipe(*inputArgs, **inputKwargs)
        else:
            assert self.pipe
            self.argsForMp = (*self.argsForMp, *genToTuple(inputArgs))

    def pipeMpSubTask(self, *args, **kwargs) -> SingleTask:
        args = genToTuple(args)
        kwargs = genToTuple(kwargs)
        desc = self.mpTaskDescGetter(self.desc, *args, **kwargs)
        subTask = SingleTask(desc,
                        functools.partial(MpTask.subFn, fn=self.fn, desc=desc), 
                        args=((self.args, args, self.kwargs, kwargs),),
                        msgIndent='\t')
        if hasattr(self, 'id'):
            subTask.id = self.id
        return subTask

    def isMergable(self, other: 'MpTask'):
        if self is other:
            return True
        result = \
            isinstance(other, MpTask) and\
            super().isMergable(other) and\
            self.pipeToMpArgs == other.pipeToMpArgs and\
            self.mpPipe == other.mpPipe and\
            self.chunkSize == other.chunkSize
        return result

'''