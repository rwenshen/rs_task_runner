__all__ = [
    'TaskDef',
    'SubGraphTask',
    'TaskGraph',
]

from dataclasses import dataclass, field
from typing import Iterable, Mapping, Hashable
from typing import Tuple, List, Deque, Set, Dict
from collections import deque, OrderedDict

from .task import Task, TaskContext, ERunType, EPipeType


@dataclass
class TaskDef:
    task: Task
    context: TaskContext | None = None
    runType: ERunType = ERunType.default
    precursors: Hashable | Tuple[Hashable] = field(default_factory=tuple)
    pipeTypes: EPipeType | Dict[Hashable, EPipeType] = field(default_factory=dict)

    def verify(self):
        if not isinstance(self.precursors, tuple):
            self.precursors = (self.precursors, )
        if isinstance(self.pipeTypes, EPipeType):
            assert len(self.precursors) == 1,\
                f'Cannot find precursor task for pipeType {self.pipeTypes}'\
                f' in task "{self.task.desc}"'
            self.pipeTypes = {self.precursors[0]: self.pipeTypes}
        else:
            precursorSet = set(self.precursors)
            for taskId, pipeType in self.pipeTypes.items():
                assert taskId in precursorSet,\
                    f'Cannot find precursor task for pipeType {pipeType}'\
                    f' in task "{self.task.desc}"'

    def isMergable(self, other: 'TaskDef'):
        if self is other:
            return True
        result = \
            self.task.isMergable(other.task) and \
            self.context == other.context and \
            self.runType == other.runType and \
            self.pipeType == other.pipeType
        return result


class _TaskNode:

    # task None means root
    def __init__(self, taskDef:TaskDef|None=None):
        self.taskDef: TaskDef | None = taskDef
        if self.taskDef is None:
            self.nodeDict: Dict[Hashable, _TaskNode] = {}
        self.precursors: 'List[_TaskNode]' = []
        self.precursorsIdSet: Set[Hashable] = set()
        self.successors: 'List[_TaskNode]' = []
        self.successorsIdSet: Set[Hashable] = set()
        self.__allPrecursorIdSet: Set[Hashable] = set()

    def iterSelfAndAllSuccessors(self, visitedSet: Set) -> 'Iterable[_TaskNode]':
        if self.taskDef.id in visitedSet:
            return
        visitedSet.add(self.taskDef.id)
        yield self
        for successor in self.successors:
            yield from successor.iterSelfAndAllSuccessors(visitedSet)

    def addSuccessor(self, node: '_TaskNode'):
        # validation
        assert node.taskDef.id not in self.successorsIdSet, f'Task {node.taskDef.id}'\
            f' has been a successor of task {self.taskDef.id}!'
        if self.taskDef is not None:
            assert self.taskDef.id not in node.precursorsIdSet, 'Task '\
            f'{self.taskDef.id} has been a precursor of task {node.taskDef.id}!'
            assert node.taskDef.id not in self.__allPrecursorIdSet, 'Recursive '\
            f'graph! Task {node.taskDef.id} has been a precursor of task'\
            f' {self.taskDef.id}, it CANNOT be a successor of task {self.taskDef.id}!'
        # connect
        self.successors.append(node)
        self.successorsIdSet.add(node.taskDef.id)
        if self.taskDef is not None:
            node.precursors.append(self)
            node.precursorsIdSet.add(self.taskDef.id)
            newPrecursorIdSet = self.__allPrecursorIdSet.copy()
            newPrecursorIdSet.add(self.taskDef.id)
            visitedSet = set()
            for successor in self.iterSelfAndAllSuccessors(visitedSet):
                successor.__allPrecursorIdSet |= newPrecursorIdSet
'''
    def inputPipe(self, taskId, *inputArgs):
        assert len(self.precursors) > 0
        taskIndex = None
        for index, precursor in enumerate(self.precursors):
            if taskId == precursor.task.id:
                taskIndex = index
                break
        assert taskIndex is not None, f'Task "{taskId}" is not a precursor of'\
                f' task {self.task.id}!'
        if taskIndex == 0:
            self.task.inputPipe(*inputArgs)
        else:
            self.task.inputPipe(**{
                f'args{taskIndex}': inputArgs
            })

    def pipeMpSubTask(self, taskId, *inputArgs) -> SingleTask:
        assert isinstance(self.task, MpTask)
        result, argsMp, kwargsMp, returnData = inputArgs
        assert result
        self.__mpSubTaskSent += 1
        return self.task.pipeMpSubTask(*argsMp, *returnData, **kwargsMp)
    
    def finishPipingMpSubTask(self):
        self.__mpSubTaskCount = self.__mpSubTaskSent

    def iterMpSubTask(self) -> Iterable[SingleTask]:
        assert isinstance(self.task, MpTask)
        for singleTask in self.task.iterSingleTask():
            self.__mpSubTaskSent += 1
            yield singleTask
        self.__mpSubTaskCount = self.__mpSubTaskSent

    def handleSingleTaskResult(self, fullResult) -> HandledData:
        result, returnData = self.task.handleResult(fullResult)
        if not result:
            self.task.printMsg(self.task.failureMsgPrinter)
        else:
            self.task.printMsg(self.task.successMsgPrinter)
        return _TaskNode.HandledData(
            result, True, self.task, returnData)

    def handleMpTaskResult(self, subResult) -> HandledData:
        self.__mpSubTaskHandled += 1
        self.__mpResults.append(subResult)
        if self.__mpSubTaskHandled != self.__mpSubTaskCount:
            return _TaskNode.HandledData(
                True, False, self.task, returnDataMpSub=subResult)

        # final result
        finalResult = True
        for result, argsMp, kwargsMp, returnData in self.__mpResults:
            if not result:
                subTaskDesc=self.task.mpTaskDescGetter(
                        self.task.desc, *argsMp, **kwargsMp)
                self.task.failureMsgPrinter(
                            subTaskDesc, self.task.msgIndent)
                finalResult = False
        if not finalResult:
            self.task.printMsg(self.task.failureMsgPrinter)
        else:
            self.task.printMsg(self.task.successMsgPrinter)
        #   for last one, return the last result and full result
        return _TaskNode.HandledData(
            finalResult, True, self.task, self.__mpResults, subResult)

    def getResultHandler(self) -> Callable[..., HandledData]:
        if isinstance(self.task, SingleTask):
            return self.handleSingleTaskResult
        if isinstance(self.task, MpTask):
            return self.handleMpTaskResult
        assert False
'''

@dataclass
class SubGraphTask(Task):
    desc: str = field(init=False)
    fn: None = field(init=False, default=None)
    graph: 'TaskGraph|None' = None
    deps: Mapping[Hashable, Set[Hashable]] = field(default_factory=dict)

    def __post_init__(self):
        assert self.graph is not None

    def isMergable(self, other: 'SubGraphTask'):
        return False


class TaskGraph:
    # define example
    #   TaskGraph({
    #       id0: TaskDef(task=task0, context=c),
    #       id1: TaskDef(task=task1, context=c),
    #       ...,                                                                parallel
    #       id1_0: TaskDef(task=task1_0, precursors=id1),                       depends
    #       id0_0: TaskDef(task=task0_0, precursors=(task0_id, task1_0_id)),    multi depends
    #       id3: TaskDef(task=task3),                                           random order
    #       id4: TaskDef(task=SubGraphTask(graph=OtherGraph, deps))
    #       ...,
    #   })
    def __init__(self, taskDict: Mapping[Hashable, TaskDef],
                defaultRunType: ERunType = ERunType.mp):
        self.__defaultRunType = defaultRunType
        self.__initDict = OrderedDict()
        self.__hasSynchronousTask = False
        self.__hasMpTask = False
        self.__hasCoroutineTask = False

        currentDict: Mapping[Hashable, TaskDef] = OrderedDict()
        def getDepIds(td):
            depIds = ()
            if isinstance(td.precursors, tuple):
                depIds = td.precursors
            else:
                depIds = (td.precursors, )
            return depIds
        for taskId, taskDef in taskDict.items():
            depIds = getDepIds(taskDef)

            if isinstance(taskDef.task, SubGraphTask):
                tmpGraph = TaskGraph(currentDict)
                otherGraph = taskDef.task.graph
                depDict = taskDef.task.deps.copy()
                dep = depDict.get(otherGraph.startId, ())
                dep = (*depIds, *dep)
                depDict[otherGraph.startId] = dep
                tmpGraph = tmpGraph.merge(otherGraph, depDict)
                currentDict = tmpGraph.__initDict
            else:
                currentDict[taskId] = taskDef

        self.root = _TaskNode()
        def newNode(td: TaskDef):
            node = _TaskNode(td)
            assert td.id not in self.root.nodeDict
            self.root.nodeDict[td.id] = node
            return node
        for taskId, taskDef in currentDict.items():
            if taskDef.runType == ERunType.default:
                taskDef.runType = self.__defaultRunType
            # verify task
            taskDef.verify()
            if taskDef.runType == ERunType.coroutine:
                assert taskDef.task.fnCoroutine is not None or taskDef.task.fn is not None
            else:
                assert taskDef.task.fn is not None
            # parse task and dependencies
            depIds = getDepIds(taskDef)
            if len(depIds) == 0:
                deps = (self.root,)
            else:
                deps = []
                for depTaskId in depIds:
                    assert depTaskId in self.root.nodeDict,\
                        f'Dependent task "{depTaskId}" has NOT been defined!'
                    depNode = self.root.nodeDict[depTaskId]
                    deps.append(depNode)

            # normal task
            taskDef.id = taskId
            node = newNode(taskDef)
            for depNode in deps:
                depNode.addSuccessor(node)

            self.__finishInit()

    @property
    def startId(self):
        return self.root.successors[0].taskDef.id
    @property
    def hasSynchronousTask(self) -> bool:
        return self.__hasSynchronousTask
    @property
    def hasMpTask(self) -> bool:
        return self.__hasMpTask
    @property
    def hasCoroutineTask(self) -> bool:
        return self.__hasCoroutineTask

    def __depIter(self) -> Iterable[_TaskNode]:
        nodeDeque: Deque[_TaskNode] = deque(self.root.successors)
        addedSet: Set = set(self.root.successorsIdSet)
        yieldedSet: Set = set()
        while len(nodeDeque) > 0:
            node = nodeDeque.popleft()
            needOther = False
            for precursor in node.precursors:
                if precursor.taskDef.id not in yieldedSet:
                    needOther = True
                    break
            if needOther:
                nodeDeque.append(node)
                continue

            yieldedSet.add(node.taskDef.id)
            yield node
            for successor in node.successors:
                if successor.taskDef.id not in addedSet:
                    nodeDeque.append(successor)
                    addedSet.add(successor.taskDef.id)

    def __finishInit(self):
        for node in self.__depIter():
            self.__initDict[node.taskDef.id] = node.taskDef
            if node.taskDef.runType == ERunType.mp:
                self.__hasMpTask = True
            elif node.taskDef.runType == ERunType.coroutine:
                self.__hasCoroutineTask = True
            else:
                self.__hasSynchronousTask = True

    def copy(self) -> 'TaskGraph':
        return TaskGraph(self.__initDict)

    # merge example
    #   depDict: should be dependencies between two graph
    #   this.merge({
    #       taskid0_3: taskid1_4,
    #       taskid1_0: taskid0_1,
    #       taskid1_1: (taskid0_1, taskid0_2),
    #       ...
    #   })
    def merge(self, other: 'TaskGraph',
            depDict: Mapping[Hashable, Set[Hashable]]) -> 'TaskGraph':
        # verify and preparation
        thisTaskIdSet = set(self.root.nodeDict.keys())
        thatTaskIdSet = set(other.root.nodeDict.keys())
        commonTaskIdSet = thisTaskIdSet & thatTaskIdSet
        for taskId in commonTaskIdSet:
            thisNode = self.root.nodeDict[taskId]
            thatNode = other.root.nodeDict[taskId]
            assert thisNode.taskDef.isMergable(thatNode.task)

        s2pDict = {}
        p2sDict = {}
        for successorId, precursorIds in depDict.items():
            if not isinstance(precursorIds, tuple):
                precursorIds = (precursorIds, )
            s2pDict[successorId] = precursorIds
            for precursorId in precursorIds:
                if successorId in thisTaskIdSet:
                    assert precursorId in thatTaskIdSet,\
                        f'"{successorId}" and "{precursorId}" are both in this'\
                        ' graph! Only dependencies between graphs is allowed in'\
                        'merging!'
                elif successorId in thatTaskIdSet:
                    assert precursorId in thisTaskIdSet,\
                        f'"{successorId}" and "{precursorId}" are both in that'\
                        ' graph! Only dependencies between graphs is allowed in'\
                        'merging!'
                else:
                    assert 0
                ss = p2sDict.setdefault(precursorId, [])
                ss.append(successorId)

        taskIdDeque = deque(node.taskDef.id\
                    for node in self.root.successors)
        taskIdDeque.extend(node.taskDef.id\
                    for node in other.root.successors\
                        if node.taskDef.id not in self.root.successorsIdSet)
        addedSet = self.root.successorsIdSet | other.root.successorsIdSet
        roundedSet: Set = set()
        circularDependencyCheck = 0
        yieldedSet: Set = set()
        newDict = OrderedDict()
        while len(taskIdDeque) > 0:
            taskId = taskIdDeque.popleft()
            thisNode = None if taskId not in thisTaskIdSet\
                            else self.root.nodeDict[taskId]
            thatNode = None if taskId not in thatTaskIdSet\
                            else other.root.nodeDict[taskId]
            taskDef = thisNode.taskDef if thisNode is not None else thatNode.taskDef
            assert thisNode is not None or thatNode is not None
            # get precursors
            def getPrecursorsFromGraph(node: _TaskNode):
                precursors = ()
                if node is not None:
                    return tuple(n.taskDef.id for n in node.precursors)
                return precursors
            precursors = getPrecursorsFromGraph(thisNode)
            otherPrecursors = getPrecursorsFromGraph(thatNode)
            if len(otherPrecursors) > 0 and len(precursors) > 0 and\
                    taskDef.pipeType != EPipeType.none:
                assert precursors == otherPrecursors,\
                    f'Pipe task "{taskId}" need the same precursors!'
            for id in otherPrecursors:
                if id not in precursors:
                    precursors = (*precursors, id)
            otherPrecursors = () if taskId not in s2pDict else s2pDict[taskId]
            assert not (len(otherPrecursors) > 0 and taskDef.pipeType != EPipeType.none),\
                f'Cannot add new dependencies to a pipe task "{taskId}"!'
            for id in otherPrecursors:
                assert id not in precursors, \
                    f'Duplicated dependency: "{taskId}" <- "{id}"'
            precursors = (*precursors, *otherPrecursors)
            
            # iterate
            needOther = False
            for precursor in precursors:
                if precursor not in yieldedSet:
                    needOther = True
                    break
            if needOther:
                taskIdDeque.append(taskId)
                if taskId in roundedSet:
                    if (roundedSet - yieldedSet) == set(taskIdDeque):
                        assert circularDependencyCheck <= len(taskIdDeque),\
                            f'Circular dependency is found! {tuple(taskIdDeque)}'
                        circularDependencyCheck += 1
                else:
                    roundedSet.add(taskId)
                continue

            yieldedSet.add(taskId)
            taskDef.precursors = precursors
            newDict[taskId] = taskDef

            # get successor
            successors = set()
            if thisNode is not None:
                successors |= set(node.taskDef.id for node in thisNode.successors)
            if thatNode is not None:
                successors |= set(node.taskDef.id for node in thatNode.successors)
            if taskId in p2sDict:
                successors |= set(p2sDict[taskId])
            for successor in successors:
                if successor not in addedSet:
                    taskIdDeque.append(successor)
                    addedSet.add(successor)

        return TaskGraph(newDict)

    def printGraph(self):
        print('Start:')
        for node in self.__depIter():
            s = '  "' + str(node.taskDef.id) + '"'
            successorNum = len(node.successors)
            if successorNum > 0:
                s += ' -> '
            if successorNum > 1:
                s += '('
            isFirst = True
            for successor in node.successors:
                if isFirst:
                    isFirst = False
                else:
                    s += ', '
                s += '"' + str(successor.taskDef.id) + '"'
            if successorNum > 1:
                s += ')'
            s += ';'
            print(s)
        print('End.')
        print()
